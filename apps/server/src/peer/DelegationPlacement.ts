import { usedDelegatedCapacity } from "../orchestration-v2/DelegatedCapacity.ts";
import {
  type ModelSelection,
  type ExecutionEnvironmentPlatform,
  ExecutionRequirements,
  GitCommitSha,
  OrchestratorMcpCapabilitiesInput,
  OrchestratorMcpCapabilitiesResult,
  type OrchestratorMcpDelegateTaskInput,
  OrchestratorMcpFailure,
  OrchestratorMcpTarget,
  type ProjectId,
  type ProviderDriverKind,
} from "@t3tools/contracts";
import { chooseLoadBalancedEnvironment } from "@t3tools/shared/loadBalancing";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { Tool } from "effect/ai";

import type * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import { runtimeModeRank } from "../mcp/delegatedTaskModes.ts";
import { EnvironmentToolkit } from "../mcp/toolkits/environment/tools.ts";
import { ProjectToolkit } from "../mcp/toolkits/project/tools.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import * as HostResources from "../resourceTelemetry/HostResources.ts";
import * as PeerForwarding from "./PeerForwarding.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";

type Scope = McpInvocationContext.McpThreadInvocationScope;
type PinnedWorktree = { readonly projectId: ProjectId; readonly commitSha: string };
const decodeCommit = Schema.decodeEffect(GitCommitSha);

const CapabilitiesTool = Tool.make("orchestrator_capabilities", {
  parameters: OrchestratorMcpCapabilitiesInput,
  success: OrchestratorMcpCapabilitiesResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
});

export class DelegationPlacement extends Context.Service<
  DelegationPlacement,
  {
    readonly availability: (
      scope: Scope,
      limit: number | null,
    ) => Effect.Effect<string, OrchestratorMcpFailure>;
    readonly pinWorkspace: (
      scope: Scope,
      input: OrchestratorMcpDelegateTaskInput & { readonly clientRequestId: string },
      parent?: { readonly projectId: ProjectId; readonly worktreePath: string | null },
    ) => Effect.Effect<PinnedWorktree | undefined, OrchestratorMcpFailure>;
    readonly check: (
      scope: Scope,
      input: OrchestratorMcpDelegateTaskInput & { readonly clientRequestId: string },
      workspace?: PinnedWorktree,
    ) => Effect.Effect<
      | {
          readonly environmentId: Scope["environmentId"];
          readonly label: string;
          readonly platform: ExecutionEnvironmentPlatform;
        }
      | undefined,
      OrchestratorMcpFailure
    >;
    /** Only after a definitive refusal, while holding the request lock across selection and launch. */
    readonly reject: (
      scope: Scope,
      input: OrchestratorMcpDelegateTaskInput & { readonly clientRequestId: string },
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly select: (
      scope: Scope,
      input: OrchestratorMcpDelegateTaskInput & { readonly clientRequestId: string },
      parent: {
        readonly projectId: ProjectId;
        readonly modelSelection: ModelSelection;
        readonly driverKind: ProviderDriverKind | undefined;
      },
      localTarget: OrchestratorMcpTarget | undefined,
      workspace?: PinnedWorktree,
      excluded?: ReadonlySet<Scope["environmentId"]>,
    ) => Effect.Effect<OrchestratorMcpTarget, OrchestratorMcpFailure>;
  }
>()("t3/peer/DelegationPlacement") {}

const placementFailure = () =>
  new OrchestratorMcpFailure({
    code: "orchestration_error",
    message: "Unable to read or save the delegated task's machine selection.",
  });

const make = Effect.gen(function* () {
  const forwarding = yield* PeerForwarding.PeerForwarding;
  const projects = yield* ProjectService.ProjectService;
  const identities = yield* RepositoryIdentityResolver.RepositoryIdentityResolver;
  const resources = yield* HostResources.HostResources;
  const sql = yield* SqlClient.SqlClient;
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const git = yield* GitVcsDriver.GitVcsDriver;
  const decodeTarget = Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestratorMcpTarget));
  const encodeTarget = Schema.encodeEffect(Schema.fromJsonString(OrchestratorMcpTarget));
  const encodeRequirements = Schema.encodeEffect(Schema.fromJsonString(ExecutionRequirements));
  const requirementsJson = (requirements: ExecutionRequirements | undefined) =>
    encodeRequirements(
      requirements?.os === undefined ? {} : { os: [...new Set(requirements.os)].sort() },
    ).pipe(Effect.mapError(placementFailure));
  const readRequest = Effect.fn("DelegationPlacement.readRequest")(function* (
    scope: Scope,
    key: string,
    requirements: string,
    isolation: "shared" | "worktree",
  ) {
    const rows = yield* sql<{
      readonly target_json: string;
      readonly requirements_json: string;
      readonly isolation: string;
      readonly base_commit: string | null;
    }>`
      SELECT target_json, requirements_json, isolation, base_commit FROM delegated_task_placements
      WHERE request_namespace = ${scope.requestNamespace}
        AND parent_thread_id = ${scope.thread.threadId}
        AND client_request_id = ${key}
    `.pipe(Effect.mapError(placementFailure));
    if (rows[0] === undefined) return undefined;
    if (rows[0].requirements_json !== requirements)
      return yield* new OrchestratorMcpFailure({
        code: "request_conflict",
        message:
          "This clientRequestId already has different execution requirements. Use a new key for a different task.",
      });
    if (rows[0].isolation !== isolation)
      return yield* new OrchestratorMcpFailure({
        code: "request_conflict",
        message:
          "This clientRequestId already has different workspace isolation. Use a new key for a different task.",
      });
    return rows[0];
  });
  const readPlacement = Effect.fn("DelegationPlacement.readPlacement")(function* (
    scope: Scope,
    key: string,
    requirements: string,
    isolation: "shared" | "worktree",
  ) {
    const row = yield* readRequest(scope, key, requirements, isolation);
    if (row === undefined) return undefined;
    const target = yield* decodeTarget(row.target_json).pipe(Effect.mapError(placementFailure));
    // The parent commit is pinned before placement; an empty target is still awaiting a machine.
    return Object.keys(target).length === 0 ? undefined : target;
  });
  const pin = Effect.fn("DelegationPlacement.pin")(function* (
    scope: Scope,
    key: string,
    requirements: string,
    target: OrchestratorMcpTarget,
    isolation: "shared" | "worktree",
  ) {
    const targetJson = yield* encodeTarget(target).pipe(Effect.mapError(placementFailure));
    yield* sql`
      INSERT INTO delegated_task_placements (request_namespace, parent_thread_id, client_request_id, target_json, requirements_json, isolation)
      VALUES (${scope.requestNamespace}, ${scope.thread.threadId}, ${key}, ${targetJson}, ${requirements}, ${isolation})
      ON CONFLICT DO UPDATE SET target_json = excluded.target_json
      WHERE delegated_task_placements.target_json = '{}'
        AND delegated_task_placements.requirements_json = excluded.requirements_json
        AND delegated_task_placements.isolation = excluded.isolation
    `.pipe(Effect.mapError(placementFailure));
    return yield* readPlacement(scope, key, requirements, isolation);
  });
  const pinWorkspace: DelegationPlacement["Service"]["pinWorkspace"] = Effect.fn(
    "DelegationPlacement.pinWorkspace",
  )(function* (scope, input, parent) {
    const requirements = yield* requirementsJson(input.requirements);
    const isolation = input.isolation ?? "shared";
    const existing = yield* readRequest(scope, input.clientRequestId, requirements, isolation);
    if (isolation === "worktree" && parent === undefined)
      return yield* new OrchestratorMcpFailure({
        code: "invalid_request",
        message: "Worktree isolation requires the parent checkout.",
      });
    if (existing === undefined) {
      let commitSha: string | null = null;
      if (isolation === "worktree" && parent !== undefined) {
        const project = yield* projects
          .getById(parent.projectId)
          .pipe(Effect.mapError(placementFailure));
        if (Option.isNone(project))
          return yield* new OrchestratorMcpFailure({
            code: "invalid_request",
            message: "The parent project was not found.",
          });
        const base = yield* git
          .resolveCommit({
            cwd: parent.worktreePath ?? project.value.workspaceRoot,
            revision: "HEAD",
          })
          .pipe(
            Effect.mapError(
              () =>
                new OrchestratorMcpFailure({
                  code: "base_unavailable",
                  message:
                    "Unable to resolve the parent checkout commit. Commit the code before delegating isolated work.",
                }),
            ),
          );
        commitSha = yield* decodeCommit(base.commitSha).pipe(Effect.mapError(placementFailure));
      }
      yield* sql`
        INSERT INTO delegated_task_placements (request_namespace, parent_thread_id, client_request_id, target_json, requirements_json, isolation, base_commit)
        VALUES (${scope.requestNamespace}, ${scope.thread.threadId}, ${input.clientRequestId}, '{}', ${requirements}, ${isolation}, ${commitSha})
        ON CONFLICT DO NOTHING
      `.pipe(Effect.mapError(placementFailure));
    }
    const pinned = yield* readRequest(scope, input.clientRequestId, requirements, isolation);
    if (isolation === "shared") return undefined;
    if (pinned?.base_commit == null || parent === undefined)
      return yield* new OrchestratorMcpFailure({
        code: "request_conflict",
        message: "This request has no pinned worktree commit. Use a new clientRequestId.",
      });
    const commitSha = yield* decodeCommit(pinned.base_commit).pipe(
      Effect.mapError(placementFailure),
    );
    return { projectId: parent.projectId, commitSha };
  });
  const commitAvailable = Effect.fn("DelegationPlacement.commitAvailable")(function* (
    scope: Scope,
    environmentId: Scope["environmentId"],
    projectId: ProjectId,
    commitSha: string,
  ) {
    if (environmentId !== scope.environmentId)
      return (yield* forwarding.call(
        scope,
        ProjectToolkit.tools.t3_project_check_commit,
        environmentId,
        { projectId, commitSha },
      )).available;
    const project = yield* projects.getById(projectId).pipe(Effect.mapError(placementFailure));
    if (Option.isNone(project)) return false;
    return yield* git.resolveCommit({ cwd: project.value.workspaceRoot, revision: commitSha }).pipe(
      Effect.map((commit) => commit.commitSha === commitSha),
      Effect.orElseSucceed(() => false),
    );
  });
  const check: DelegationPlacement["Service"]["check"] = Effect.fn("DelegationPlacement.check")(
    function* (scope, input, workspace) {
      const requirements = yield* requirementsJson(input.requirements);
      const existing = yield* readPlacement(
        scope,
        input.clientRequestId,
        requirements,
        input.isolation ?? "shared",
      );
      const destination = input.target?.environmentId ?? scope.environmentId;
      if (existing !== undefined && (existing.environmentId ?? scope.environmentId) !== destination)
        return yield* new OrchestratorMcpFailure({
          code: "request_conflict",
          message:
            "This clientRequestId already targets a different environment. Use a new key for a different task.",
        });
      const descriptor =
        destination === scope.environmentId
          ? yield* environment.getDescriptor
          : (yield* forwarding.links(scope)).links.find(
              (link) => link.environmentId === destination && link.status === "reachable",
            );
      const platform = descriptor?.platform;
      if (workspace !== undefined) {
        if (
          destination !== scope.environmentId &&
          !(
            descriptor !== undefined &&
            "delegatedWorktrees" in descriptor &&
            descriptor.delegatedWorktrees === true
          )
        )
          return yield* new OrchestratorMcpFailure({
            code: "capability_denied",
            message:
              "Update T3 Code on the destination to support delegated worktrees. No task was started.",
          });
        const projectId =
          destination === scope.environmentId ? workspace.projectId : input.target?.projectId;
        if (
          projectId === undefined ||
          !(yield* commitAvailable(scope, destination, projectId, workspace.commitSha))
        )
          return yield* new OrchestratorMcpFailure({
            code: "base_unavailable",
            message: `Commit ${workspace.commitSha} is unavailable in the destination project. Transfer or fetch it there before retrying; no task was started.`,
          });
      }
      if (input.requirements?.os !== undefined) {
        const supported =
          destination === scope.environmentId ||
          (descriptor !== undefined &&
            "executionOsRequirements" in descriptor &&
            descriptor.executionOsRequirements === true);
        if (!supported)
          return yield* new OrchestratorMcpFailure({
            code: "capability_denied",
            message:
              "The destination cannot enforce OS requirements. Update T3 Code there and check the link; no task was started.",
          });
        if (platform === undefined || !input.requirements.os.some((os) => os === platform.os))
          return yield* new OrchestratorMcpFailure({
            code: "no_compatible_environment",
            message: `This task requires ${input.requirements.os.join(" or ")}; the destination runs ${platform?.os ?? "an unknown OS"}. No task was started.`,
          });
      }
      const pinned = yield* pin(
        scope,
        input.clientRequestId,
        requirements,
        {
          ...input.target,
          environmentId: destination,
        },
        input.isolation ?? "shared",
      );
      if (pinned?.environmentId !== destination)
        return yield* new OrchestratorMcpFailure({
          code: "request_conflict",
          message:
            "This clientRequestId already targets a different environment. Use a new key for a different task.",
        });
      return platform === undefined || descriptor === undefined
        ? undefined
        : { environmentId: destination, label: descriptor.label, platform };
    },
  );

  const select: DelegationPlacement["Service"]["select"] = Effect.fn("DelegationPlacement.select")(
    function* (scope, input, parent, localTarget, workspace, excluded) {
      if (input.target?.environmentId !== undefined || input.target?.projectId !== undefined) {
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "Auto placement cannot name target.environmentId or target.projectId.",
        });
      }
      if (input.isolation === "worktree" && workspace === undefined) {
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "Worktree isolation requires a pinned parent commit.",
        });
      }
      const requirements = yield* requirementsJson(input.requirements);
      const existing = yield* readPlacement(
        scope,
        input.clientRequestId,
        requirements,
        input.isolation ?? "shared",
      );
      if (existing !== undefined) return existing;
      const localDescriptor = yield* environment.getDescriptor;

      // Use the same link checks and access limits as an explicitly targeted delegation.
      const linked = yield* forwarding.links(scope);
      const project = yield* projects
        .getById(parent.projectId)
        .pipe(Effect.mapError(placementFailure));
      const key = Option.isSome(project)
        ? (yield* identities.resolve(project.value.workspaceRoot))?.canonicalKey
        : undefined;
      const model = input.target?.model ?? parent.modelSelection.model;
      const driverKind =
        input.target?.driverKind ??
        (input.target?.providerInstanceId === undefined ? parent.driverKind : undefined);
      const inheritedOptions =
        model === parent.modelSelection.model &&
        (driverKind === undefined || driverKind === parent.driverKind)
          ? (parent.modelSelection.options ?? [])
          : [];
      const requestedOptions = input.target?.options;
      const options =
        requestedOptions?.length === 0
          ? []
          : [
              ...new Map(
                [...inheritedOptions, ...(requestedOptions ?? [])].map((option) => [
                  option.id,
                  option,
                ]),
              ).values(),
            ];
      const local =
        excluded?.has(scope.environmentId) ||
        localTarget === undefined ||
        (workspace !== undefined &&
          !(yield* commitAvailable(
            scope,
            scope.environmentId,
            workspace.projectId,
            workspace.commitSha,
          ))) ||
        (input.requirements?.os !== undefined &&
          !input.requirements.os.some((os) => os === localDescriptor.platform.os))
          ? []
          : [
              {
                environmentId: scope.environmentId,
                target: {
                  ...localTarget,
                  environmentId: scope.environmentId,
                  projectId: parent.projectId,
                },
                resources: yield* resources.read,
                receivedAt: DateTime.toEpochMillis(yield* DateTime.now),
                weight: 1,
              },
            ];
      const remote = yield* Effect.forEach(
        linked.links.filter(
          (link) =>
            !excluded?.has(link.environmentId) &&
            link.status === "reachable" &&
            link.access !== "read-only" &&
            (workspace === undefined || link.delegatedWorktrees === true) &&
            (input.requirements?.os === undefined ||
              (link.executionOsRequirements === true &&
                input.requirements.os.some((os) => os === link.platform?.os))) &&
            (input.runtimeMode === undefined ||
              input.runtimeMode === "inherit" ||
              runtimeModeRank(input.runtimeMode) <= runtimeModeRank(link.access)),
        ),
        (link) =>
          Effect.gen(function* () {
            if (
              key === undefined ||
              (driverKind === undefined && input.target?.providerInstanceId === undefined)
            )
              return undefined;
            const matches: Array<ProjectId> = [];
            let cursor: number | null = 0;
            while (cursor !== null) {
              const page: Tool.Success<typeof ProjectToolkit.tools.t3_project_list> =
                yield* forwarding.call(
                  scope,
                  ProjectToolkit.tools.t3_project_list,
                  link.environmentId,
                  { cursor, limit: 100 },
                );
              matches.push(
                ...page.projects
                  .filter((candidate) => candidate.repositoryIdentity?.canonicalKey === key)
                  .map((candidate) => candidate.id),
              );
              cursor = page.nextCursor;
            }
            if (matches.length !== 1) return undefined;
            const projectId = matches[0]!;
            if (
              workspace !== undefined &&
              !(yield* commitAvailable(scope, link.environmentId, projectId, workspace.commitSha))
            )
              return undefined;
            const capabilities = yield* forwarding.call(
              scope,
              CapabilitiesTool,
              link.environmentId,
              { projectId, search: model },
            );
            const capable = capabilities.providers.filter(
              (provider) =>
                provider.canRunChildTask &&
                (input.target?.providerInstanceId === undefined ||
                  provider.providerInstanceId === input.target.providerInstanceId) &&
                (driverKind === undefined || provider.driverKind === driverKind) &&
                provider.models.some(
                  (candidate) =>
                    candidate.id === model &&
                    options.every((selection) =>
                      candidate.options?.some(
                        (descriptor) =>
                          descriptor.id === selection.id &&
                          descriptor.values.includes(selection.value),
                      ),
                    ),
                ) &&
                (input.runtimeMode === undefined ||
                  input.runtimeMode === "inherit" ||
                  provider.runtimeModes === undefined ||
                  provider.runtimeModes.includes(input.runtimeMode)),
            );
            const provider =
              capable.find(
                (candidate) => candidate.providerInstanceId === parent.modelSelection.instanceId,
              ) ?? capable[0];
            if (provider === undefined) return undefined;
            const snapshot = yield* forwarding.call(
              scope,
              EnvironmentToolkit.tools.t3_environment_resources,
              link.environmentId,
              {},
            );
            return {
              environmentId: link.environmentId,
              target: {
                ...input.target,
                environmentId: link.environmentId,
                projectId,
                providerInstanceId: provider.providerInstanceId,
                driverKind: provider.driverKind,
                model,
                options,
              },
              resources: snapshot,
              receivedAt: DateTime.toEpochMillis(yield* DateTime.now),
              weight:
                snapshot.delegation != null &&
                snapshot.delegation.limit !== null &&
                snapshot.delegation.used >= snapshot.delegation.limit
                  ? 0
                  : 1,
            };
          }).pipe(
            Effect.timeout("5 seconds"),
            Effect.orElseSucceed(() => undefined),
          ),
        { concurrency: 4 },
      );
      const candidates = [...local, ...remote.filter((candidate) => candidate !== undefined)];
      const selected = chooseLoadBalancedEnvironment(
        candidates,
        DateTime.toEpochMillis(yield* DateTime.now),
      );
      const candidate = candidates.find((candidate) => candidate.environmentId === selected);
      if (candidate === undefined) {
        return yield* new OrchestratorMcpFailure({
          code:
            candidates.length === 0 && !excluded?.size
              ? "no_compatible_environment"
              : "no_available_capacity",
          message:
            candidates.length === 0 && !excluded?.size
              ? `No compatible machine has this repository, provider, model and execution requirements${input.requirements?.os === undefined ? "" : ` (${input.requirements.os.join(" or ")})`}${workspace === undefined ? "" : ` with commit ${workspace.commitSha}; transfer it to an eligible worker before retrying`}. Available operating systems: ${[localDescriptor.platform.os, ...linked.links.flatMap((link) => link.platform?.os ?? [])].join(", ")}. No task was started.`
              : "Compatible machines have no available task, CPU or memory capacity. No task was started or queued.",
        });
      }
      // Pin before launching: retries and concurrent calls must never start this request on two machines.
      const pinned = yield* pin(
        scope,
        input.clientRequestId,
        requirements,
        candidate.target,
        input.isolation ?? "shared",
      );
      return pinned ?? candidate.target;
    },
  );
  const reject: DelegationPlacement["Service"]["reject"] = Effect.fn("DelegationPlacement.reject")(
    function* (scope, input) {
      const target = yield* encodeTarget(input.target ?? {});
      yield* sql`
      UPDATE delegated_task_placements SET target_json = '{}'
      WHERE request_namespace = ${scope.requestNamespace}
        AND parent_thread_id = ${scope.thread.threadId}
        AND client_request_id = ${input.clientRequestId}
        AND target_json = ${target}
    `;
    },
    Effect.mapError(placementFailure),
  );
  const encodeAvailability = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
  const availability: DelegationPlacement["Service"]["availability"] = Effect.fn(
    "DelegationPlacement.availability",
  )(function* (scope, limit) {
    const state = (snapshot: import("@t3tools/contracts").HostResourcesSnapshot) => [
      snapshot.cpuUtilization !== null && snapshot.cpuUtilization < 0.95,
      snapshot.totalMemoryBytes > 0 &&
        snapshot.availableMemoryBytes / snapshot.totalMemoryBytes > 0.05,
    ];
    const local = yield* resources.read;
    const used = yield* usedDelegatedCapacity().pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
      Effect.mapError(placementFailure),
    );
    const links = yield* forwarding.links(scope);
    const remote = yield* Effect.forEach(
      links.links,
      (link) =>
        Effect.gen(function* () {
          if (link.status !== "reachable" || link.access === "read-only")
            return [link.environmentId, link.status, link.access];
          const snapshot = yield* forwarding
            .call(scope, EnvironmentToolkit.tools.t3_environment_resources, link.environmentId, {})
            .pipe(
              Effect.timeout("5 seconds"),
              Effect.orElseSucceed(() => undefined),
            );
          return [
            link.environmentId,
            link.access,
            snapshot === undefined
              ? null
              : [
                  ...state(snapshot),
                  snapshot.delegation?.limit ?? null,
                  snapshot.delegation?.used ?? null,
                ],
          ];
        }),
      { concurrency: 4 },
    );
    return yield* encodeAvailability([[...state(local), limit, used], remote]).pipe(
      Effect.mapError(placementFailure),
    );
  });
  return DelegationPlacement.of({ select, check, pinWorkspace, reject, availability });
});

export const layer = Layer.effect(DelegationPlacement, make);
