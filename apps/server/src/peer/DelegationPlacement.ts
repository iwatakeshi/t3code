import {
  type ModelSelection,
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

type Scope = McpInvocationContext.McpThreadInvocationScope;

const CapabilitiesTool = Tool.make("orchestrator_capabilities", {
  parameters: OrchestratorMcpCapabilitiesInput,
  success: OrchestratorMcpCapabilitiesResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
});

export class DelegationPlacement extends Context.Service<
  DelegationPlacement,
  {
    readonly select: (
      scope: Scope,
      input: OrchestratorMcpDelegateTaskInput & { readonly clientRequestId: string },
      parent: {
        readonly projectId: ProjectId;
        readonly modelSelection: ModelSelection;
        readonly driverKind: ProviderDriverKind | undefined;
      },
      localTarget: OrchestratorMcpTarget | undefined,
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
  const decodeTarget = Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestratorMcpTarget));
  const encodeTarget = Schema.encodeEffect(Schema.fromJsonString(OrchestratorMcpTarget));

  const select: DelegationPlacement["Service"]["select"] = Effect.fn("DelegationPlacement.select")(
    function* (scope, input, parent, localTarget) {
      if (input.target?.environmentId !== undefined || input.target?.projectId !== undefined) {
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "Auto placement cannot name target.environmentId or target.projectId.",
        });
      }
      if (input.isolation === "worktree") {
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message:
            'Auto placement supports shared checkouts only. For isolation "worktree", choose a machine explicitly.',
        });
      }
      const read = sql<{ readonly target_json: string }>`
        SELECT target_json FROM delegated_task_placements
        WHERE request_namespace = ${scope.requestNamespace}
          AND parent_thread_id = ${scope.thread.threadId}
          AND client_request_id = ${input.clientRequestId}
      `.pipe(
        Effect.flatMap((rows) =>
          rows[0] === undefined ? Effect.undefined : decodeTarget(rows[0].target_json),
        ),
        Effect.mapError(placementFailure),
      );
      const existing = yield* read;
      if (existing !== undefined) return existing;

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
        localTarget === undefined
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
            link.status === "reachable" &&
            link.access !== "read-only" &&
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
              weight: 1,
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
          code: "provider_unavailable",
          message:
            "No machine is ready for Auto placement. Link machines with this repository and provider, update them to support environment resources, or choose a machine explicitly. No task was started.",
        });
      }
      // Pin before launching: retries and concurrent calls must never start this request on two machines.
      const targetJson = yield* encodeTarget(candidate.target).pipe(
        Effect.mapError(placementFailure),
      );
      yield* sql`
        INSERT INTO delegated_task_placements (request_namespace, parent_thread_id, client_request_id, target_json)
        VALUES (${scope.requestNamespace}, ${scope.thread.threadId}, ${input.clientRequestId}, ${targetJson})
        ON CONFLICT DO NOTHING
      `.pipe(Effect.mapError(placementFailure));
      const pinned = yield* read;
      return pinned ?? candidate.target;
    },
  );
  return DelegationPlacement.of({ select });
});

export const layer = Layer.effect(DelegationPlacement, make);
