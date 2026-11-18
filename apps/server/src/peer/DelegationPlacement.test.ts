import {
  EnvironmentId,
  type HostResourcesSnapshot,
  type OrchestratorMcpEnvironmentLink,
  type OrchestratorMcpProviderCapability,
  OrchestratorMcpFailure,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  type RepositoryIdentity,
  ThreadId,
} from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as SqlClient from "effect/sql/SqlClient";

import type * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import * as HostResources from "../resourceTelemetry/HostResources.ts";
import * as DelegationPlacement from "./DelegationPlacement.ts";
import * as PeerForwarding from "./PeerForwarding.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";

const here = EnvironmentId.make("local");
const there = EnvironmentId.make("remote");
const projectId = ProjectId.make("local-project");
const remoteProject = ProjectId.make("remote-project");
const instanceId = ProviderInstanceId.make("codex");
const remoteInstance = ProviderInstanceId.make("codex-remote");
const driverKind = ProviderDriverKind.make("codex");
const parent = { projectId, modelSelection: { instanceId, model: "test-model" }, driverKind };
const scope: McpInvocationContext.McpThreadInvocationScope = {
  environmentId: here,
  requestNamespace: "test-session",
  thread: {
    threadId: ThreadId.make("parent"),
    providerSessionId: "test-session",
    providerInstanceId: instanceId,
  },
  client: undefined,
  capabilities: new Set(["orchestration"]),
  issuedAt: 0,
};
const snapshot: HostResourcesSnapshot = {
  sampledAt: 100_000,
  cpuUtilization: 0.1,
  cpuCount: 8,
  totalMemoryBytes: 16_000,
  availableMemoryBytes: 12_000,
};
const provider: OrchestratorMcpProviderCapability = {
  providerInstanceId: remoteInstance,
  driverKind,
  displayName: null,
  models: [{ id: "test-model" }],
  canRunChildTask: true,
  canRunCrossProviderChildTask: true,
  constraints: [],
};
const link: OrchestratorMcpEnvironmentLink = {
  environmentId: there,
  label: "Worker",
  status: "reachable",
  access: "full-access",
  expiresAt: "2030-01-01T00:00:00.000Z",
  lastError: null,
  platform: { os: "linux", arch: "x64" },
  executionOsRequirements: true,
};

const setup = Effect.gen(function* () {
  const state = yield* Ref.make({
    local: { ...snapshot, cpuUtilization: 0.9 } as HostResourcesSnapshot,
    remote: snapshot,
    links: [link] as ReadonlyArray<OrchestratorMcpEnvironmentLink>,
    providers: [provider] as ReadonlyArray<OrchestratorMcpProviderCapability>,
    repositories: [remoteProject] as ReadonlyArray<ProjectId>,
    supportsResources: true,
  });
  const calls = yield* Ref.make<ReadonlyArray<string>>([]);
  const database = yield* Sqlite.layerMemory.pipe(Layer.build);
  const dependencies = Layer.mergeAll(
    Layer.mock(ServerEnvironment.ServerEnvironment)({
      getDescriptor: Effect.succeed({
        environmentId: here,
        label: "Local",
        platform: { os: "darwin", arch: "arm64" },
        serverVersion: "test",
        capabilities: { repositoryIdentity: true, executionOsRequirements: true },
      }),
    }),
    Layer.succeedContext(database),
    Layer.mock(HostResources.HostResources)({
      read: Ref.get(state).pipe(Effect.map((s) => s.local)),
    }),
    Layer.mock(ProjectService.ProjectService)({
      getById: () => Effect.succeedSome({ workspaceRoot: "/workspace" } as never),
    }),
    Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({
      resolve: () => Effect.succeed({ canonicalKey: "github.com/acme/app" } as RepositoryIdentity),
    }),
    Layer.mock(PeerForwarding.PeerForwarding)({
      links: () =>
        Ref.get(state).pipe(
          Effect.map((s) => ({ environmentId: here, links: s.links, notListed: "" })),
        ),
      call: (_scope, tool, _environmentId, params) =>
        Effect.gen(function* () {
          yield* Ref.update(calls, (seen) => [...seen, tool.name]);
          const current = yield* Ref.get(state);
          let result: unknown;
          if (tool.name === "t3_project_list") {
            result = {
              projects: current.repositories.map((id) => ({
                id,
                repositoryIdentity: { canonicalKey: "github.com/acme/app" },
              })),
              nextCursor: null,
            };
            // Project fixtures only need the identity fields placement reads.
            return result as never;
          }
          if (tool.name === "orchestrator_capabilities") {
            expect(params).toEqual({ projectId: remoteProject, search: "test-model" });
            return { providers: current.providers } as never;
          }
          if (tool.name === "t3_environment_resources" && current.supportsResources) {
            result = current.remote;
          }
          if (!current.supportsResources)
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "Unknown tool",
            });
          return result as never;
        }),
    }),
  );
  const build = DelegationPlacement.layer.pipe(
    Layer.provide(dependencies),
    Layer.fresh,
    Layer.build,
  );
  const context = yield* build;
  return {
    state,
    calls,
    service: yield* DelegationPlacement.DelegationPlacement.pipe(Effect.provide(context)),
    rebuild: build.pipe(
      Effect.flatMap((context) =>
        DelegationPlacement.DelegationPlacement.pipe(Effect.provide(context)),
      ),
    ),
    sql: yield* SqlClient.SqlClient.pipe(Effect.provide(database)),
  };
});

it.effect("selects a capable idle peer and pins the destination across service restarts", () =>
  Effect.gen(function* () {
    const { service, state, calls, rebuild, sql } = yield* setup;
    const input = { task: "Run checks", placement: "auto" as const, clientRequestId: "one" };
    const selected = yield* service.select(scope, input, parent, {});
    expect(selected).toMatchObject({
      environmentId: there,
      projectId: remoteProject,
      providerInstanceId: remoteInstance,
      model: "test-model",
    });
    const seen = yield* Ref.get(calls);
    yield* Ref.update(state, (s) => ({ ...s, links: [], local: snapshot }));
    const restarted = yield* rebuild;
    expect(yield* restarted.select(scope, input, parent, {})).toEqual(selected);
    expect(yield* Ref.get(calls)).toEqual(seen);
    expect(yield* sql`SELECT * FROM delegated_task_placements`).toHaveLength(1);
  }),
);

it.effect("skips peers without capacity, access, repository, provider or resource support", () =>
  Effect.gen(function* () {
    const { service, state } = yield* setup;
    const initial = yield* Ref.get(state);
    const cases = [
      { remote: { ...snapshot, cpuUtilization: 0.99 } },
      { remote: { ...snapshot, availableMemoryBytes: 100 } },
      { links: [{ ...link, access: "read-only" as const }] },
      { links: [{ ...link, status: "unreachable" as const }] },
      { providers: [{ ...provider, canRunChildTask: false }] },
      { providers: [{ ...provider, models: [{ id: "different" }] }] },
      { repositories: [] },
      { repositories: [remoteProject, ProjectId.make("ambiguous")] },
      { supportsResources: false },
    ];
    for (const [index, change] of cases.entries()) {
      yield* Ref.set(state, { ...initial, ...change, local: snapshot });
      const selected = yield* service.select(
        scope,
        { task: "Checks", clientRequestId: String(index) },
        parent,
        {},
      );
      expect(selected.environmentId).toBe(here);
    }
  }),
);

it.effect("fails without launching or pinning when no machine qualifies", () =>
  Effect.gen(function* () {
    const { service, state, sql } = yield* setup;
    yield* Ref.update(state, (s) => ({ ...s, links: [] }));
    const error = yield* service
      .select(scope, { task: "Checks", clientRequestId: "none" }, parent, undefined)
      .pipe(Effect.flip);
    expect(error.code).toBe("no_compatible_environment");
    expect(yield* sql`SELECT * FROM delegated_task_placements`).toHaveLength(0);
  }),
);

it.effect(
  "rejects explicit destinations and worktree isolation instead of changing their meaning",
  () =>
    Effect.gen(function* () {
      const { service, calls } = yield* setup;
      for (const input of [
        { target: { environmentId: there } },
        { target: { projectId: remoteProject } },
        { isolation: "worktree" as const },
      ]) {
        const error = yield* service
          .select(scope, { task: "Checks", clientRequestId: "invalid", ...input }, parent, {})
          .pipe(Effect.flip);
        expect(error.code).toBe("invalid_request");
      }
      expect(yield* Ref.get(calls)).toEqual([]);
    }),
);

it.effect("concurrent retries share one durable placement", () =>
  Effect.gen(function* () {
    const { service, sql } = yield* setup;
    const select = service.select(scope, { task: "Checks", clientRequestId: "same" }, parent, {});
    const results = yield* Effect.all([select, select], { concurrency: "unbounded" });
    expect(results[0]).toEqual(results[1]);
    expect(yield* sql`SELECT * FROM delegated_task_placements`).toHaveLength(1);
  }),
);

it.effect("preserves model options across instances and rejects incompatible peers", () =>
  Effect.gen(function* () {
    const { service, state } = yield* setup;
    const options = [{ id: "effort", value: "high" }];
    const configuredParent = { ...parent, modelSelection: { ...parent.modelSelection, options } };
    yield* Ref.update(state, (s) => ({
      ...s,
      providers: [
        {
          ...provider,
          models: [{ id: "test-model", options: [{ id: "effort", values: ["low", "high"] }] }],
        },
      ],
    }));
    const selected = yield* service.select(
      scope,
      { task: "Review", clientRequestId: "options" },
      configuredParent,
      {},
    );
    expect(selected).toMatchObject({ environmentId: there, options });
    const cleared = yield* service.select(
      scope,
      { task: "Review", clientRequestId: "cleared", target: { options: [] } },
      configuredParent,
      {},
    );
    expect(cleared).toMatchObject({ environmentId: there, options: [] });
    yield* Ref.update(state, (s) => ({ ...s, providers: [provider], local: snapshot }));
    const incompatible = yield* service.select(
      scope,
      { task: "Review", clientRequestId: "incompatible" },
      configuredParent,
      {},
    );
    expect(incompatible.environmentId).toBe(here);
  }),
);

it.effect("filters by OS before capacity and excludes peers that cannot enforce requirements", () =>
  Effect.gen(function* () {
    const { service, state } = yield* setup;
    yield* Ref.update(state, (s) => ({ ...s, local: { ...snapshot, cpuUtilization: 0 } }));
    const linux = yield* service.select(
      scope,
      { task: "Linux tests", clientRequestId: "linux", requirements: { os: ["linux"] } },
      parent,
      {},
    );
    expect(linux.environmentId).toBe(there);
    const mac = yield* service.select(
      scope,
      { task: "Mac tests", clientRequestId: "mac", requirements: { os: ["darwin"] } },
      parent,
      {},
    );
    expect(mac.environmentId).toBe(here);
    yield* Ref.update(state, (s) => ({
      ...s,
      links: [{ ...link, executionOsRequirements: false }],
    }));
    const oldPeer = yield* service
      .select(
        scope,
        { task: "Linux tests", clientRequestId: "old", requirements: { os: ["linux"] } },
        parent,
        {},
      )
      .pipe(Effect.flip);
    expect(oldPeer.code).toBe("no_compatible_environment");
  }),
);

it.effect(
  "distinguishes incompatible execution from unavailable capacity without pinning work",
  () =>
    Effect.gen(function* () {
      const { service, state, sql } = yield* setup;
      yield* Ref.update(state, (s) => ({ ...s, remote: { ...snapshot, cpuUtilization: 1 } }));
      for (const [os, expected] of [
        ["linux", "no_available_capacity"],
        ["windows", "no_compatible_environment"],
      ] as const) {
        const error = yield* service
          .select(
            scope,
            { task: "Tests", clientRequestId: os, requirements: { os: [os] } },
            parent,
            {},
          )
          .pipe(Effect.flip);
        expect(error.code).toBe(expected);
      }
      expect(yield* sql`SELECT * FROM delegated_task_placements`).toHaveLength(0);
    }),
);

it.effect("validates explicit destinations and returns their actual platform", () =>
  Effect.gen(function* () {
    const { service, state } = yield* setup;
    const local = yield* service.check(scope, {
      task: "Tests",
      clientRequestId: "local",
      requirements: { os: ["darwin"] },
    });
    expect(local).toMatchObject({ environmentId: here, platform: { os: "darwin", arch: "arm64" } });
    const remote = {
      task: "Tests",
      clientRequestId: "remote",
      target: { environmentId: there },
      requirements: { os: ["linux"] as const },
    };
    expect(yield* service.check(scope, remote)).toMatchObject({
      environmentId: there,
      platform: { os: "linux" },
    });
    const mismatch = yield* service
      .check(scope, { ...remote, clientRequestId: "wrong", requirements: { os: ["windows"] } })
      .pipe(Effect.flip);
    expect(mismatch.code).toBe("no_compatible_environment");
    yield* Ref.update(state, (s) => ({
      ...s,
      links: [{ ...link, executionOsRequirements: false }],
    }));
    const old = yield* service
      .check(scope, { ...remote, clientRequestId: "old" })
      .pipe(Effect.flip);
    expect(old.code).toBe("capability_denied");
  }),
);

it.effect(
  "pins normalized requirements and refuses changes, removal and retargeting on retry",
  () =>
    Effect.gen(function* () {
      const { service, rebuild } = yield* setup;
      const input = {
        task: "Tests",
        clientRequestId: "retry",
        target: { environmentId: there },
        requirements: { os: ["linux", "windows"] as const },
      };
      yield* service.check(scope, input);
      const restarted = yield* rebuild;
      expect(
        yield* restarted.check(scope, {
          ...input,
          requirements: { os: ["windows", "linux", "linux"] },
        }),
      ).toMatchObject({ environmentId: there });
      for (const changed of [
        { ...input, requirements: { os: ["darwin"] as const } },
        { ...input, requirements: undefined },
        { ...input, target: { environmentId: here } },
      ]) {
        const error = yield* restarted.check(scope, changed).pipe(Effect.flip);
        expect(error.code).toBe("request_conflict");
      }
      const auto = {
        task: "Tests",
        clientRequestId: "auto-retry",
        requirements: { os: ["linux"] as const },
      };
      yield* service.select(scope, auto, parent, {});
      const changed = yield* restarted
        .select(scope, { ...auto, requirements: { os: ["darwin"] } }, parent, {})
        .pipe(Effect.flip);
      expect(changed.code).toBe("request_conflict");
    }),
);
