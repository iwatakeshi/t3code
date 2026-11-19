import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  EnvironmentId,
  MessageId,
  NodeId,
  ProviderDriverKind,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as Sqlite from "../persistence/Sqlite.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import { delegatedCapacityError, usedDelegatedCapacity } from "./DelegatedCapacity.ts";
import { idleThreadProjection, liveThreadShell } from "../mcp/McpToolAccess.testkit.ts";

const layer = EventSink.layer.pipe(
  Layer.provideMerge(Layer.merge(EventStore.layer, ProjectionStore.layer)),
  Layer.provideMerge(Sqlite.layerMemory),
);

it.effect("rejects a planned fork whose source was deleted before it committed", () =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const now = yield* DateTime.now;
    const sourceId = ThreadId.make("fork-source");
    const targetId = ThreadId.make("fork-target");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const source: OrchestrationV2AppThread = {
      id: sourceId,
      projectId: ProjectId.make("project"),
      title: "Source",
      createdBy: "user",
      creationSource: "web",
      providerInstanceId,
      modelSelection: { instanceId: providerInstanceId, model: "gpt-5" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: null,
      lineage: { rootThreadId: sourceId, parentThreadId: null, relationshipToParent: null },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      deletedAt: null,
      settledOverride: null,
      settledAt: null,
      snoozedUntil: null,
      snoozedAt: null,
      lastVisitedAt: null,
    };
    yield* sink.write({
      events: [
        {
          id: EventId.make("source-created"),
          type: "thread.created",
          threadId: sourceId,
          occurredAt: now,
          payload: source,
        },
      ],
    });
    const plannedFork = {
      commandId: CommandId.make("fork"),
      commandType: "thread.fork",
      threadId: targetId,
      acceptedAt: now,
      effects: [],
      events: [
        {
          id: EventId.make("fork-created"),
          type: "thread.created" as const,
          threadId: targetId,
          occurredAt: now,
          payload: {
            ...source,
            id: targetId,
            forkedFrom: { type: "run" as const, threadId: sourceId, runId: RunId.make("run") },
          },
        },
      ],
    };
    yield* sink.write({
      events: [
        {
          id: EventId.make("source-deleted"),
          type: "thread.deleted",
          threadId: sourceId,
          occurredAt: now,
          payload: { ...source, deletedAt: now },
        },
      ],
    });
    const rejected = yield* sink.commitCommand(plannedFork);
    assert.equal(rejected.receipt.status, "rejected");
    assert.isFalse(rejected.committed);
    const replayed = yield* sink.commitCommand(plannedFork);
    assert.deepEqual(replayed.receipt, rejected.receipt);
    assert.isFalse(replayed.committed);
    const missing = yield* projections.getThread(targetId).pipe(Effect.flip);
    assert.instanceOf(missing, ProjectionStore.ProjectionStoreThreadNotFoundError);
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "atomically admits delegated branches, replays accepted requests and releases durable capacity",
  () =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const eventsFor = (key: string, parent?: ThreadId, remote = false) => {
        const threadId = ThreadId.make(key);
        const thread = idleThreadProjection(liveThreadShell(threadId)).thread;
        const run: OrchestrationV2Run = {
          id: RunId.make(`run:${key}`),
          threadId,
          ordinal: 1,
          providerInstanceId: thread.providerInstanceId,
          modelSelection: thread.modelSelection,
          providerThreadId: null,
          userMessageId: MessageId.make(`message:${key}`),
          rootNodeId: null,
          activeAttemptId: null,
          status: "preparing",
          requestedAt: now,
          startedAt: null,
          completedAt: null,
          checkpointId: null,
          contextHandoffId: null,
        };
        const events: ReadonlyArray<OrchestrationV2DomainEvent> = [
          {
            id: EventId.make(`thread:${key}`),
            threadId,
            occurredAt: now,
            type: "thread.created",
            payload: {
              ...thread,
              ...(parent === undefined
                ? {}
                : {
                    lineage: {
                      ...thread.lineage,
                      parentThreadId: parent,
                      relationshipToParent: "subagent",
                    },
                  }),
              ...(remote
                ? {
                    delegatedFrom: {
                      environmentId: EnvironmentId.make("remote"),
                      threadId: ThreadId.make("parent-there"),
                      title: "Parent",
                    },
                  }
                : {}),
            },
          },
          {
            id: EventId.make(`created:${key}`),
            threadId,
            occurredAt: now,
            type: "run.created",
            payload: run,
          },
        ];
        return {
          commandId: CommandId.make(`launch:${key}`),
          commandType: "thread.create",
          threadId,
          acceptedAt: now,
          events,
          effects: [],
          maxDelegatedTasks: 1,
        };
      };
      const root = eventsFor("ordinary");
      yield* sink.commitCommand(root);
      assert.equal(yield* usedDelegatedCapacity(), 0);
      const first = eventsFor("first", root.threadId);
      const second = eventsFor("second", undefined, true);
      const results = yield* Effect.forEach(
        [first, second],
        (request) => sink.commitCommand(request).pipe(Effect.result),
        { concurrency: "unbounded" },
      );
      assert.equal(results.filter((result) => result._tag === "Success").length, 1);
      const refused = results.find((result) => result._tag === "Failure");
      assert.isDefined(refused);
      if (refused?._tag === "Failure") assert.isDefined(delegatedCapacityError(refused.failure));
      const accepted = results[0]?._tag === "Success" ? first : second;
      const rejected = accepted === first ? second : first;
      assert.isFalse((yield* sink.commitCommand(accepted)).committed);
      assert.equal(yield* usedDelegatedCapacity(), 1);
      const missing = yield* projections.getThread(rejected.threadId).pipe(Effect.flip);
      assert.instanceOf(missing, ProjectionStore.ProjectionStoreThreadNotFoundError);

      const fresh = yield* EventSink.layer.pipe(Layer.fresh, Layer.build);
      const restarted = yield* EventSink.EventSinkV2.pipe(Effect.provide(fresh));
      assert.isDefined(
        delegatedCapacityError(
          yield* restarted.withProviderBatch(restarted.commitCommand(rejected).pipe(Effect.flip)),
        ),
      );
      const nested = eventsFor("nested", accepted.threadId);
      yield* sink.commitCommand(nested);
      assert.equal(yield* usedDelegatedCapacity(), 1);
      const sibling = eventsFor("sibling", accepted.threadId);
      assert.isDefined(
        delegatedCapacityError(yield* sink.commitCommand(sibling).pipe(Effect.flip)),
      );
      const finish = (threadId: ThreadId, status: "completed" | "failed" | "cancelled") =>
        Effect.gen(function* () {
          const run = (yield* projections.getThreadProjection(threadId)).runs.toSorted(
            (a, b) => b.ordinal - a.ordinal,
          )[0]!;
          yield* sink.write({
            events: [
              {
                id: EventId.make(`finish:${threadId}:${run.id}`),
                type: "run.updated",
                threadId,
                occurredAt: now,
                payload: { ...run, status, completedAt: now },
              },
            ],
          });
        });
      yield* finish(nested.threadId, "failed");
      assert.equal(yield* usedDelegatedCapacity(), 1);
      yield* finish(accepted.threadId, "cancelled");
      assert.equal(yield* usedDelegatedCapacity(), 0);
      // A capacity refusal leaves its key reusable after a slot becomes available.
      yield* restarted.commitCommand(rejected);
      assert.equal(yield* usedDelegatedCapacity(), 1);
      yield* finish(rejected.threadId, "completed");
      assert.equal(yield* usedDelegatedCapacity(), 0);
      yield* sink.commitCommand(sibling);
      assert.equal(yield* usedDelegatedCapacity(), 1);
      const previous = (yield* projections.getThreadProjection(rejected.threadId)).runs[0]!;
      const followUp = {
        ...rejected,
        commandId: CommandId.make("follow-up"),
        commandType: "message.dispatch",
        events: [
          {
            id: EventId.make("follow-up-run"),
            threadId: rejected.threadId,
            occurredAt: now,
            type: "run.created" as const,
            payload: {
              ...previous,
              id: RunId.make("follow-up"),
              ordinal: 2,
              status: "queued" as const,
              completedAt: null,
            },
          },
        ],
      };
      assert.isDefined(
        delegatedCapacityError(yield* sink.commitCommand(followUp).pipe(Effect.flip)),
      );
      assert.equal((yield* projections.getThreadProjection(rejected.threadId)).runs.length, 1);
      yield* finish(sibling.threadId, "completed");
      yield* sink.commitCommand(followUp);
      assert.equal(yield* usedDelegatedCapacity(), 1);
      const paused = { ...eventsFor("paused", undefined, true), maxDelegatedTasks: 0 };
      assert.isDefined(delegatedCapacityError(yield* sink.commitCommand(paused).pipe(Effect.flip)));
      yield* sink.commitCommand({ ...eventsFor("manual"), maxDelegatedTasks: 0 });
      assert.equal(yield* usedDelegatedCapacity(), 1);
      yield* sink.commitCommand({ ...paused, maxDelegatedTasks: null });
      assert.equal(yield* usedDelegatedCapacity(), 2);
      yield* finish(paused.threadId, "completed");
      yield* finish(rejected.threadId, "failed");
      const recovery = eventsFor("recovering", undefined, true);
      yield* sink.commitCommand(recovery);
      const recoveringRun = (yield* projections.getThreadProjection(recovery.threadId)).runs[0]!;
      const recoveryEffect = `effect:restart-continuation:${recoveringRun.id}`;
      yield* sink.writeWithEffects({
        events: [
          {
            id: EventId.make("recovering-failed"),
            type: "run.updated",
            threadId: recovery.threadId,
            occurredAt: now,
            payload: { ...recoveringRun, status: "failed", completedAt: now },
          },
        ],
        effects: [
          {
            id: recoveryEffect,
            commandId: recovery.commandId,
            threadId: recovery.threadId,
            request: { type: "provider-runtime.continue", sourceRunId: recoveringRun.id },
          },
        ],
      });
      assert.equal(yield* usedDelegatedCapacity(), 1);
      assert.isDefined(
        delegatedCapacityError(
          yield* restarted
            .commitCommand({ ...eventsFor("during-recovery", undefined, true) })
            .pipe(Effect.flip),
        ),
      );
      const outboxContext = yield* EffectOutbox.layer.pipe(Layer.build);
      const outbox = yield* EffectOutbox.EffectOutboxV2.pipe(Effect.provide(outboxContext));
      yield* outbox.claimNext({ workerId: "test" });
      yield* outbox.succeed({ effectId: recoveryEffect, workerId: "test" });
      assert.equal(yield* usedDelegatedCapacity(), 0);
      const settling = eventsFor("settling", root.threadId);
      yield* sink.commitCommand(settling);
      const settlingRun = (yield* projections.getThreadProjection(settling.threadId)).runs[0]!;
      const task = {
        id: NodeId.make("task:settling"),
        threadId: root.threadId,
        runId: RunId.make("run:ordinary"),
        parentNodeId: NodeId.make("node:ordinary"),
        origin: "app_owned" as const,
        createdBy: "agent" as const,
        driver: ProviderDriverKind.make("codex"),
        providerInstanceId: settlingRun.providerInstanceId,
        providerThreadId: null,
        childThreadId: settling.threadId,
        nativeTaskRef: null,
        prompt: "Work",
        title: null,
        model: settlingRun.modelSelection.model,
        modelSelection: settlingRun.modelSelection,
        status: "running" as const,
        result: null,
        startedAt: now,
        completedAt: null,
        updatedAt: now,
      };
      yield* sink.write({
        events: [
          {
            id: EventId.make("settling-task"),
            type: "subagent.updated",
            threadId: root.threadId,
            occurredAt: now,
            payload: task,
          },
          {
            id: EventId.make("settling-run"),
            type: "run.updated",
            threadId: settling.threadId,
            occurredAt: now,
            payload: { ...settlingRun, status: "completed", completedAt: now },
          },
        ],
      });
      assert.equal(yield* usedDelegatedCapacity(), 1);
      yield* sink.write({
        events: [
          {
            id: EventId.make("settled-task"),
            type: "subagent.updated",
            threadId: root.threadId,
            occurredAt: now,
            payload: { ...task, status: "completed", completedAt: now, result: "Done" },
          },
        ],
      });
      assert.equal(yield* usedDelegatedCapacity(), 0);
    }).pipe(Effect.provide(layer)),
);
