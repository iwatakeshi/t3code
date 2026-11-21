import * as Stream from "effect/Stream";
import { PeerLinkError } from "@t3tools/contracts";
import { requestGuarded } from "../rpc/client.ts";
import { type PeerLinkSummary, WS_METHODS } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import { Atom } from "effect/reactivity";

import * as EnvironmentRegistry from "../connection/registry.ts";
import * as EnvironmentSupervisor from "../connection/supervisor.ts";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { LinkedConnectionTarget } from "../connection/model.ts";
import { createEnvironmentRpcCommand, createEnvironmentRpcQueryAtomFamily } from "./runtime.ts";

export function createPeerLinkEnvironmentAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry.EnvironmentRegistry | R, E>,
) {
  const list = createEnvironmentRpcQueryAtomFamily(runtime, {
    label: "environment-data:peer-links:list",
    tag: WS_METHODS.peerLinksList,
    // Listing probes every linked environment, so a list stays fresh briefly.
    staleTimeMs: 10_000,
  });
  return {
    list,
    openConversation: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:peer-links:open-conversation",
      tag: WS_METHODS.peerLinksOpenConversation,
      concurrency: {
        mode: "singleFlight",
        key: ({ environmentId, input }) => JSON.stringify([environmentId, input.environmentId]),
      },
      execute: Effect.fn("peerLinks.openConversation")(function* (input, _atoms, environmentId) {
        const connection = yield* requestGuarded(WS_METHODS.peerLinksOpenConversation, input);
        const environments = yield* EnvironmentRegistry.EnvironmentRegistry;
        const supervisor = yield* EnvironmentSupervisor.EnvironmentSupervisor;
        const source = yield* SubscriptionRef.get(supervisor.prepared);
        if (Option.isNone(source))
          return yield* new PeerLinkError({
            reason: "unreachable",
            message: "The coordinator disconnected. Reconnect and retry.",
          });
        const httpUrl = new URL(source.value.httpBaseUrl);
        httpUrl.pathname = `/api/peer-conversations/${connection.routeId}`;
        httpUrl.search = "";
        httpUrl.hash = "";
        const wsUrl = new URL(httpUrl);
        wsUrl.protocol = wsUrl.protocol === "https:" ? "wss:" : "ws:";
        wsUrl.pathname += "/ws";
        yield* environments.registerLinked(
          new LinkedConnectionTarget({
            environmentId: connection.environmentId,
            label: connection.label,
            coordinatorId: environmentId,
            httpBaseUrl: httpUrl.toString(),
            wsBaseUrl: wsUrl.toString(),
            token: connection.token,
          }),
        );
        yield* environments
          .run(
            connection.environmentId,
            Effect.gen(function* () {
              const remote = yield* EnvironmentSupervisor.EnvironmentSupervisor;
              const state = yield* SubscriptionRef.changes(remote.state).pipe(
                Stream.filter((value) => value.phase === "connected" || value.lastFailure !== null),
                Stream.take(1),
                Stream.runHead,
              );
              if (Option.isSome(state) && state.value.lastFailure !== null) {
                return yield* new PeerLinkError({
                  reason: "unreachable",
                  message: state.value.lastFailure.message,
                });
              }
            }),
          )
          .pipe(
            Effect.mapError(
              () =>
                new PeerLinkError({
                  reason: "unreachable",
                  message:
                    "Could not connect to the linked conversation. Check the worker and retry.",
                }),
            ),
          );
        return connection;
      }),
    }),
    link: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:peer-links:link",
      tag: WS_METHODS.peerLinksLink,
      onSuccess: ({ environmentId }, registry) =>
        Effect.sync(() => registry.refresh(list({ environmentId, input: {} }))),
    }),
    // Leaves the list alone: a batch refreshes it once, when it ends (`runLinkBatch`).
    linkInBatch: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:peer-links:link-in-batch",
      tag: WS_METHODS.peerLinksLink,
    }),
    unlink: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:peer-links:unlink",
      tag: WS_METHODS.peerLinksUnlink,
      onSuccess: ({ environmentId }, registry) =>
        Effect.sync(() => registry.refresh(list({ environmentId, input: {} }))),
    }),
    // An agent's link card. One answer per card at a time.
    answerRequest: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:peer-links:answer-request",
      tag: WS_METHODS.peerLinksAnswerRequest,
      concurrency: {
        mode: "singleFlight",
        key: ({ environmentId, input }) =>
          JSON.stringify([environmentId, input.threadId, input.turnItemId]),
      },
      onSuccess: ({ environmentId }, registry) =>
        Effect.sync(() => registry.refresh(list({ environmentId, input: {} }))),
    }),
  };
}

const EXPIRY_WARNING_DAYS = 5;

export type PeerLinkHealth =
  | { readonly kind: "reachable"; readonly expiresInDays: number | null }
  | { readonly kind: "unreachable"; readonly detail: string | null }
  | { readonly kind: "failing"; readonly detail: string }
  | { readonly kind: "expired" };

/**
 * Listing only checks that the peer answers, so a link whose last call failed
 * (the peer revoked it, say) is `failing` with the recorded reason; the next
 * successful call clears it.
 */
export function peerLinkHealth(link: PeerLinkSummary, now: DateTime.Utc): PeerLinkHealth {
  if (link.status === "expired" || DateTime.isLessThanOrEqualTo(link.expiresAt, now)) {
    return { kind: "expired" };
  }
  if (link.status === "unreachable") return { kind: "unreachable", detail: link.lastError };
  if (link.lastError !== null) return { kind: "failing", detail: link.lastError };
  const days = Math.ceil(Duration.toDays(DateTime.distance(now, link.expiresAt)));
  return { kind: "reachable", expiresInDays: days <= EXPIRY_WARNING_DAYS ? days : null };
}
