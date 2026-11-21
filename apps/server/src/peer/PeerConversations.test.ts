import * as Redacted from "effect/Redacted";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, it } from "@effect/vitest";
import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  AuthSettingsWriteScope,
  AuthSessionId,
  EnvironmentId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { FetchHttpClient, HttpServerRequest } from "effect/http";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as SessionStore from "../auth/SessionStore.ts";
import * as PeerLinks from "./PeerLinks.ts";
import * as PeerConversations from "./PeerConversations.ts";

const environmentId = EnvironmentId.make("worker");
const localSessionId = AuthSessionId.make("conversation");
const source = {
  sessionId: AuthSessionId.make("source"),
  subject: "user",
  method: "bearer-access-token" as const,
  scopes: [AuthOrchestrationReadScope, AuthOrchestrationOperateScope, AuthSettingsWriteScope],
};
const request = (path: string, token = "conversation-token") =>
  HttpServerRequest.fromWeb(
    new Request(`http://coordinator${path}`, {
      headers: { authorization: `Bearer ${token}`, cookie: "private=coordinator" },
    }),
  );
const setup = Effect.gen(function* () {
  const grants: Array<Parameters<EnvironmentAuth.EnvironmentAuth["Service"]["issueSession"]>[0]> =
    [];
  const forwarded: Array<{ url: string; authorization: string | null; cookie: string | null }> = [];
  const linkedAt = DateTime.makeUnsafe("2026-11-21T00:00:00Z");
  const link = {
    environmentId,
    label: "Worker",
    urls: ["http://worker"],
    access: "full-access" as const,
    linkedAt,
    expiresAt: DateTime.add(linkedAt, { days: 30 }),
    lastReachedAt: null,
    lastError: null,
  };
  let linked = true;
  let peerLink = true;
  const deps = Layer.mergeAll(
    Layer.mock(EnvironmentAuth.EnvironmentAuth)({
      authenticateMcpClient: () =>
        Effect.succeed({
          sessionId: source.sessionId,
          label: "Coordinator",
          access: "full-access",
          peerLink,
        }),
      authenticateHttpRequest: (input) =>
        Effect.succeed({
          ...source,
          sessionId:
            input.headers.cookie === undefined &&
            input.headers.authorization === "Bearer conversation-token"
              ? localSessionId
              : source.sessionId,
        }),
      issueSession: (input) => {
        grants.push(input);
        return Effect.succeed({
          sessionId: localSessionId,
          token: "conversation-token",
          method: "bearer-access-token",
          scopes: input?.scopes ?? [],
          subject: input?.subject ?? "",
          client: { deviceType: "bot" },
          expiresAt: linkedAt,
        });
      },
    }),
    Layer.mock(SessionStore.SessionStore)({
      cookieName: "test",
      legacyCookieName: undefined,
      awaitInvalidation: () => Effect.never,
    }),
    Layer.mock(PeerLinks.PeerLinks)({
      enabled: Effect.succeed(true),
      awaitInvalidation: () => Effect.never,
      resolve: () => Effect.succeed({ link, url: "http://worker", token: "private-peer-token" }),
      get: () => Effect.sync(() => (linked ? Option.some(link) : Option.none())),
    }),
    FetchHttpClient.layer.pipe(
      Layer.provide(
        Layer.succeed(FetchHttpClient.Fetch, async (input, init) => {
          const headers = new Headers(init?.headers);
          forwarded.push({
            url: String(input),
            authorization: headers.get("authorization"),
            cookie: headers.get("cookie"),
          });
          return Response.json(
            String(input).endsWith("/mcp/conversations")
              ? {
                  token: "private-worker-token",
                  scopes: [AuthOrchestrationReadScope, AuthOrchestrationOperateScope],
                }
              : { history: "worker conversation" },
            {
              headers: {
                "content-security-policy": "sandbox",
                "x-content-type-options": "nosniff",
              },
            },
          );
        }),
      ),
    ),
    NodeCrypto.layer,
  );
  const context = yield* Layer.build(PeerConversations.layer.pipe(Layer.provide(deps)));
  return {
    conversations: context,
    grants,
    forwarded,
    unlink: () => {
      linked = false;
    },
    outsideClient: () => {
      peerLink = false;
    },
  };
});

it.effect(
  "opens a scoped route without exposing either server credential and rejects other sessions",
  () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      const service = yield* PeerConversations.PeerConversations.pipe(
        Effect.provide(fixture.conversations),
      );
      const connection = yield* service.open(environmentId, source);
      expect(Redacted.value(connection.token)).toBe("conversation-token");
      expect(fixture.grants[0]?.scopes).toEqual([
        AuthOrchestrationReadScope,
        AuthOrchestrationOperateScope,
      ]);
      expect(
        (yield* service.proxy(
          request("/", "another-user"),
          connection.routeId,
          "/api/auth/session",
        )).status,
      ).toBe(403);
      expect(
        (yield* service.proxy(request("/"), connection.routeId, "/api/access/sessions")).status,
      ).toBe(403);
      expect(
        (yield* service.proxy(request("/"), connection.routeId, "//another-host/api/auth/session"))
          .status,
      ).toBe(400);
      const response = yield* service.proxy(request("/"), connection.routeId, "/api/auth/session");
      expect(response.status).toBe(200);
      expect(fixture.forwarded).toEqual([
        {
          url: "http://worker/mcp/conversations",
          authorization: "Bearer private-peer-token",
          cookie: null,
        },
        {
          url: "http://worker/api/auth/session",
          authorization: "Bearer private-worker-token",
          cookie: null,
        },
      ]);
      const asset = yield* service.proxy(
        HttpServerRequest.fromWeb(new Request("http://coordinator/")),
        connection.routeId,
        "/api/assets/signed/image.png",
      );
      expect(asset.status).toBe(200);
      expect(asset.headers["content-security-policy"]).toBe("sandbox");
      expect(asset.headers["x-content-type-options"]).toBe("nosniff");
      fixture.unlink();
      expect(
        (yield* service.proxy(request("/"), connection.routeId, "/api/auth/session")).status,
      ).toBe(410);
    }).pipe(Effect.scoped),
);

it.effect("only exchanges a peer link for scopes the requesting user already has", () =>
  Effect.gen(function* () {
    const fixture = yield* setup;
    const service = yield* PeerConversations.PeerConversations.pipe(
      Effect.provide(fixture.conversations),
    );
    const input = () =>
      HttpServerRequest.fromWeb(
        new Request("http://worker/mcp/conversations", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ scopes: [AuthOrchestrationReadScope] }),
        }),
      );
    expect((yield* service.issue(input())).status).toBe(200);
    expect(fixture.grants[0]?.scopes).toEqual([AuthOrchestrationReadScope]);
    fixture.outsideClient();
    expect((yield* service.issue(input())).status).toBe(403);
    expect(fixture.grants).toHaveLength(1);
  }).pipe(Effect.scoped),
);
