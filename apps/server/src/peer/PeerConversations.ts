import * as Headers from "effect/http/Headers";
import * as Redacted from "effect/Redacted";
import * as SecureRandom from "effect/crypto/SecureRandom";
import {
  AuthEnvironmentScopes,
  AuthOrchestrationReadScope,
  type AuthSessionId,
  type EnvironmentId,
  type PeerConversationConnection,
  PeerLinkError,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import {
  HttpClient,
  HttpClientRequest,
  HttpMethod,
  HttpServerRequest,
  HttpServerResponse,
} from "effect/http";
import * as Socket from "effect/socket/Socket";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as SessionStore from "../auth/SessionStore.ts";
import { upstreamWebSocketConstructor } from "../device/DeviceHubProxy.ts";
import * as PeerLinks from "./PeerLinks.ts";
import { conversationScopes, PEER_CONVERSATION_SUBJECT } from "./PeerConversationAccess.ts";

const Access = Schema.Struct({ scopes: AuthEnvironmentScopes });
const Grant = Schema.Struct({ token: Schema.String, scopes: AuthEnvironmentScopes });

interface ConversationRoute {
  readonly sessionId: AuthSessionId;
  readonly environmentId: EnvironmentId;
  readonly linkedAt: DateTime.Utc;
  readonly url: string;
  readonly token: string;
}

const unavailable = () =>
  new PeerLinkError({
    reason: "unreachable",
    message:
      "Could not open the conversation through the linked environment. Check its connection and retry.",
  });

export class PeerConversations extends Context.Service<
  PeerConversations,
  {
    readonly open: (
      environmentId: EnvironmentId,
      session: EnvironmentAuth.AuthenticatedSession,
    ) => Effect.Effect<PeerConversationConnection, PeerLinkError>;
    readonly issue: (
      request: HttpServerRequest.HttpServerRequest,
    ) => Effect.Effect<HttpServerResponse.HttpServerResponse>;
    readonly proxy: (
      request: HttpServerRequest.HttpServerRequest,
      routeId: string,
      path: string,
    ) => Effect.Effect<HttpServerResponse.HttpServerResponse, never, Scope.Scope>;
  }
>()("t3/peer/PeerConversations") {}

const make = Effect.gen(function* () {
  const scope = yield* Scope.Scope;
  const random = yield* SecureRandom.SecureRandom;
  const auth = yield* EnvironmentAuth.EnvironmentAuth;
  const sessions = yield* SessionStore.SessionStore;
  const links = yield* PeerLinks.PeerLinks;
  const http = yield* HttpClient.HttpClient;
  const routes = yield* Ref.make<ReadonlyMap<string, ConversationRoute>>(new Map());

  const open = Effect.fn("PeerConversations.open")(function* (
    environmentId: EnvironmentId,
    session: EnvironmentAuth.AuthenticatedSession,
  ) {
    const peer = yield* links.resolve(environmentId);
    const scopes = conversationScopes(session.scopes, peer.link.access);
    if (!scopes.includes(AuthOrchestrationReadScope)) {
      return yield* new PeerLinkError({
        reason: "pairing_rejected",
        message: "Conversation read access is required.",
      });
    }
    const response = yield* http
      .execute(
        HttpClientRequest.post(`${peer.url}/mcp/conversations`).pipe(
          HttpClientRequest.bearerToken(peer.token),
          HttpClientRequest.bodyJsonUnsafe({ scopes }),
        ),
      )
      .pipe(Effect.timeout(PeerLinks.probeTimeout(peer.url)), Effect.mapError(unavailable));
    if (response.status !== 200) {
      return yield* new PeerLinkError({
        reason: response.status === 404 ? "incompatible" : "pairing_rejected",
        message:
          response.status === 404
            ? "Update the linked environment to open its conversations through this machine."
            : "The linked environment refused conversation access. Check its link permissions.",
      });
    }
    const grant = yield* response.json.pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(Grant)),
      Effect.timeout(PeerLinks.probeTimeout(peer.url)),
      Effect.mapError(unavailable),
    );
    const local = yield* auth
      .issueSession({
        ttl: Duration.days(1),
        subject: `${PEER_CONVERSATION_SUBJECT}${session.sessionId}`,
        scopes: conversationScopes(scopes, peer.link.access).filter((scope) =>
          grant.scopes.includes(scope),
        ),
        label: `Conversation via ${peer.link.label}`,
      })
      .pipe(Effect.mapError(unavailable));
    const routeId = yield* random.uuidV4().pipe(Effect.mapError(unavailable));
    yield* Ref.update(routes, (current) =>
      new Map(current).set(routeId, {
        sessionId: local.sessionId,
        environmentId,
        linkedAt: peer.link.linkedAt,
        url: peer.url,
        token: grant.token,
      }),
    );
    yield* sessions.awaitInvalidation(local.sessionId).pipe(
      Effect.ensuring(
        Ref.update(routes, (current) => {
          const next = new Map(current);
          next.delete(routeId);
          return next;
        }),
      ),
      Effect.ignore,
      Effect.forkIn(scope),
    );
    return { environmentId, label: peer.link.label, routeId, token: Redacted.make(local.token) };
  });

  const issue = Effect.fn("PeerConversations.issue")(
    function* (request: HttpServerRequest.HttpServerRequest) {
      if (!(yield* links.enabled)) return HttpServerResponse.empty({ status: 403 });
      const caller = yield* auth.authenticateMcpClient(request);
      if (!caller.peerLink) return HttpServerResponse.empty({ status: 403 });
      const input = yield* request.json.pipe(Effect.flatMap(Schema.decodeUnknownEffect(Access)));
      const scopes = conversationScopes(input.scopes, caller.access);
      if (!scopes.includes(AuthOrchestrationReadScope))
        return HttpServerResponse.empty({ status: 403 });
      const grant = yield* auth.issueSession({
        subject: `${PEER_CONVERSATION_SUBJECT}${caller.sessionId}`,
        scopes,
        ttl: Duration.days(1),
        label: `Conversation from ${caller.label}`,
      });
      return HttpServerResponse.jsonUnsafe({ token: grant.token, scopes });
    },
    Effect.orElseSucceed(() => HttpServerResponse.empty({ status: 403 })),
  );

  const proxy = Effect.fn("PeerConversations.proxy")(
    function* (request: HttpServerRequest.HttpServerRequest, routeId: string, path: string) {
      const route = (yield* Ref.get(routes)).get(routeId);
      if (route === undefined) return HttpServerResponse.empty({ status: 410 });
      const link = yield* links.get(route.environmentId);
      if (
        Option.isNone(link) ||
        !DateTime.Equivalence(link.value.linkedAt, route.linkedAt) ||
        !(yield* links.enabled)
      ) {
        return HttpServerResponse.empty({ status: 410 });
      }
      const target = new URL(path, "http://conversation.invalid");
      if (target.origin !== "http://conversation.invalid")
        return HttpServerResponse.empty({ status: 400 });
      const socket = request.headers.upgrade?.toLowerCase() === "websocket";
      const metadata = target.pathname === "/.well-known/t3/environment";
      // These URLs carry their own file grant; image elements and native uploads send no bearer.
      const signedResource =
        ((request.method === "GET" || request.method === "HEAD") &&
          target.pathname.startsWith("/api/assets/")) ||
        (request.method === "POST" && target.pathname.startsWith("/api/attachments/upload/"));
      if (!metadata && !signedResource) {
        const credentialRequest = request.modify({
          headers: Headers.remove(request.headers, "cookie"),
        });
        const session = yield* socket
          ? auth.authenticateWebSocketUpgrade(credentialRequest, "rpc")
          : auth.authenticateHttpRequest(credentialRequest);
        if (session.sessionId !== route.sessionId) return HttpServerResponse.empty({ status: 403 });
        if (/^\/api\/auth\/websocket-ticket(?:\/rpc)?$/.test(path.split("?")[0]!)) {
          const ticket = yield* auth.issueWebSocketTicket(session, { use: "rpc" });
          return HttpServerResponse.jsonUnsafe({
            ticket: Redacted.value(ticket.ticket),
            expiresAt: DateTime.formatIso(ticket.expiresAt),
          });
        }
      }
      const changed = links.awaitInvalidation(route.environmentId, route.linkedAt);
      const invalidated = Effect.raceFirst(
        sessions.awaitInvalidation(route.sessionId),
        changed,
      ).pipe(Effect.ignore);
      const upstream = new URL(route.url);
      upstream.pathname = target.pathname;
      upstream.search = target.search;
      upstream.searchParams.delete("token");
      upstream.searchParams.delete("ticket");
      upstream.searchParams.delete("wsTicket");
      if (socket) {
        if (upstream.pathname !== "/ws") return HttpServerResponse.empty({ status: 403 });
        upstream.protocol = upstream.protocol === "https:" ? "wss:" : "ws:";
        const remote = yield* Socket.makeWebSocket(upstream.toString(), {
          headers: { authorization: `Bearer ${route.token}` },
          openTimeout: "10 seconds",
        }).pipe(Effect.provide(upstreamWebSocketConstructor));
        const local = yield* request.upgrade;
        yield* Effect.gen(function* () {
          const toRemote = yield* remote.writer;
          const toLocal = yield* local.writer;
          yield* Effect.raceFirst(
            Effect.raceFirst(pump(remote, toLocal), pump(local, toRemote)),
            invalidated,
          );
        }).pipe(Effect.scoped, Effect.ignoreCause);
        return HttpServerResponse.empty();
      }
      if (
        !(
          metadata ||
          upstream.pathname === "/api/auth/session" ||
          upstream.pathname.startsWith("/api/orchestration/") ||
          upstream.pathname.startsWith("/api/assets/") ||
          upstream.pathname.startsWith("/api/attachments/")
        )
      )
        return HttpServerResponse.empty({ status: 403 });
      const response = yield* HttpClient.withScope(http).execute(
        HttpClientRequest.make(HttpMethod.isHttpMethod(request.method) ? request.method : "GET")(
          upstream.toString(),
        ).pipe(
          HttpClientRequest.setHeaders({
            authorization: `Bearer ${route.token}`,
            ...(request.headers["x-t3-orchestration-protocol"] === undefined
              ? {}
              : { "x-t3-orchestration-protocol": request.headers["x-t3-orchestration-protocol"] }),
            ...(request.headers.range === undefined ? {} : { range: request.headers.range }),
            ...(request.headers["content-type"] === undefined
              ? {}
              : { "content-type": request.headers["content-type"] }),
          }),
          request.method === "GET" || request.method === "HEAD"
            ? (self) => self
            : HttpClientRequest.bodyStream(request.stream, {
                contentType: request.headers["content-type"],
              }),
        ),
      );
      return HttpServerResponse.stream(response.stream.pipe(Stream.interruptWhen(invalidated)), {
        status: response.status,
        headers: {
          "cache-control": "no-store",
          ...Object.fromEntries(
            [
              "content-type",
              "content-range",
              "accept-ranges",
              "content-disposition",
              "content-security-policy",
              "x-content-type-options",
            ].flatMap((name) => {
              const value = response.headers[name];
              return value === undefined ? [] : [[name, value]];
            }),
          ),
        },
      });
    },
    Effect.orElseSucceed(() => HttpServerResponse.empty({ status: 401 })),
  );

  return PeerConversations.of({ open, issue, proxy });
});

// One pump per socket direction, with backpressure; frame-level spans would flood traces.
const pump = Effect.fnUntraced(function* (source: Socket.Socket, sink: Socket.Writer) {
  const { pull } = yield* source.reader;
  while (true) yield* sink.writeAll(yield* pull);
});

export const layer = Layer.effect(PeerConversations, make);
