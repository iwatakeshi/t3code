import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { HttpRouter, HttpServerRequest } from "effect/http";
import * as PeerConversations from "./PeerConversations.ts";

export const layer = Layer.unwrap(
  Effect.gen(function* () {
    const conversations = yield* PeerConversations.PeerConversations;
    return Layer.mergeAll(
      HttpRouter.add(
        "POST",
        "/mcp/conversations",
        Effect.gen(function* () {
          return yield* conversations.issue(yield* HttpServerRequest.HttpServerRequest);
        }),
      ),
      HttpRouter.add(
        "*",
        "/api/peer-conversations/:routeId/*",
        Effect.gen(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest;
          const { routeId = "" } = yield* HttpRouter.params;
          const prefix = `/api/peer-conversations/${routeId}`;
          return yield* conversations.proxy(request, routeId, request.url.slice(prefix.length));
        }),
      ),
    );
  }),
);
