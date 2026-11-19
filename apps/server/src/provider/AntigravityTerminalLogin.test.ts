import { assert, it } from "@effect/vitest";
import { ProviderInstanceId } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Exit from "effect/Exit";
import type * as PtyAdapter from "@t3tools/shared/PtyAdapter";
import { runAntigravityTerminalLogin } from "./AntigravityTerminalLogin.ts";

const authorizationUrl =
  "https://accounts.google.com/o/oauth2/v2/auth?response_type=code&state=test-state&redirect_uri=https%3A%2F%2Fantigravity.google%2Foauth-callback";
const callbackUrl = "https://antigravity.google/oauth-callback?code=return-code&state=test-state";

it.effect.each(["exit", "cancel", "failure"] as const)(
  "relays a chunked Google link privately and closes the native login process on %s",
  (end) =>
    Effect.gen(function* () {
      const ready = yield* Deferred.make<void>();
      const rendered = yield* Deferred.make<void>();
      let data: (value: string) => void = () => {};
      let exit: (value: PtyAdapter.PtyExitEvent) => void = () => {};
      let killed = 0;
      let detached = 0;
      const input: string[] = [];
      const login = yield* Effect.scoped(
        runAntigravityTerminalLogin({
          instanceId: ProviderInstanceId.make("antigravity-test"),
          spawn: {
            command: "/managed/antigravity",
            args: ["acp", "--gemini-dir", "/isolated"],
            cwd: "/isolated",
            env: { GEMINI_HOME: "/isolated" },
          },
          pty: {
            spawn: (spawn) =>
              Effect.sync(() => {
                assert.deepEqual(spawn.args, ["acp", "--gemini-dir", "/isolated", "--login"]);
                return {
                  pid: 1,
                  write: (value) => {
                    input.push(String(value));
                  },
                  resize: () => {},
                  kill: () => {
                    killed++;
                  },
                  onData: (callback) => {
                    data = callback;
                    Deferred.doneUnsafe(ready, Effect.void);
                    return () => {
                      detached++;
                    };
                  },
                  onExit: (callback) => {
                    exit = callback;
                    return () => {
                      detached++;
                    };
                  },
                };
              }),
          },
          publish: (interaction, respond) =>
            Effect.gen(function* () {
              assert.equal(interaction.type, "browser");
              if (interaction.type !== "browser") return;
              assert.equal(interaction.url, authorizationUrl);
              if (end === "exit") {
                for (const callback of [
                  callbackUrl.replace("test-state", "wrong"),
                  callbackUrl.replace("antigravity.google", "example.com"),
                  "code\ncommand",
                  "x".repeat(16_385),
                ]) {
                  assert.isTrue(
                    Exit.isFailure(
                      yield* Effect.exit(respond({ type: "credentials", values: { callback } })),
                    ),
                  );
                }
                assert.deepEqual(input, []);
              }
              yield* respond({
                type: "credentials",
                values: { callback: end === "exit" ? callbackUrl : "return-code" },
              });
              yield* Deferred.succeed(rendered, undefined);
            }),
        }),
      ).pipe(Effect.forkChild);
      yield* Deferred.await(ready);
      data("x".repeat(20_000) + "\r\nOpen Google: " + authorizationUrl.slice(0, 70));
      data(authorizationUrl.slice(70) + "\x1b[0m\r\nPaste the code:");
      yield* Deferred.await(rendered);
      if (end !== "cancel") {
        exit({ exitCode: end === "exit" ? 0 : 1, signal: null });
        const result = yield* Effect.exit(Fiber.join(login));
        assert.equal(Exit.isSuccess(result), end === "exit");
      } else yield* Fiber.interrupt(login);
      assert.deepEqual(input, [(end === "exit" ? callbackUrl : "return-code") + "\r"]);
      assert.equal(killed, 1);
      assert.equal(detached, 2);
    }),
);
