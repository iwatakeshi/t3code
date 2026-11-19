import { assert, it } from "@effect/vitest";
import { ProviderInstanceId } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Exit from "effect/Exit";
import type * as PtyAdapter from "@t3tools/shared/PtyAdapter";
import { runAntigravityTerminalLogin } from "./AntigravityTerminalLogin.ts";

it.effect.each(["exit", "cancel", "failure"] as const)(
  "closes the native login terminal on %s and bounds its output",
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
              if (interaction.type !== "terminal") return;
              if (interaction.output === "") {
                yield* respond({ type: "terminal", data: "return-code\r" });
                yield* Deferred.succeed(ready, undefined);
              } else {
                assert.equal(interaction.output.length, 16_384);
                assert.notInclude(interaction.output, "return-code");
                yield* Deferred.succeed(rendered, undefined);
              }
            }),
        }),
      ).pipe(Effect.forkChild);
      yield* Deferred.await(ready);
      data("x".repeat(20_000));
      yield* Deferred.await(rendered);
      if (end !== "cancel") {
        exit({ exitCode: end === "exit" ? 0 : 1, signal: null });
        const result = yield* Effect.exit(Fiber.join(login));
        assert.equal(Exit.isSuccess(result), end === "exit");
      } else yield* Fiber.interrupt(login);
      assert.deepEqual(input, ["return-code\r"]);
      assert.equal(killed, 1);
      assert.equal(detached, 2);
    }),
);
