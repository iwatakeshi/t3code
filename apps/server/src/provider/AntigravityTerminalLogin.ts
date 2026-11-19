import { ProviderSetupError, type ProviderInstanceId } from "@t3tools/contracts";
import * as PtyAdapter from "@t3tools/shared/PtyAdapter";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import type * as AcpSessionRuntime from "@t3tools/provider-acp/server/AcpSessionRuntime";
import type { AntigravityNativeLogin } from "./AntigravityAuth.ts";

export const runAntigravityTerminalLogin = Effect.fn("AntigravityTerminalLogin.run")(
  function* (input: {
    readonly instanceId: ProviderInstanceId;
    readonly spawn: AcpSessionRuntime.AcpSpawnInput & { readonly cwd: string };
    readonly pty: PtyAdapter.PtyAdapter["Service"] | null;
    readonly publish: Parameters<AntigravityNativeLogin>[0];
  }) {
    const failure = (operation: string, detail: string) =>
      new ProviderSetupError({ instanceId: input.instanceId, operation, detail });
    if (input.pty === null)
      return yield* failure(
        "start",
        "The Antigravity sign-in terminal is unavailable on this environment.",
      );
    const exited = yield* Deferred.make<number>();
    const output = yield* Queue.sliding<string>(64);
    const process = yield* input.pty
      .spawn({
        shell: input.spawn.command,
        args: [...input.spawn.args, "--login"],
        cwd: input.spawn.cwd,
        env: input.spawn.env ?? {},
        cols: 80,
        rows: 24,
      })
      .pipe(
        Effect.mapError(() =>
          failure("start", "Could not start the Antigravity sign-in terminal."),
        ),
      );
    const detachData = process.onData((data) => {
      Queue.offerUnsafe(output, data.slice(-16_384));
    });
    const detachExit = process.onExit(({ exitCode }) => {
      Deferred.doneUnsafe(exited, Effect.succeed(exitCode));
    });
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        detachData();
        detachExit();
        try {
          process.kill();
        } catch {}
      }),
    );
    let transcript = "";
    let outputOffset = 0;
    const update = () =>
      input.publish(
        { type: "terminal", id: "google-login", output: transcript, outputOffset },
        (response) =>
          response.type === "terminal"
            ? Effect.try({
                try: () => {
                  if (response.size) process.resize(response.size.cols, response.size.rows);
                  if (response.data)
                    process.write(PtyAdapter.ptyInput(response.data, response.binary));
                },
                catch: () =>
                  failure("respond", "The Antigravity sign-in terminal is no longer available."),
              })
            : Effect.fail(failure("respond", "Use the Antigravity sign-in terminal to continue.")),
      );
    yield* update();
    yield* Queue.take(output).pipe(
      Effect.flatMap((data) => {
        outputOffset += data.length;
        transcript = (transcript + data).slice(-16_384);
        return update();
      }),
      Effect.forever,
      Effect.forkScoped,
    );
    if ((yield* Deferred.await(exited)) !== 0)
      return yield* failure(
        "start",
        "Antigravity sign-in did not finish successfully. Start sign-in again.",
      );
  },
);
