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
    const exited = yield* Deferred.make<number, ProviderSetupError>();
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
    let authorization: URL | undefined;
    yield* Queue.take(output).pipe(
      Effect.flatMap((data) => {
        if (authorization) return Effect.void;
        transcript = (transcript + data).slice(-32_768);
        const match = /https:\/\/accounts\.google\.com\/[^\s\x1b]+(?=[\s\x1b])/u.exec(transcript);
        if (!match) return Effect.void;
        const url = new URL(match[0]);
        const redirect = url.searchParams.get("redirect_uri");
        if (
          url.href.length > 16_384 ||
          url.searchParams.get("response_type") !== "code" ||
          !url.searchParams.get("state") ||
          redirect !== "https://antigravity.google/oauth-callback"
        ) {
          return Effect.fail(
            failure("start", "Antigravity returned an unsupported Google sign-in link."),
          );
        }
        authorization = url;
        transcript = "";
        return input.publish(
          {
            type: "browser",
            id: "google-login",
            url: url.href,
            requiresConsent: false,
            acceptsCallback: true,
          },
          (response) =>
            Effect.gen(function* () {
              const value =
                response.type === "credentials" ? response.values.callback?.trim() : undefined;
              if (!value || value.length > 16_384 || /[\x00-\x1f\x7f]/u.test(value)) {
                return yield* failure(
                  "complete",
                  "Paste the Google authorization code or complete return URL.",
                );
              }
              if (/^https?:/iu.test(value)) {
                const callback = yield* Effect.try({
                  try: () => new URL(value),
                  catch: () => failure("complete", "Paste the complete Google return URL."),
                });
                const states = callback.searchParams.getAll("state");
                const codes = callback.searchParams.getAll("code");
                if (
                  callback.origin + callback.pathname !== redirect ||
                  callback.username ||
                  callback.password ||
                  callback.hash ||
                  states.length !== 1 ||
                  states[0] !== url.searchParams.get("state") ||
                  codes.length !== 1 ||
                  !codes[0]
                ) {
                  return yield* failure(
                    "complete",
                    "This return URL does not belong to the current Google sign-in.",
                  );
                }
              } else if (!/^[A-Za-z0-9._~+/-]+$/u.test(value)) {
                return yield* failure(
                  "complete",
                  "Paste the Google authorization code or complete return URL.",
                );
              }
              yield* Effect.try({
                try: () => process.write(value + "\r"),
                catch: () =>
                  failure("complete", "The Antigravity sign-in process is no longer available."),
              });
            }),
        );
      }),
      Effect.forever,
      Effect.catch((error) => Deferred.fail(exited, error)),
      Effect.forkScoped,
    );
    if ((yield* Deferred.await(exited)) !== 0)
      return yield* failure(
        "start",
        "Antigravity sign-in did not finish successfully. Start sign-in again.",
      );
  },
);
