/// <reference lib="dom" />
import { expect } from "e2e";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as HostProcess from "@t3tools/shared/HostProcess";

import { T3_HOME } from "../e2e.config.ts";
import { runPromise, test, WEB } from "./t3.ts";

test(
  "Antigravity sign-in offers a browser and return-code field instead of a terminal",
  WEB,
  async ({ t3, app, screen }) => {
    await t3.project("demo");
    const binary = `${T3_HOME}/agy`;
    await runPromise(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const node = yield* HostProcess.ExecutablePath;
        yield* fs.writeFileString(
          binary,
          `#!${node}
if (process.argv.includes('--version')) { console.log('1.5.12'); process.exit(0); }
if (process.argv.includes('--login')) {
  console.log('Open Google:\\nhttps://accounts.google.com/o/oauth2/v2/auth?client_id=test-client&response_type=code&state=test-state&redirect_uri=https%3A%2F%2Fantigravity.google%2Foauth-callback\\n');
  process.stdin.resume();
} else {
  const readline = require('node:readline');
  readline.createInterface({input:process.stdin}).on('line', line => {
    const request=JSON.parse(line);
    const result=request.method==='initialize' ? {protocolVersion:1,agentInfo:{name:'Antigravity',version:'1.5.12'},agentCapabilities:{},authMethods:[{id:'oauth-personal',name:'Google'}]} : {};
    console.log(JSON.stringify({jsonrpc:'2.0',id:request.id,result}));
  });
}
`,
        );
        yield* fs.chmod(binary, 0o700);
      }),
    );
    await t3.open("/settings/providers");
    await screen.getByRole("button", "Add provider", { exact: true }).first().tap();
    await screen.getByRole("radio", "Antigravity", { exact: true }).tap();
    await screen.getByRole("button", "Next", { exact: true }).tap();
    await screen.getByRole("button", "Next", { exact: true }).tap();
    await screen.getByRole("textbox", "Binary path", { exact: true }).fill(binary);
    await screen.getByRole("button", "Add instance", { exact: true }).tap();
    await screen.getByRole("button", "Sign in", { exact: true }).tap();
    await expect(screen.getByRole("button", "Open browser", { exact: true })).toBeVisible();
    await expect(
      screen.getByRole("textbox", "Authorization code or return URL", { exact: true }),
    ).toBeVisible();
    await expect(screen.getByLabel("Provider sign-in terminal")).toBeHidden();
    await app.screenshot("google-sign-in");
  },
);
