import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  AuthSettingsWriteScope,
} from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";

import { conversationScopes } from "./PeerConversationAccess.ts";

it("intersects the user's permissions with the link's access without granting settings access", () => {
  const scopes = [
    AuthOrchestrationReadScope,
    AuthOrchestrationOperateScope,
    AuthSettingsWriteScope,
  ];
  expect(conversationScopes(scopes, "full-access")).toEqual([
    AuthOrchestrationReadScope,
    AuthOrchestrationOperateScope,
  ]);
  expect(conversationScopes(scopes, "read-only")).toEqual([AuthOrchestrationReadScope]);
  expect(conversationScopes([AuthOrchestrationReadScope], "full-access")).toEqual([
    AuthOrchestrationReadScope,
  ]);
});

it("does not turn a supervised agent link into an unsupervised client session", () => {
  for (const access of ["read-only", "approval-required", "auto-accept-edits", "auto"] as const) {
    expect(
      conversationScopes([AuthOrchestrationReadScope, AuthOrchestrationOperateScope], access),
    ).toEqual([AuthOrchestrationReadScope]);
  }
});
