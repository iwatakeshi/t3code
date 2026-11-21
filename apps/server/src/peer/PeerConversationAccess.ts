import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  type AuthEnvironmentScope,
  type AuthMcpClientAccess,
  AuthSessionId,
  sessionGrantsScope,
} from "@t3tools/contracts";

export const PEER_CONVERSATION_SUBJECT = "peer-conversation:";

export function conversationScopes(
  scopes: ReadonlyArray<AuthEnvironmentScope>,
  access: AuthMcpClientAccess,
): ReadonlyArray<AuthEnvironmentScope> {
  const session = { authenticated: true, scopes };
  return [
    ...(sessionGrantsScope(session, AuthOrchestrationReadScope)
      ? [AuthOrchestrationReadScope]
      : []),
    ...(access === "full-access" && sessionGrantsScope(session, AuthOrchestrationOperateScope)
      ? [AuthOrchestrationOperateScope]
      : []),
  ];
}

export function conversationParentSession(subject: string): AuthSessionId | undefined {
  return subject.startsWith(PEER_CONVERSATION_SUBJECT)
    ? AuthSessionId.make(subject.slice(PEER_CONVERSATION_SUBJECT.length))
    : undefined;
}
