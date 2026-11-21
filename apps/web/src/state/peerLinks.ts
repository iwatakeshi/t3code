import { useRouter } from "@tanstack/react-router";
import { useGT, useMessages } from "gt-react";
import { toastManager } from "../components/ui/toast";
import { useAtomValue } from "@effect/atom-react";
import {
  linkableMachines,
  linkedEnvironmentNames,
  offersLinkedEnvironments,
} from "@t3tools/client-runtime/link-request";
import { createPeerLinkEnvironmentAtoms } from "@t3tools/client-runtime/state/peerLinks";
import { AuthAccessReadScope, type EnvironmentId, type ThreadId } from "@t3tools/contracts";
import { Atom } from "effect/reactivity";
import { useEffect, useEffectEvent, useState } from "react";

import { connectionAtomRuntime } from "../connection/runtime";
import { readEnvironment, useEnvironmentIdentities, useEnvironments } from "./environments";
import { environmentPresentations } from "./presentation";
import { relayEnvironmentDiscovery } from "./relay";
import { formatEnvironmentQueryError, useEnvironmentQuery } from "./query";
import { useEnvironmentsWithScope } from "./session";
import { useAtomCommand } from "./use-atom-command";

export const peerLinkEnvironment = createPeerLinkEnvironmentAtoms(connectionAtomRuntime);

/** Whether `environmentId` offers links, so its threads may name another machine. */
export const offersLinkedEnvironmentsAtom = Atom.family((environmentId: EnvironmentId) =>
  Atom.make((get) =>
    offersLinkedEnvironments(get(environmentPresentations.serverConfigsAtom).get(environmentId)),
  ).pipe(Atom.withLabel(`peer-links:offered:${environmentId}`)),
);

const offersNoLinksAtom = Atom.make(false).pipe(Atom.withLabel("peer-links:offered:none"));

/**
 * The machines `environmentId` links to, by name, while it offers links, and none otherwise.
 * Its agents can read threads there. Listing asks each linked machine whether it answers, so
 * every caller shares one briefly cached list.
 */
export function useLinkedEnvironments(
  environmentId: EnvironmentId | null,
): ReadonlyMap<EnvironmentId, string> {
  const offered = useAtomValue(
    environmentId === null ? offersNoLinksAtom : offersLinkedEnvironmentsAtom(environmentId),
  );
  const identities = useEnvironmentIdentities();
  const links = useEnvironmentQuery(
    offered && environmentId !== null
      ? peerLinkEnvironment.list({ environmentId, input: {} })
      : null,
  );
  return linkedEnvironmentNames({
    links: offered ? (links.data?.links ?? []) : [],
    environments: identities,
  });
}

export function usePeerLinkHosts() {
  const { environments } = useEnvironments();
  const supported = environments.filter(
    (environment) =>
      environment.connection.phase === "connected" &&
      offersLinkedEnvironments(environment.serverConfig),
  );
  const readable = useEnvironmentsWithScope(supported, AuthAccessReadScope);
  return supported.filter((environment) => readable.has(environment.environmentId));
}

/**
 * The user's other machines, for picking one to link `environmentId` to,
 * including those only on their T3 Connect account. Reads that environment's
 * links only while `enabled`, since listing them asks every linked machine
 * whether it answers.
 */
export function useLinkableMachines(environmentId: EnvironmentId, enabled: boolean) {
  const { environments } = useEnvironments();
  const discovery = useAtomValue(relayEnvironmentDiscovery.stateValueAtom);
  const refreshDiscovery = useAtomCommand(relayEnvironmentDiscovery.refresh, {
    reportFailure: false,
  });
  // Connect hosts are listed only once something asked for them, so ask on open.
  const discoverIfUnasked = useEffectEvent(() => {
    if (discovery.environments.size === 0 && !discovery.refreshing) void refreshDiscovery();
  });
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- discovery follows the picker's visibility, which a card's mount or a dialog's open sets
  useEffect(() => {
    if (enabled) discoverIfUnasked();
  }, [enabled]);
  const links = useEnvironmentQuery(
    enabled ? peerLinkEnvironment.list({ environmentId, input: {} }) : null,
  );
  return linkableMachines({
    environments,
    threadEnvironmentId: environmentId,
    links: links.data?.links ?? [],
    connect: discovery.environments,
  });
}

export function useOpenLinkedConversation(coordinatorId: EnvironmentId) {
  const command = useAtomCommand(peerLinkEnvironment.openConversation, { reportFailure: false });
  const [opening, setOpening] = useState(false);
  const router = useRouter();
  const gt = useGT();
  const messages = useMessages();
  const openConversation = async (target: {
    readonly environmentId: EnvironmentId;
    readonly threadId: ThreadId;
  }) => {
    const environment = readEnvironment(target.environmentId);
    if (
      environment !== null &&
      (environment.entry.target._tag !== "LinkedConnectionTarget" ||
        environment.connection.phase === "connected")
    )
      return true;
    const location = router.latestLocation;
    setOpening(true);
    const result = await command({ environmentId: coordinatorId, input: target });
    setOpening(false);
    if (router.latestLocation !== location) return false;
    if (result._tag === "Failure") {
      toastManager.add({
        type: "error",
        title: gt("Could not open conversation"),
        description: formatEnvironmentQueryError(result.cause, messages),
      });
      return false;
    }
    return true;
  };
  return { openConversation, opening };
}
