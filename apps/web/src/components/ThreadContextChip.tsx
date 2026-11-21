import { useAtomCommand } from "~/state/use-atom-command";
import { toastManager } from "./ui/toast";
import { useOpenLinkedConversation } from "~/state/peerLinks";
import { scopeProjectRef } from "@t3tools/client-runtime/environment";
import type { ThreadContextRecord, ProjectId, EnvironmentId, ThreadId } from "@t3tools/contracts";
import { Link, useNavigate } from "@tanstack/react-router";
import { MessagesSquareIcon } from "lucide-react";
import { useGT } from "gt-react";

import { resolveThreadOpenRoute } from "~/lib/sideChatPane";
import { buildThreadRouteParams } from "~/threadRoutes";
import { useProject, useThreadShell } from "~/state/entities";
import { threadTitlesRead, orchestrationEnvironment } from "~/state/orchestration";
import { useEnvironmentQuery } from "~/state/query";
import { ContextChip, ContextChipLabel } from "./ContextChip";
import { ProjectFavicon } from "./ProjectFavicon";
import { Tooltip, TooltipPopup, TooltipTrigger } from "./ui/tooltip";

export function ThreadContextChip(props: {
  record: Pick<ThreadContextRecord, "environmentId" | "threadId" | "title">;
  projectId?: ProjectId | null;
  copyMarkdown?: string;
  unreadable?: boolean;
  coordinatorId?: EnvironmentId;
  candidateThreadIds?: readonly ThreadId[];
}) {
  const gt = useGT();
  const readTitles = useAtomCommand(threadTitlesRead, { reportFailure: false });
  const { environmentId, threadId } = props.record;
  const { openConversation, opening } = useOpenLinkedConversation(
    props.coordinatorId ?? environmentId,
  );
  const shell = useThreadShell({ environmentId, threadId });
  const lookup = useEnvironmentQuery(
    shell === null && props.projectId === undefined
      ? orchestrationEnvironment.threadLinkMetadata({
          environmentId,
          input: { threadIds: [threadId] },
        })
      : null,
  );
  const metadata = lookup.data?.[threadId];
  const projectId = shell?.projectId ?? props.projectId ?? metadata?.projectId;
  const project = useProject(projectId ? scopeProjectRef(environmentId, projectId) : null);
  const title = shell?.title?.trim() || metadata?.title.trim() || props.record.title;
  const navigate = useNavigate();
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <ContextChip
            kind="thread"
            aria-busy={opening || undefined}
            {...(props.unreadable ? { state: "invalid" as const } : {})}
            render={
              <Link
                to="/$environmentId/$threadId"
                params={{ environmentId, threadId }}
                onClick={async (event) => {
                  if (props.coordinatorId !== undefined && props.coordinatorId !== environmentId) {
                    event.preventDefault();
                    if (!(await openConversation({ environmentId, threadId }))) return;
                    const candidates = props.candidateThreadIds ?? [threadId];
                    const titles = await readTitles({
                      environmentId,
                      input: { threadIds: candidates },
                    });
                    if (titles._tag === "Failure") {
                      toastManager.add({ type: "error", title: gt("Could not open conversation") });
                      return;
                    }
                    const found = candidates.find((id) => titles.value[id] !== undefined);
                    if (found === undefined) {
                      toastManager.add({ type: "info", title: gt("Thread no longer available") });
                      return;
                    }
                    void navigate({
                      to: "/$environmentId/$threadId",
                      params: { environmentId, threadId: found },
                    });
                    return;
                  }
                  if (shell?.lineage.sideChat !== true || event.button !== 0) return;
                  if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
                  // A side chat opens as a tab beside its parent, not as its own page.
                  event.preventDefault();
                  void navigate({
                    to: "/$environmentId/$threadId",
                    params: buildThreadRouteParams(
                      resolveThreadOpenRoute({ environmentId, threadId }, shell),
                    ),
                  });
                }}
                draggable={false}
              />
            }
            aria-label={gt("Thread, {title}", { title })}
            data-markdown-copy={props.copyMarkdown}
            className="no-underline"
          >
            {project ? (
              <ProjectFavicon project={project} className="size-[1em] self-center" />
            ) : (
              <MessagesSquareIcon />
            )}
            <ContextChipLabel>{opening ? gt("Opening conversation…") : title}</ContextChipLabel>
          </ContextChip>
        }
      />
      <TooltipPopup side="top">
        {props.unreadable
          ? gt(
              "On another machine, so this machine's agent can't read it. Remove it or switch back to send.",
            )
          : gt("Open thread")}
      </TooltipPopup>
    </Tooltip>
  );
}
