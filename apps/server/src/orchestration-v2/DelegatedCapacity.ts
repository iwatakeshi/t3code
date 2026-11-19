import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

export class DelegatedCapacityError extends Schema.TaggedError<DelegatedCapacityError>()(
  "DelegatedCapacityError",
  { limit: Schema.Number, used: Schema.Number },
) {
  override get message(): string {
    return `This environment has no delegated task capacity (${this.used}/${this.limit}). No task was accepted or queued.`;
  }
}

const isDelegatedCapacityError = Schema.is(DelegatedCapacityError);

export function delegatedCapacityError(error: unknown): DelegatedCapacityError | undefined {
  const seen = new Set<unknown>();
  while (error !== null && typeof error === "object" && !seen.has(error)) {
    if (isDelegatedCapacityError(error)) return error;
    seen.add(error);
    error = "cause" in error ? error.cause : undefined;
  }
  return undefined;
}

// Count leaves so delegating a first local child transfers its parent's slot without deadlocking.
export const usedDelegatedCapacity = Effect.fn("DelegatedCapacity.used")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ readonly used: number }>`
    WITH RECURSIVE reserved AS (
      SELECT thread_id FROM orchestration_v2_projection_runs
      WHERE status IN ('preparing', 'queued', 'starting', 'running', 'waiting')
      UNION
      SELECT child_thread_id FROM orchestration_v2_projection_subagents
      WHERE origin = 'app_owned' AND status IN ('pending', 'running', 'waiting')
        AND child_thread_id IS NOT NULL
      UNION
      SELECT thread_id FROM orchestration_v2_effect_outbox
      WHERE effect_type IN ('provider-runtime.continue', 'provider-turn.restart')
        AND status IN ('pending', 'running')
    ), active AS (
      SELECT DISTINCT t.thread_id,
        CASE WHEN json_extract(t.payload_json, '$.lineage.relationshipToParent') = 'subagent'
          THEN json_extract(t.payload_json, '$.lineage.parentThreadId') END AS parent_id
      FROM reserved r
      JOIN orchestration_v2_projection_threads t ON t.thread_id = r.thread_id
      WHERE t.deleted_at IS NULL
        AND ((json_extract(t.payload_json, '$.lineage.relationshipToParent') = 'subagent'
          AND json_extract(t.payload_json, '$.creationSource') != 'provider')
          OR json_type(t.payload_json, '$.delegatedFrom') = 'object')
    ), ancestors(descendant, ancestor) AS (
      SELECT thread_id, parent_id FROM active WHERE parent_id IS NOT NULL
      UNION
      SELECT a.descendant, json_extract(t.payload_json, '$.lineage.parentThreadId')
      FROM ancestors a JOIN orchestration_v2_projection_threads t ON t.thread_id = a.ancestor
      WHERE json_extract(t.payload_json, '$.lineage.relationshipToParent') = 'subagent'
        AND json_extract(t.payload_json, '$.lineage.parentThreadId') IS NOT NULL
    )
    SELECT COUNT(*) AS used FROM active
    WHERE thread_id NOT IN (SELECT ancestor FROM ancestors)
  `;
  return rows[0]?.used ?? 0;
});
