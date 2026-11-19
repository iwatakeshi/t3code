import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE INDEX orchestration_v2_projection_runs_delegated_capacity_idx
    ON orchestration_v2_projection_runs(thread_id)
    WHERE status IN ('preparing', 'queued', 'starting', 'running', 'waiting')
  `;
  yield* sql`
    CREATE INDEX orchestration_v2_projection_subagents_delegated_capacity_idx
    ON orchestration_v2_projection_subagents(child_thread_id)
    WHERE origin = 'app_owned' AND status IN ('pending', 'running', 'waiting')
      AND child_thread_id IS NOT NULL
  `;
  yield* sql`
    CREATE INDEX orchestration_v2_effect_outbox_delegated_capacity_idx
    ON orchestration_v2_effect_outbox(thread_id)
    WHERE effect_type IN ('provider-runtime.continue', 'provider-turn.restart')
      AND status IN ('pending', 'running')
  `;
});
