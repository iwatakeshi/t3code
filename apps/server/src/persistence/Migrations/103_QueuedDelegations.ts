import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE INDEX orchestration_v2_projection_subagents_queued_placement_idx
    ON orchestration_v2_projection_subagents(started_at, subagent_id)
    WHERE status IN ('pending', 'cancelled', 'interrupted')
      AND json_type(payload_json, '$.queuedPlacement') = 'object'
      AND (status = 'pending' OR json_extract(payload_json, '$.queuedPlacement.phase') = 'dispatching')
  `;
});
