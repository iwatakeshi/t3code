import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE delegated_task_placements (
      request_namespace TEXT NOT NULL,
      parent_thread_id TEXT NOT NULL,
      client_request_id TEXT NOT NULL,
      target_json TEXT NOT NULL CHECK (json_valid(target_json)),
      PRIMARY KEY (request_namespace, parent_thread_id, client_request_id)
    )
  `;
});
