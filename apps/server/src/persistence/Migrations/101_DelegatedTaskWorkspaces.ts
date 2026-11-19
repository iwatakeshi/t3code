import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`ALTER TABLE delegated_task_placements ADD COLUMN isolation TEXT NOT NULL DEFAULT 'shared' CHECK (isolation IN ('shared', 'worktree'))`;
  yield* sql`ALTER TABLE delegated_task_placements ADD COLUMN base_commit TEXT`;
});
