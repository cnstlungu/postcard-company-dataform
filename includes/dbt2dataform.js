/**
 * Helpers for constructs dbt2dataform ported from dbt.
 */

/**
 * The column names and types of a relation, from INFORMATION_SCHEMA.
 * Pass the result of ref(), which also records the dependency:
 *
 *   SELECT * FROM ${dbt2dataform.information_schema_columns(ref("dim_date"))}
 *
 * @param {string} relation a quoted name as ref() returns it: `project.dataset.table`
 */
function information_schema_columns(relation) {
  const match = /^`([^`]+)\.([^.`]+)`$/.exec(relation);
  if (!match) {
    throw new Error(`expected a ref() like \`project.dataset.table\`, got ${relation}`);
  }
  const [, dataset, table] = match;
  return `(SELECT column_name, data_type FROM \`${dataset}.INFORMATION_SCHEMA.COLUMNS\` WHERE table_name = '${table}')`;
}

module.exports = { information_schema_columns };
