/**
 * The dbt_utils macros this project used, as Dataform includes.
 * Written by dbt2dataform.
 */

/**
 * dbt_utils.generate_surrogate_key, emitting the SQL dbt-bigquery would, so
 * keys come out identical to the ones the dbt project built:
 *
 *   to_hex(md5(cast(concat(
 *     coalesce(cast(a as string), '_dbt_utils_surrogate_key_null_'), '-',
 *     coalesce(cast(b as string), '_dbt_utils_surrogate_key_null_')
 *   ) as string)))
 *
 * @param {string[]} fields SQL expressions, e.g. ["'0'", "customer_id"]
 */
function generate_surrogate_key(fields) {
  const parts = fields.map(
    (field) => `coalesce(cast(${field} as string), '_dbt_utils_surrogate_key_null_')`
  );
  return `to_hex(md5(cast(concat(${parts.join(", '-', ")}) as string)))`;
}

module.exports = { generate_surrogate_key };
