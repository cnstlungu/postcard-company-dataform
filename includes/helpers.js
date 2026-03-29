/**
 * Generates a surrogate key using MD5 hash of concatenated column values.
 * Equivalent to dbt_utils.generate_surrogate_key() — BigQuery implementation.
 *
 * @param {string[]} columns - Array of SQL column expressions or literals to hash
 * @returns {string} BigQuery SQL expression producing a hex MD5 surrogate key
 *
 * Example:
 *   generate_surrogate_key(["'0'", 'customer_id'])
 *   => TO_HEX(MD5(CONCAT(COALESCE(CAST('0' AS STRING), ''), '|', COALESCE(CAST(customer_id AS STRING), ''))))
 */
function generate_surrogate_key(columns) {
  const parts = columns.map(col => `COALESCE(CAST(${col} AS STRING), '')`);
  return `TO_HEX(MD5(CONCAT(${parts.join(", '|', ")})))`;
}

module.exports = { generate_surrogate_key };
