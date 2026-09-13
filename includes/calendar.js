/**
 * The calendar that dim_date materialises.
 *
 * Every generated sale has to fall inside this range, or fact_sales ends up
 * with bought_date_key values that join to no row in dim_date and
 * assert_fact_sales_date_key_valid fails.
 *
 * Keep in step with CALENDAR_START / CALENDAR_END in generator/generate.py,
 * which refuses to generate outside the range. CI compiles the project and
 * checks the compiled dim_date query against the generator's constants.
 */
const CALENDAR_START = "2010-01-01";
const CALENDAR_END = "2035-12-31";

module.exports = { CALENDAR_START, CALENDAR_END };
