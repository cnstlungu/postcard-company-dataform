#!/usr/bin/env bash
# Regenerates this Dataform project from postcard-company-datamart's dbt
# project with dbt2dataform, using the settings in dbt2dataform.yml.
#
# CI runs this and fails if it changes anything, so the committed project is
# always exactly what the pinned versions below produce. To move to a new
# datamart release, bump DATAMART_REF, run this, and commit the result.
#
#   DATAMART_DIR=../postcard-company-datamart  use a local checkout instead
#   DBT2DATAFORM=../dbt2dataform                use a local converter instead
set -euo pipefail

DATAMART_REF="${DATAMART_REF:-v1.3.0}"
DBT2DATAFORM_REF="${DBT2DATAFORM_REF:-v0.1.0}"

cd "$(dirname "$0")/.."
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

datamart="${DATAMART_DIR:-}"
if [ -z "$datamart" ]; then
    datamart="$work/datamart"
    git clone --quiet --depth 1 --branch "$DATAMART_REF" \
        https://github.com/cnstlungu/postcard-company-datamart.git "$datamart"
fi

# The datamart's CI requirements pin the dbt-core and dbt-duckdb it is tested
# with; dbt2dataform needs them to parse the project.
requirements="$datamart/requirements-ci.txt"
if [ -n "${DBT2DATAFORM:-}" ]; then
    # Editable, so edits to the checkout apply; uvx would reuse a stale build.
    uv run --quiet --project "$DBT2DATAFORM" --with-requirements "$requirements" \
        dbt2dataform "$datamart/postcard_company" . "$@"
else
    uvx --python 3.12 \
        --from "git+https://github.com/cnstlungu/dbt2dataform@${DBT2DATAFORM_REF}" \
        --with-requirements "$requirements" \
        dbt2dataform "$datamart/postcard_company" . "$@"
fi
