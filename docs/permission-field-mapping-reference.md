# Permission Field Mapping Reference

This document describes the Community Edition field-mapping scan at a basic level.

## Purpose

`foggy.field.mapping.status` scans active read rules (`ir.rule`) on Odoo models that are exposed through the Foggy query models.

The scan answers one question:

- Is a field used by an Odoo rule currently mapped to a Foggy query field?

If a field is not mapped, the bridge may reject the related query path in fail-closed mode.

## What Gets Scanned

The Community Edition scan:

- iterates over models listed in `QM_TO_ODOO_MODEL`
- skips models whose Odoo modules are not installed
- reads active `perm_read` rules from `ir.rule`
- extracts top-level fields referenced by `domain_force`
- checks whether each field is known by the bridge mapping tables

The result is stored in `foggy.field.mapping.status`.

## Stored Fields

Each status row contains:

- `odoo_model`: Odoo model name
- `qm_model`: Foggy query model name
- `rule_name`: record rule name
- `rule_field`: field referenced by the rule
- `qm_field`: mapped Foggy field when available
- `status`: `mapped` or `unmapped`

## Current Mapping Sources

The basic scan checks fields against these built-in mapping tables:

- `DIRECT_FIELD_MAP`
- `HIERARCHY_FIELD_MAP`

This is intentionally a simple visibility feature for the Community Edition. It helps administrators see whether common permission fields are covered before they rely on a model in production.

## Limitations

The Community Edition scan is intentionally conservative:

- it only reports mapping presence, not remediation guidance
- it does not classify findings by severity
- it does not attempt automatic fixes
- it only extracts top-level fields from rule domains

## How To Use It

1. Open `Settings -> Foggy MCP`.
2. Click `Refresh Mapping`.
3. Open `View Mapping Status`.
4. Review rows with `status = unmapped`.

If unmapped fields appear, you can decide whether to add bridge support for those fields in a future release or avoid exposing that model until coverage is sufficient.
