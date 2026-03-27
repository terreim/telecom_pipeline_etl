"""Schema Registry — single source of truth for table schemas.

Loads YAML contract files from the schemas/ directory and provides:
- Column lists by category (required, expected, passthrough)
- Schema drift detection (compare actual source vs contract)
- Validation rule lookup (references QualityProfile field names)
- ClickHouse type mapping

Usage
-----
    from common.schema_registry import REGISTRY

    contract = REGISTRY.get("subscriber_traffic")
    cols      = contract.all_source_columns()
    drift     = contract.detect_drift(set(df.columns))
    if drift.is_breaking:
        raise RuntimeError(drift.summary())
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

SCHEMAS_DIR = Path(__file__).parent / "schemas"


# ══════════════════════════════════════════════════════════════════════════════
# Data Classes
# ══════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class ColumnDef:
    """A single source column from the schema contract."""

    name: str
    category: str               # required | expected | passthrough
    pg_type: str = ""
    ch_type: str = ""
    validate: dict[str, Any] = field(default_factory=dict)
    soft_failure: bool = False


@dataclass(frozen=True)
class PipelineColumnDef:
    """A column added by the pipeline (derived, enrichment, or quality)."""

    name: str
    ch_type: str = ""
    expression: str = ""        # for derived columns
    source: str = ""            # for enrichment columns (e.g. dim_station)


@dataclass(frozen=True)
class SchemaDrift:
    """Result of comparing actual source columns against the contract."""

    table: str
    new_columns: frozenset[str]
    missing_required: frozenset[str]
    missing_expected: frozenset[str]
    missing_passthrough: frozenset[str]

    @property
    def is_breaking(self) -> bool:
        """True if any required column is missing — pipeline must stop."""
        return len(self.missing_required) > 0

    @property
    def has_drift(self) -> bool:
        """True if any difference exists between source and contract."""
        return bool(
            self.new_columns
            or self.missing_required
            or self.missing_expected
            or self.missing_passthrough
        )

    def summary(self) -> str:
        """Human-readable drift report for logs or error messages."""
        parts: list[str] = [f"Schema drift detected for '{self.table}':"]
        if self.missing_required:
            parts.append(
                f"  BREAKING — missing required: {sorted(self.missing_required)}"
            )
        if self.missing_expected:
            parts.append(
                f"  WARNING — missing expected (will NULL-fill): "
                f"{sorted(self.missing_expected)}"
            )
        if self.missing_passthrough:
            parts.append(
                f"  INFO — missing passthrough (skipped): "
                f"{sorted(self.missing_passthrough)}"
            )
        if self.new_columns:
            parts.append(
                f"  INFO — new unregistered columns (passthrough): "
                f"{sorted(self.new_columns)}"
            )
        return "\n".join(parts)

    def log(self) -> None:
        """Emit structured log messages for each drift category."""
        if not self.has_drift:
            logger.info(
                "Schema check OK for '%s' — no drift", self.table,
            )
            return

        if self.missing_required:
            logger.error(
                "BREAKING drift on '%s' — missing required columns: %s",
                self.table, sorted(self.missing_required),
            )
        if self.missing_expected:
            logger.warning(
                "Drift on '%s' — missing expected columns (NULL-filled): %s",
                self.table, sorted(self.missing_expected),
            )
        if self.missing_passthrough:
            logger.info(
                "Drift on '%s' — missing passthrough columns (skipped): %s",
                self.table, sorted(self.missing_passthrough),
            )
        if self.new_columns:
            logger.info(
                "Drift on '%s' — new unregistered columns (passthrough): %s",
                self.table, sorted(self.new_columns),
            )


# ══════════════════════════════════════════════════════════════════════════════
# Schema Contract
# ══════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class SchemaContract:
    """Parsed schema contract for a single table."""

    source_name: str
    primary_key: str
    time_column: str
    cdc_column: str
    columns: dict[str, ColumnDef]
    derived: dict[str, PipelineColumnDef]
    enrichment: dict[str, PipelineColumnDef]
    quality: dict[str, PipelineColumnDef]

    # ── Column Lists ──────────────────────────────────────────

    def required_columns(self) -> list[str]:
        """Columns the pipeline cannot function without."""
        return [c.name for c in self.columns.values()
                if c.category == "required"]

    def expected_columns(self) -> list[str]:
        """Columns used in validation/transforms — NULL-filled if absent."""
        return [c.name for c in self.columns.values()
                if c.category == "expected"]

    def passthrough_columns(self) -> list[str]:
        """Columns carried along if present, skipped if absent."""
        return [c.name for c in self.columns.values()
                if c.category == "passthrough"]

    def all_source_columns(self) -> list[str]:
        """All columns expected from the source (any category)."""
        return list(self.columns.keys())

    def soft_failure_columns(self) -> set[str]:
        """Columns whose validation failures are recoverable."""
        return {c.name for c in self.columns.values() if c.soft_failure}

    def validation_rules(self) -> dict[str, dict[str, Any]]:
        """Map of column_name -> validation rules dict."""
        return {
            c.name: c.validate
            for c in self.columns.values()
            if c.validate
        }

    # ── ClickHouse Helpers ────────────────────────────────────

    def ch_type_map(self) -> dict[str, str]:
        """Map every column (source + pipeline) to its ClickHouse type."""
        mapping: dict[str, str] = {}
        for col in self.columns.values():
            if col.ch_type:
                mapping[col.name] = col.ch_type
        for section in (self.quality, self.enrichment, self.derived):
            for col in section.values():
                if col.ch_type:
                    mapping[col.name] = col.ch_type
        return mapping

    def pg_type_map(self) -> dict[str, str]:
        """Map source column names to their Postgres types (columns with pg_type only)."""
        return {c.name: c.pg_type for c in self.columns.values() if c.pg_type}

    # ── Drift Detection ───────────────────────────────────────

    def detect_drift(self, actual_columns: set[str]) -> SchemaDrift:
        """Compare actual columns from a source DataFrame against this contract.

        Parameters
        ----------
        actual_columns
            Column names observed in the incoming data (e.g. from a Parquet
            file or a PostgreSQL query result).

        Returns
        -------
        SchemaDrift
            Categorised difference: new, missing-required, missing-expected,
            missing-passthrough.
        """
        contract_cols = set(self.columns.keys())

        return SchemaDrift(
            table=self.source_name,
            new_columns=frozenset(actual_columns - contract_cols),
            missing_required=frozenset(
                c for c in self.required_columns()
                if c not in actual_columns
            ),
            missing_expected=frozenset(
                c for c in self.expected_columns()
                if c not in actual_columns
            ),
            missing_passthrough=frozenset(
                c for c in self.passthrough_columns()
                if c not in actual_columns
            ),
        )


# ══════════════════════════════════════════════════════════════════════════════
# YAML Loader
# ══════════════════════════════════════════════════════════════════════════════

def _parse_contract(raw: dict) -> SchemaContract:
    """Parse a raw YAML dict into a SchemaContract."""
    table = raw["table"]

    columns: dict[str, ColumnDef] = {}
    for name, defn in raw.get("columns", {}).items():
        columns[name] = ColumnDef(
            name=name,
            category=defn["category"],
            pg_type=defn.get("pg_type", ""),
            ch_type=defn.get("ch_type", ""),
            validate=defn.get("validate", {}),
            soft_failure=defn.get("soft_failure", False),
        )

    derived: dict[str, PipelineColumnDef] = {}
    for name, defn in raw.get("derived", {}).items():
        derived[name] = PipelineColumnDef(
            name=name,
            ch_type=str(defn.get("ch_type", "")),
            expression=defn.get("expression", ""),
        )

    enrichment: dict[str, PipelineColumnDef] = {}
    for name, defn in raw.get("enrichment", {}).items():
        enrichment[name] = PipelineColumnDef(
            name=name,
            ch_type=str(defn.get("ch_type", "")),
            source=defn.get("source", ""),
        )

    quality: dict[str, PipelineColumnDef] = {}
    for name, defn in raw.get("quality", {}).items():
        quality[name] = PipelineColumnDef(
            name=name,
            ch_type=str(defn.get("ch_type", "")),
        )

    return SchemaContract(
        source_name=table["source_name"],
        primary_key=table["primary_key"],
        time_column=table["time_column"],
        cdc_column=table["cdc_column"],
        columns=columns,
        derived=derived,
        enrichment=enrichment,
        quality=quality,
    )


# ══════════════════════════════════════════════════════════════════════════════
# Registry
# ══════════════════════════════════════════════════════════════════════════════

class SchemaRegistry:
    """Loads all YAML contracts and provides lookup by source table name.

    Usage
    -----
        registry = SchemaRegistry()          # loads from default schemas/ dir
        contract = registry.get("subscriber_traffic")
        drift    = contract.detect_drift(set(df.columns))
    """

    def __init__(self, schemas_dir: Path = SCHEMAS_DIR) -> None:
        self._schemas_dir = schemas_dir
        self._contracts: dict[str, SchemaContract] = {}
        self._load_all()

    def _load_all(self) -> None:
        for yaml_path in sorted(self._schemas_dir.glob("*.yaml")):
            with open(yaml_path, "r", encoding="utf-8") as fh:
                raw = yaml.safe_load(fh)
            contract = _parse_contract(raw)
            self._contracts[contract.source_name] = contract
            logger.debug("Loaded schema contract: %s", contract.source_name)

    def get(self, table_name: str) -> SchemaContract:
        """Get the contract for a source table, or raise ValueError."""
        if table_name not in self._contracts:
            raise ValueError(
                f"No schema contract for '{table_name}'. "
                f"Available: {sorted(self._contracts.keys())}"
            )
        return self._contracts[table_name]

    def tables(self) -> list[str]:
        """List all registered source table names."""
        return sorted(self._contracts.keys())


# Module-level singleton — import and use directly
REGISTRY = SchemaRegistry()
