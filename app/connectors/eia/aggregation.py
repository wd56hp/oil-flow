"""Collapse EIA crude-import rows that share the canonical business key (e.g. by destination)."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal

from app.connectors.eia.diagnostics import trade_flow_business_key
from app.schemas.trade_flow import TradeFlowRecord


@dataclass
class CrudeImportAggregateSummary:
    """Counts before/after merging rows that share the same canonical key."""

    normalized_out_before_aggregation: int
    aggregated_out: int
    # Groups with 2+ input rows merged into one.
    groups_collapsed: int
    # First row's unit kept when units disagreed.
    quantity_unit_inconsistent_groups: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "normalized_out_before_aggregation": self.normalized_out_before_aggregation,
            "aggregated_out": self.aggregated_out,
            "groups_collapsed": self.groups_collapsed,
            "quantity_unit_inconsistent_groups": self.quantity_unit_inconsistent_groups,
        }


def aggregate_eia_crude_imports_for_canonical(
    records: Sequence[TradeFlowRecord],
) -> tuple[list[TradeFlowRecord], CrudeImportAggregateSummary]:
    """
    One :class:`TradeFlowRecord` per canonical business key.

    - Sums ``quantity`` (ignores nulls; all-null → null).
    - Uses the single ``quantity_unit`` when all agree; otherwise first non-null and counts mismatch.
    - ``observed_at`` = max within the group.
    - ``eia_destination_id`` cleared (multi-destination rolled up).
    - ``eia_origin_id`` / ``eia_grade_id`` taken from the first row in each group (stable order).
    """
    before = len(records)
    if before == 0:
        return [], CrudeImportAggregateSummary(0, 0, 0, 0)

    groups: dict[tuple, list[TradeFlowRecord]] = defaultdict(list)
    for r in records:
        groups[trade_flow_business_key(r)].append(r)

    out: list[TradeFlowRecord] = []
    groups_collapsed = 0
    unit_mismatch_groups = 0

    for _key, group in groups.items():
        if len(group) > 1:
            groups_collapsed += 1

        base = group[0]
        qty_vals = [r.quantity for r in group if r.quantity is not None]
        total_qty: Decimal | None = None if not qty_vals else sum(qty_vals, Decimal(0))

        unique_units: list[str] = []
        for r in group:
            u = (r.quantity_unit or "").strip()
            if not u:
                continue
            if u not in unique_units:
                unique_units.append(u)
        if len(unique_units) == 1:
            unit = unique_units[0]
        elif len(unique_units) == 0:
            unit = None
        else:
            unit = unique_units[0]
            unit_mismatch_groups += 1

        observed_at = max(r.observed_at for r in group)

        merged = base.model_copy(
            update={
                "quantity": total_qty,
                "quantity_unit": unit,
                "observed_at": observed_at,
                "eia_destination_id": None,
            }
        )
        out.append(merged)

    out.sort(key=trade_flow_business_key)
    after = len(out)
    summary = CrudeImportAggregateSummary(
        normalized_out_before_aggregation=before,
        aggregated_out=after,
        groups_collapsed=groups_collapsed,
        quantity_unit_inconsistent_groups=unit_mismatch_groups,
    )
    return out, summary
