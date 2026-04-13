"""EIA crude-import diagnostics: business keys and duplicate detection (CLI / dry-run)."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import Any

from app.schemas.trade_flow import TradeFlowRecord


def trade_flow_business_key(rec: TradeFlowRecord) -> tuple[Any, ...]:
    """Stable tuple matching ingestion uniqueness (measure fields excluded)."""
    return (
        rec.source,
        rec.dataset,
        rec.period_date,
        rec.reporter_country,
        rec.partner_country,
        rec.commodity,
        rec.flow_direction,
    )


def duplicate_business_key_report(
    records: Sequence[TradeFlowRecord],
    *,
    top_n: int = 25,
) -> dict[str, Any]:
    """
    Count unique keys and list the top ``top_n`` keys that appear more than once.

    For each duplicate key, include count and one sample row's lineage + quantity.
    """
    key_to_indices: dict[tuple[Any, ...], list[int]] = defaultdict(list)
    for i, rec in enumerate(records):
        key_to_indices[trade_flow_business_key(rec)].append(i)

    duplicate_counts = {k: len(v) for k, v in key_to_indices.items() if len(v) > 1}
    sorted_dups = sorted(duplicate_counts.items(), key=lambda kv: -kv[1])[:top_n]

    top_duplicates: list[dict[str, Any]] = []
    for key, count in sorted_dups:
        sample = records[key_to_indices[key][0]]
        top_duplicates.append(
            {
                "count": count,
                "business_key": {
                    "source": key[0],
                    "dataset": key[1],
                    "period_date": key[2].isoformat() if key[2] is not None else None,
                    "reporter_country": key[3],
                    "partner_country": key[4],
                    "commodity": key[5],
                    "flow_direction": key[6],
                },
                "sample": {
                    "eia_origin_id": sample.eia_origin_id,
                    "eia_destination_id": sample.eia_destination_id,
                    "eia_grade_id": sample.eia_grade_id,
                    "quantity": str(sample.quantity) if sample.quantity is not None else None,
                    "quantity_unit": sample.quantity_unit,
                },
            }
        )

    rows_in_duplicate_groups = sum(duplicate_counts.values())

    return {
        "normalized_row_count": len(records),
        "unique_business_keys": len(key_to_indices),
        "keys_with_duplicates": len(duplicate_counts),
        "rows_in_duplicate_key_groups": rows_in_duplicate_groups,
        "top_duplicate_keys": top_duplicates,
    }
