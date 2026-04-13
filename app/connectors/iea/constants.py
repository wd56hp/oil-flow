"""Stable identifiers for IEA-shaped data (ingestion engine uses ``source`` + ``dataset``)."""

# Canonical ``TradeFlowRecord.source`` value for this connector.
IEA_SOURCE = "iea"

# Registered table / product identifiers (extend as licensed datasets are wired).
# Values are stored as ``TradeFlowRecord.dataset``.
DATASET_OIL_TRADE_TABLE = "iea-oil-trade-table"
DATASET_STUB = "iea-stub"
