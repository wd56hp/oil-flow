"""ingestion_runs failed_count

Revision ID: a1b2c3d4e5f6
Revises: 7b268535ed50
Create Date: 2026-04-13

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, None] = "7b268535ed50"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "ingestion_runs",
        sa.Column("failed_count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.alter_column("ingestion_runs", "failed_count", server_default=None)


def downgrade() -> None:
    op.drop_column("ingestion_runs", "failed_count")
