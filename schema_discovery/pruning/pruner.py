from __future__ import annotations

from typing import List

from schema_discovery.normalization.models import NormalizedColumnProfile
from schema_discovery.pruning.models import PrunedColumnCandidate
from schema_discovery.pruning.rules import PruningConfig, classify_column


def prune_normalized_profiles(
    cfg: PruningConfig,
    normalized_profiles: List[NormalizedColumnProfile],
) -> List[PrunedColumnCandidate]:
    out: List[PrunedColumnCandidate] = []

    for p in normalized_profiles:
        pk_like, fk_like, other_key_like, reject, reasons = classify_column(p, cfg)

        out.append(
            PrunedColumnCandidate(
                table_name=p.table_name,
                column_name=p.column_name,
                dtype=p.dtype,
                dtype_family=p.dtype_family,
                canonical_types=p.canonical_types,
                pk_like=pk_like,
                fk_like=fk_like,
                other_key_like=other_key_like,
                reject=reject,
                reasons=reasons,
            )
        )

    return out