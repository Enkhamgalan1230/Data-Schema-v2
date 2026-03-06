from __future__ import annotations

from dataclasses import dataclass

from schema_discovery.normalization.models import NormalizedColumnProfile


@dataclass(frozen=True)
class PruningConfig:
    reject_null_ratio: float = 0.98
    reject_top1_ratio: float = 0.999

    pk_like_min_unique_ratio_non_null: float = 0.90
    pk_like_max_top1_ratio: float = 0.50

    other_key_like_min_unique_ratio_non_null: float = 0.70

    fk_like_min_non_null: int = 2
    fk_like_max_null_ratio: float = 0.98

    reject_free_text: bool = True
    reject_boolean: bool = True


def classify_column(p: NormalizedColumnProfile, cfg: PruningConfig) -> tuple[bool, bool, bool, bool, list[str]]:
    reasons: list[str] = []

    reject = False
    pk_like = False
    fk_like = False
    other_key_like = False

    if p.n_non_null == 0:
        reject = True
        reasons.append("all_null")

    if cfg.reject_boolean and p.dtype_family == "boolean":
        reject = True
        reasons.append("boolean_like")

    if cfg.reject_free_text and p.free_text_like:
        reject = True
        reasons.append("free_text_like")

    if p.null_ratio >= cfg.reject_null_ratio:
        reject = True
        reasons.append("too_many_nulls")

    if p.top1_ratio >= cfg.reject_top1_ratio:
        reject = True
        reasons.append("single_value_dominated")

    if reject:
        return False, False, False, True, reasons

    if (
        p.approx_unique_ratio_non_null >= cfg.pk_like_min_unique_ratio_non_null
        and p.top1_ratio <= cfg.pk_like_max_top1_ratio
        and p.n_non_null > 0
    ):
        pk_like = True
        reasons.append("pk_like")

    if (
        p.approx_unique_ratio_non_null >= cfg.other_key_like_min_unique_ratio_non_null
        and not pk_like
    ):
        other_key_like = True
        reasons.append("other_key_like")

    if (
        p.n_non_null >= cfg.fk_like_min_non_null
        and p.null_ratio <= cfg.fk_like_max_null_ratio
        and p.dtype_family in {"integer", "float", "text", "datetime"}
    ):
        fk_like = True
        reasons.append("fk_like")

    if not pk_like and not fk_like and not other_key_like:
        reject = True
        reasons.append("no_candidate_role")

    return pk_like, fk_like, other_key_like, reject, reasons