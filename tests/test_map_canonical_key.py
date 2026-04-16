"""Parametrized unit tests for ``_map_canonical_key`` (map version suffix stripping)."""

import pytest

from app.search.search import _map_canonical_key


@pytest.mark.parametrize(
    "raw_map,expected",
    [
        # ETF2L / RGL ``_f*`` tails
        ("cp_process_f12", "cp_process"),
        ("cp_process_f9a", "cp_process"),
        ("cp_gullywash_f9", "cp_gullywash"),
        # ``final`` / numeric tails
        ("cp_process_final", "cp_process"),
        ("cp_gullywash_final1", "cp_gullywash"),
        ("cp_metalworks_f5", "cp_metalworks"),
        # ``rcx`` / letter RC variants
        ("koth_product_rcx", "koth_product"),
        ("koth_product_rcb", "koth_product"),
        ("koth_product_final", "koth_product"),
        # ``rc\d+`` + optional letters
        ("pl_vigil_rc9", "pl_vigil"),
        ("pl_vigil_rc9a", "pl_vigil"),
        ("pl_vigil_rc10", "pl_vigil"),
        ("koth_cascade_rc1a", "koth_cascade"),
        # ``rc\d+`` + letter+digit suffix (e.g. ``rc17a3``) — must strip so ``*_pro_rc*`` maps group with base
        ("pl_vigil_rc17a3", "pl_vigil"),
        ("cp_granary_pro_rc17a3", "cp_granary"),
        ("cp_granary_pro", "cp_granary"),
        # Beta / alpha style tails
        ("cp_sultry_b8a", "cp_sultry"),
        ("koth_clearcut_b15d", "koth_clearcut"),
        ("koth_clearcut_b15c", "koth_clearcut"),
        ("cp_villa_b17a", "cp_villa"),
        # Trailing numeric segment (only when enough path segments)
        ("pl_upward_2", "pl_upward"),
        # Multi-segment map names — do not strip meaningful segments
        ("cp_5gorge", "cp_5gorge"),
        ("cp_5_gorge", "cp_5_gorge"),
        ("cp_badlands", "cp_badlands"),
        # Bare ``rc`` / ``r`` must not strip (``r(?:c)?\d+`` requires digits)
        ("cp_rc", "cp_rc"),
        # Sentinels / empty
        ("(unknown)", "(unknown)"),
        ("", "(unknown)"),
        ("   ", "(unknown)"),
        ("  (unknown)  ", "(unknown)"),
    ],
)
def test_map_canonical_key_parametrized(raw_map: str, expected: str) -> None:
    assert _map_canonical_key(raw_map) == expected
