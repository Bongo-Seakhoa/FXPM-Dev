"""Invented software-only Source303 metadata and causal history checks."""
from __future__ import annotations

import copy
import json
import math
from typing import Any, Mapping, Optional, Set

import numpy as np
import pandas as pd
import pytest

import pm_core
import pm_pipeline as pipeline
import pm_production_search_planner as planner
import pm_state_contract as state_contract
from pm_strategies import StrategyRegistry, resolve_strategy_parameters


@pytest.fixture(scope="module")
def source_metadata():
    with planner._process_verification_scope():
        legacy = planner._noise101_legacy_domains()
        metadata = planner.noise101_process_metadata()
    old = {"schema_version": planner.PROCESS_SCHEMA,
        "candidate_domain_sha256": state_contract.STRATEGY_CANDIDATE_DOMAIN_SHA256,
        "state_contract_registry_sha256": state_contract.DEFAULT_AUTHORITY.registry_hash,
        "semantics": planner._process_semantics(legacy)}
    return metadata, old


# Accepted cur303 sampler, copied verbatim apart from this name and qualified imports.
def _legacy_nondefault_parameters(
    strategy_name: str,
    *,
    symbol: str,
    timeframe: str,
    regime: str,
    variant_ordinal: int = 0,
    already_emitted: Optional[Set[str]] = None,
    domain: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    """One deterministic, outcome-blind non-default grid point for a strategy.

    ``variant_ordinal`` selects which point, so a strategy can contribute several
    distinct designs to a cell plan without any of them depending on an outcome.
    The current edition traverses a seeded coprime permutation of legal points.
    """
    if domain is None:
        domain = planner._strategy_domain(strategy_name, 0)
    if domain.get("strategy_id") != strategy_name:
        planner._fail("VARIANT_STRATEGY_OUTSIDE_DOMAIN", strategy_name)
    if domain.get("schema_version") != planner.DOMAIN_SCHEMA:
        planner._fail("RETAINED_PROCESS_IS_READ_ONLY")
    seen = already_emitted if already_emitted is not None else set()
    if domain.get("schema_version") == planner.DOMAIN_SCHEMA:
        structured = domain.get("parameter_domain")
        baseline = planner.validate_parameter_domain_member(structured, domain.get("defaults"))
        count = structured["point_count"]
        if type(variant_ordinal) is not int or not 0 <= variant_ordinal < count - 1:
            planner._fail("NONDEFAULT_PARAMETER_POINT_UNAVAILABLE", strategy_name)
        digest = planner._cell_hash(
            symbol, timeframe, regime, strategy_name, "legal-domain-permutation",
            seed=planner.GROUPED_PARAMETER_DESIGN_SEED,
        )
        start = int(digest[:32], 16) % count
        stride = 1 + int(digest[32:], 16) % (count - 1)
        while math.gcd(stride, count) != 1:
            stride = 1 if stride == count - 1 else stride + 1
        default_ordinal = planner.parameter_domain_default_ordinal(structured)
        selected = -1
        for offset in range(variant_ordinal + 2):
            ordinal = (start + offset * stride) % count
            if ordinal == default_ordinal:
                continue
            selected += 1
            if selected != variant_ordinal:
                continue
            wire = planner.parameter_domain_point(structured, ordinal)
            base = planner.StrategyRegistry.get(strategy_name)
            runtime = planner.decode_parameter_mapping(wire, base.get_default_params())
            normalized = planner.canonicalize_parameter_mapping(
                planner.StrategyRegistry.get(strategy_name, **runtime).get_params()
            )
            if normalized != wire or normalized == baseline or planner._sha(normalized) in seen:
                planner._fail("NONDEFAULT_PARAMETER_MATERIALIZATION_DRIFT", strategy_name)
            planner.validate_parameter_domain_member(structured, normalized)
            seen.add(planner._sha(normalized))
            return normalized
        planner._fail("NONDEFAULT_PARAMETER_POINT_UNAVAILABLE", strategy_name)


def test_noise101_catalogue_is_deduplicated_and_legacy_authority_stays_separate(source_metadata):
    metadata, old = source_metadata
    edition = planner.source_edition_info(metadata)
    catalogue = metadata["semantics"]["candidate_catalogue"]
    assert edition.strategy_count == 101 and edition.attempt_count == 303
    assert len(edition.to_dict()) == 14
    assert len(catalogue["domain_pool"]) == 106
    assert old["candidate_domain_sha256"] == state_contract.STRATEGY_CANDIDATE_DOMAIN_SHA256
    assert planner.process_attempt_count(old) == 300
    assert state_contract.DEFAULT_AUTHORITY.registry_hash != edition.state_registry_sha256
    for timeframe in planner.TIMEFRAMES:
        domains = planner._domain_by_name(metadata, timeframe=timeframe)
        assert list(domains) == list(edition.ordered_strategy_ids)
        assert domains[planner.NOISE101_STRATEGY_ID]["defaults"]["bar_minutes"] == planner.StrategyRegistry.get(planner.NOISE101_STRATEGY_ID)._BAR_MINUTES[timeframe]
        assert catalogue["rosters_by_timeframe"][timeframe]["domain_sha256_by_strategy_ordinal"][:100] == catalogue["rosters_by_timeframe"]["M5"]["domain_sha256_by_strategy_ordinal"][:100]


@pytest.mark.parametrize("timeframe", planner.TIMEFRAMES)
def test_old100_defaults_and_all200_variants_are_byte_identical(source_metadata, timeframe):
    metadata, old = source_metadata
    old_domains = planner._domain_by_name(old)
    designs = planner._attempt_designs(metadata, symbol="US100", timeframe=timeframe, regime="TREND")
    assert len(designs) == 303
    retained = [row for row in designs if row["strategy_id"] != planner.NOISE101_STRATEGY_ID]
    expected = [{"design_role": "REGISTRY_DEFAULT", "strategy_id": name,
        "parameters": dict(domain["defaults"])} for name, domain in old_domains.items()]
    emitted = {}
    for name, ordinal in planner._variant_strategy_names(list(old_domains.values()), symbol="US100", timeframe=timeframe, regime="TREND"):
        expected.append({"design_role": "DETERMINISTIC_NONDEFAULT_GRID_POINT", "strategy_id": name,
            "parameters": _legacy_nondefault_parameters(name, symbol="US100", timeframe=timeframe,
                regime="TREND", variant_ordinal=ordinal, already_emitted=emitted.setdefault(name, set()),
                domain=old_domains[name])})
    assert len(expected) == len(retained) == 300
    assert planner._canonical_bytes(retained) == planner._canonical_bytes(expected)


@pytest.mark.parametrize("field", ["timeframe", "domain_roster_sha256"])
def test_source_context_refuses_crossed_identity(source_metadata, field):
    metadata, _ = source_metadata
    context = planner.source_edition_context_for_cell(metadata, "H1")
    assert context["noise_state_sha256"] is None
    assert len(context) == 8
    context[field] = "M5" if field == "timeframe" else "0" * 64
    with pytest.raises((ValueError, planner.ProductionSearchPlannerError)):
        planner.verify_source_edition_context(metadata, "H1", context)


def test_noise101_d1_and_session_h4_keep_all_three_structural_zero_attempts(source_metadata):
    metadata, _ = source_metadata
    for timeframe in ("D1", "H4"):
        rows = [row for row in planner._attempt_designs(metadata, symbol="US100", timeframe=timeframe,
            regime="TREND") if row["strategy_id"] == planner.NOISE101_STRATEGY_ID]
        assert len(rows) == 3
        for row in rows:
            result = planner.noise101_structural_disposition(timeframe, "US100", row["parameters"])
            assert result["structurally_zero_signal"] is True
            assert result["removed_from_declared_roster"] is False


def _invented_recorded_history():
    labels = []
    opens = []
    closes = []
    for day_number, day in enumerate(pd.date_range("2025-06-01", periods=28, tz="Europe/Budapest")):
        if day_number in {5, 13, 21}:
            continue
        for hour in range(24):
            if day_number % 3 == 0 and hour in {2, 8, 19}:
                continue
            labels.append(day + pd.Timedelta(hours=hour))
            opened = 100.0 + day_number * 0.02
            opens.append(opened)
            closes.append(opened * (1.0 + (((hour + day_number) % 5) - 2) * 0.004))
    return pd.DataFrame({"Open": opens, "Close": closes}, index=pd.DatetimeIndex(labels))


def _invented_checkpoint(source_metadata):
    metadata, _ = source_metadata
    defaults, _, _ = resolve_strategy_parameters(StrategyRegistry.get(planner.NOISE101_STRATEGY_ID), "H1", defaults_only=True)
    strategy = StrategyRegistry.get(planner.NOISE101_STRATEGY_ID, **{**defaults, "lookback": 7, "band_k": 0.5})
    frame = _invented_recorded_history()
    split = len(frame) - 36
    prefix = frame.iloc[:split]
    spec = strategy.session_spec("EURUSD")
    inside, keys = pm_core._recorded_session_arrays(prefix.index, spec)
    relative = (prefix.index.hour * 60 + prefix.index.minute - spec[0]) % 1440
    # Invented software boundary identity, never used as a production source receipt.
    context = planner.source_edition_context_for_cell(metadata, "H1", state_boundary_source_sha256=planner._sha({"invented_source": "recorded-history-checkpoint", "prefix": prefix.index[-1].isoformat()}))
    bundle = state_contract.encode_noise101_replay_state(source_context=context,
        implementation_sha256=planner._callable_sha256(type(strategy).generate_signals),
        symbol="EURUSD", parameters=strategy.params, session_spec=spec,
        session_keys=[int(value) for value in keys], in_session=[bool(value) for value in inside],
        slot_minutes=[int(value) + strategy.params["bar_minutes"] for value in relative],
        open_prices=prefix["Open"].tolist(), close_prices=prefix["Close"].tolist(),
        recorded_opens=[label.isoformat() for label in prefix.index], recorded_timezone=str(prefix.index.tz))
    return strategy, frame, split, bundle


def test_recorded_session_split_reopen_matches_direct_signals_with_missing_slots_and_gaps(source_metadata):
    strategy, frame, split, bundle = _invented_checkpoint(source_metadata)
    assert any(value is None for row in bundle["state"]["history_absolute_moves"] for value in row)
    assert len(bundle["state"]["history_session_keys"]) == 7
    assert "+02:00" in bundle["state"]["last_recorded_open"]
    reopened = json.loads(json.dumps(bundle, allow_nan=False))
    actual = planner.resume_noise101_recorded_session_signals(reopened, frame.iloc[split:], strategy=strategy, symbol="EURUSD")
    expected = strategy.generate_signals(frame, "EURUSD").iloc[split:]
    assert np.count_nonzero(expected.to_numpy()) > 0
    pd.testing.assert_series_equal(actual, expected)


def test_recorded_session_checkpoint_refuses_changed_state_and_parameters(source_metadata):
    strategy, frame, split, bundle = _invented_checkpoint(source_metadata)
    changed = copy.deepcopy(bundle)
    changed["state"]["history_absolute_moves"][0][0] = 0.0
    with pytest.raises(ValueError, match="NOISE101_STATE_BINDING_INVALID"):
        state_contract.decode_noise101_replay_state(changed)
    crossed = StrategyRegistry.get(planner.NOISE101_STRATEGY_ID, **{**strategy.params, "band_k": 1.25})
    with pytest.raises(planner.ProductionSearchPlannerError, match="NOISE101_RESUME_STRATEGY_CROSSED"):
        planner.resume_noise101_recorded_session_signals(bundle, frame.iloc[split:], strategy=crossed, symbol="EURUSD")


def test_pipeline_fingerprint_binds_source_timeframe_without_name_fallback(source_metadata):
    metadata, _ = source_metadata
    h1 = pipeline._strategy_surface_fingerprint(source_edition=metadata, timeframe="H1")
    m5 = pipeline._strategy_surface_fingerprint(source_edition=metadata, timeframe="M5")
    assert h1 != m5
    with pytest.raises(ValueError, match="NOISE101_AUTHENTICATED_TIMEFRAME_REQUIRED"):
        pipeline._strategy_surface_fingerprint(source_edition=metadata)


def test_noise_requirement_needs_actual_state_and_transition_hashes(source_metadata):
    metadata, _ = source_metadata
    incomplete = planner.source_edition_context_for_cell(metadata, "H1")
    with pytest.raises(ValueError, match="NOISE101_SOURCE_CONTEXT_HASH_INVALID"):
        planner.verify_source_edition_context(metadata, "H1", incomplete,
            strategy_id=planner.NOISE101_STRATEGY_ID)
    _, _, _, bundle = _invented_checkpoint(source_metadata)
    assert planner.verify_source_edition_context(metadata, "H1", bundle["source_edition_context"],
        strategy_id=planner.NOISE101_STRATEGY_ID) == bundle["source_edition_context"]
