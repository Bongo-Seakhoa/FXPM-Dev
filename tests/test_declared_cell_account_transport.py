"""Declared-cell transport assurance; synthetic arithmetic grants no authority.

The retained STOP witness authenticates refusal only. These cases do not supply
thirteen genuine OWNER commits or demonstrate market profitability.
"""

from copy import deepcopy
from dataclasses import FrozenInstanceError, asdict
from types import SimpleNamespace
import hashlib
import json

import pandas as pd
import pytest

import pm_authority_spool as spool
import pm_causal_evidence_producer as causal
import pm_core
import pm_forward_oos_replay_producer as forward
from pm_core import PipelineConfig
from pm_current_refit_parameters import canonicalize_parameter_mapping
from tests.test_frozen_spread_execution import (
    _assert_continuous_expected,
    _continuous_case,
)
from tests.test_declared_slot_owner_surface import (
    _clone_declared_green_stop,
    retained_declared_green_stop,
)


_PROJECTION_RETAINED_FIELDS = (
    "schema_version", "generation_id", "generation_context_sha256", "native_universe_sha256",
    "config_sha256", "declared_precommit_sha256", "search_process_receipt_sha256", "cell",
    "pipeline_config_binding", "declared_commit_artifacts", "slot_summaries", "measurement", "genesis",
    "execution_source", "spread_source", "execution_spec_receipt", "spread_source_execution_spec_receipt",
    "frozen_profile_descriptor", "deployment_source_descriptors", "original_config_identity", "measured_config_identity",
)


def _projection_reader_stub(monkeypatch, status):
    """Invented reader-projection fixture; upstream custody verifiers are stubbed.

    This does not authenticate raw accounts, native data or thirteen OWNER
    commits. It tests only retention after the reader's existing verifier seam.
    """
    from datetime import datetime, timedelta, timezone

    digest = lambda name: hashlib.sha256(("SYNTHETIC_PROJECTION_STUB:" + name).encode("ascii")).hexdigest()
    start = datetime(2021, 1, 4, tzinfo=timezone.utc)
    schedule = [{"slot_id": f"SYNTHETIC_SLOT_{ordinal:02d}", "ordinal": ordinal,
        "start_utc": (start + timedelta(days=28 * (ordinal - 1))).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_exclusive_utc": (start + timedelta(days=28 * ordinal)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": ("OWNER" if ordinal == 1 else "NO_TRADE") if status == "COMPLETE"
                  else "STOP" if ordinal == 1 else "MISSING",
        "owner_id": "SYNTHETIC_OWNER" if status == "COMPLETE" and ordinal == 1 else None}
        for ordinal in range(1, 14)]
    committed = schedule if status == "COMPLETE" else schedule[:1]
    body = {
        "schema_version": forward.DECLARED_CELL_RAW_ACCOUNT_SCHEMA,
        "generation_id": "SYNTHETIC_PROJECTION_STUB_NOT_RAW_AUTHORITY",
        **{name: digest(name) for name in ("generation_context_sha256", "native_universe_sha256",
            "config_sha256", "declared_precommit_sha256", "search_process_receipt_sha256")},
        "cell": {"cell_id": digest("cell"), "symbol": "AUDUSD", "timeframe": "H1", "regime": "TREND"},
        "pipeline_config_binding": {"active_config_path": "SYNTHETIC_NOT_OPENED/config.json",
            "config_sha256": digest("config_sha256"), "effective_config_sha256": digest("effective-config")},
        "declared_commit_artifacts": [{"artifact_id": row["slot_id"], "sha256": digest(row["slot_id"])}
            for row in committed],
        "slot_summaries": [{"slot_id": row["slot_id"], "ordinal": row["ordinal"],
            "status": row["status"], "commit_sha256": digest(row["slot_id"])} for row in committed],
        "slot_schedule": schedule, "progress_status": status,
        "reason_code": None if status == "COMPLETE" else "SYNTHETIC_TECHNICAL_STOP",
        "measurement": {"calendar_days": 364, "refit_cycles": 13, "cycle_days": 28,
            "risk_per_trade_pct": 0.25, "terminal_policy": "MARK_OPEN",
            "start_utc": schedule[0]["start_utc"], "end_exclusive_utc": schedule[-1]["end_exclusive_utc"]},
        "genesis": {"mode": forward.NEW_SIMULATED_FLAT_GENESIS, "initial_cash": 10000.0, "initial_inventory": []},
        "owner_records": [{"owner": {"owner_id": "SYNTHETIC_OWNER"}}] if status == "COMPLETE" else [],
        **{name: {"fixture_provenance": "SYNTHETIC_PROJECTION_STUB", "sha256": digest(name)}
            for name in ("execution_source", "spread_source", "execution_spec_receipt",
                "spread_source_execution_spec_receipt", "frozen_profile_descriptor", "deployment_source_descriptors",
                "original_config_identity", "measured_config_identity")},
        "account": {"engine_result": {"net_account_profit": -10250.0, "final_equity": -250.0,
            "terminal": {"valuation_status": "SYNTHETIC_OBSERVED", "marked_equity": -250.0,
                "open_position": {"owner_id": "SYNTHETIC_OWNER"}},
            "first_depletion_bar": 17, "max_drawdown_pct": 102.5}} if status == "COMPLETE" else None,
        "qualification_authority": False, "capital_authority": False, "live_authority": False, "order_authority": False,
    }
    encoded = json.dumps(body, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":")).encode("ascii")
    sha = hashlib.sha256(encoded).hexdigest()
    descriptor = {"schema_version": spool.ARTIFACT_DESCRIPTOR_SCHEMA,
        "relative_path": f"objects/sha256/{sha[:2]}/{sha}.canonical-json", "sha256": sha,
        "size_bytes": len(encoded), "format": spool.CANONICAL_JSON_FORMAT}
    context, process, config = object(), object(), object()
    calls = []

    def read(observed_context, observed_descriptor, **kwargs):
        assert observed_context is context and observed_descriptor == descriptor
        calls.append("read-stub")
        return body

    def manifest(observed_context, observed_body, observed_descriptor):
        assert observed_context is context and observed_body is body and observed_descriptor == descriptor
        calls.append("manifest-stub")

    def verify(observed_context, observed_body, **kwargs):
        assert observed_context is context and observed_body is body
        assert kwargs["process"] is process and kwargs["frozen_pipeline_config"] is config
        calls.append("transport-verifier-stub")
        return body

    monkeypatch.setattr(causal, "_read_descriptor_json", read)
    monkeypatch.setattr(causal, "_verify_declared_cell_account_manifest", manifest)
    monkeypatch.setattr(forward, "verify_declared_cell_account_transport", verify)
    projection = causal.read_declared_cell_account_artifact(context,
        causal.DeclaredCellRawAccountArtifact(descriptor), process=process, frozen_pipeline_config=config)
    assert calls == ["read-stub", "manifest-stub", "transport-verifier-stub"]
    return body, projection


@pytest.mark.parametrize("status", ["COMPLETE", "STOP"])
def test_declared_projection_retains_every_verified_body_field(monkeypatch, status):
    """Projection-only fixture stubs; no genuine raw account claim."""
    body, projection = _projection_reader_stub(monkeypatch, status)
    assert len(_PROJECTION_RETAINED_FIELDS) == 21
    for field in _PROJECTION_RETAINED_FIELDS:
        assert projection.verified_body[field] == body[field], field
    assert projection.verified_body == body
    assert projection.cell_id == body["cell"]["cell_id"]
    assert projection.progress_status == body["progress_status"]
    assert projection.reason_code == body["reason_code"]
    assert projection.slot_schedule == tuple(body["slot_schedule"])
    assert projection.owner_records == tuple(body["owner_records"])
    assert projection.account == body["account"]
    assert projection.admissible is (status == "COMPLETE")
    assert all(getattr(projection, name) is False for name in (
        "qualification_authority", "capital_authority", "live_authority", "order_authority"))
    for field in ("net_account_profit", "final_equity", "terminal", "first_depletion_bar", "max_drawdown_pct"):
        expected = body["account"]["engine_result"][field] if status == "COMPLETE" else None
        assert getattr(projection, field) == expected
    assert projection.verified_body["measurement"]["calendar_days"] == 364
    assert projection.verified_body["measurement"]["refit_cycles"] == len(projection.slot_schedule) == 13
    # These slots belong to one carried account outcome, not thirteen samples.
    with pytest.raises(causal.CausalEvidenceProductionError) as caught:
        projection.require_interval_qualification()
    assert caught.value.code == "DECLARED_CELL_INTERVAL_QUALIFICATION_NOT_IMPLEMENTED"


def test_declared_projection_immutable_body_isolates_mutable_legacy_views(monkeypatch):
    """Mutation isolation only; the supplied body passed fixture-level stubs."""
    body, projection = _projection_reader_stub(monkeypatch, "COMPLETE")
    expected = deepcopy(body)
    view = projection.verified_body
    view["measurement"]["calendar_days"] = 1
    view["cell"]["regime"] = "MUTATED"
    view["declared_commit_artifacts"].clear()
    body["generation_id"] = "MUTATED"
    body["genesis"]["initial_cash"] = 1.0
    projection.account["engine_result"]["net_account_profit"] = 999999.0
    projection.slot_schedule[0]["status"] = "MUTATED"
    terminal = projection.terminal
    terminal["marked_equity"] = 1.0
    assert projection.verified_body == expected
    assert projection.net_account_profit == expected["account"]["engine_result"]["net_account_profit"]
    assert projection.final_equity == expected["account"]["engine_result"]["final_equity"]
    assert projection.terminal == expected["account"]["engine_result"]["terminal"]
    assert isinstance(projection._verified_body_json, bytes)
    with pytest.raises(FrozenInstanceError):
        projection._verified_body_json = b"{}"


def test_declared_measured_config_changes_only_account_risk():
    original = PipelineConfig(
        research_mode=True,
        initial_capital=10_000.0,
        discovery_risk_per_trade_pct=1.0,
        use_spread=True,
        backtest_spread_mode="conservative_max",
    )
    before = asdict(original)
    measured = forward._declared_cell_measured_config(original)
    assert measured is not original
    assert asdict(original) == before
    assert asdict(measured) == {
        **before,
        "discovery_risk_per_trade_pct": 0.25,
    }
    assert measured.initial_capital == 10_000.0


@pytest.mark.parametrize("changed", [
    {"initial_capital": 1_000.0},
    {"discovery_risk_per_trade_pct": 0.25},
])
def test_declared_measured_config_refuses_unsupported_original(changed):
    original = PipelineConfig(**{
        "research_mode": True, "initial_capital": 10_000.0,
        "discovery_risk_per_trade_pct": 1.0, "use_spread": True,
        "backtest_spread_mode": "conservative_max", **changed,
    })
    with pytest.raises(forward.ForwardOOSReplayProductionError) as caught:
        forward._declared_cell_measured_config(original)
    assert caught.value.code == "DECLARED_CELL_ORIGINAL_CONFIG_INVALID"


@pytest.mark.parametrize("case_id", [
    "N07_LONG_STRADDLE_P75_TO_P95",
    "N10_NO_TRADE_MANAGES_HELD_LONG_STOP",
    "N14_GAP_EXIT_OLD_P75_NEW_P95_SAME_BAR",
    "N22_GAP_DEPLETION_STAYS_NEGATIVE",
    "N25_NAIVE_UTC_LABEL_WITHOUT_SHIFT",
])
def test_declared_writer_runs_one_continuous_account_with_frozen_arithmetic(
    monkeypatch, case_id,
):
    """Exercise the writer seam using predeclared, explicitly invented prices."""
    backtester, arguments, expected = _continuous_case(case_id)
    assert arguments["governance_policy"] is None
    index_before = arguments["features"].index.copy()
    closes_before = arguments["features"]["_BAR_CLOSE_TS"].copy()
    actual_run = backtester.run
    calls, constructed, returned = [], [], []

    def construct(config):
        assert config is backtester.config
        constructed.append(config)
        return backtester

    def run(*args, **kwargs):
        calls.append((args, kwargs))
        value = actual_run(*args, **kwargs)
        returned.append(value)
        return value

    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", False)
    monkeypatch.setattr(forward, "Backtester", construct)
    monkeypatch.setattr(backtester, "run", run)
    result = forward._run_declared_cell_account(
        config=backtester.config,
        **{name: arguments[name] for name in (
            "features", "symbol", "timeframe", "spec", "warmup_bars",
            "entry_schedule", "entry_spread_observations",
        )},
    )

    assert constructed == [backtester.config]
    assert len(calls) == 1 and result is returned[0]
    positional, keywords = calls[0]
    assert positional[0] is arguments["features"]
    assert positional[1] is None and positional[2] == arguments["symbol"]
    assert positional[3] is None
    assert keywords["entry_schedule"] is arguments["entry_schedule"]
    assert keywords["entry_spread_observations"] is arguments["entry_spread_observations"]
    assert keywords["terminal_policy"] == "MARK_OPEN"
    pd.testing.assert_index_equal(arguments["features"].index, index_before)
    pd.testing.assert_series_equal(arguments["features"]["_BAR_CLOSE_TS"], closes_before)
    _assert_continuous_expected(result, expected)


@pytest.mark.parametrize("side", ["writer", "reader"])
@pytest.mark.parametrize("pin_consumed", [False, True])
def test_saved_activation_pin_is_opt_in_for_both_native_reopens(
    monkeypatch, side, pin_consumed,
):
    """Check native-call routing only; the recorded product is not a verifier."""
    import pm_native_prefix_production as native

    identity = {
        "symbol": "QA142", "timeframe": "H1",
        "cutoff_utc": "2026-01-01T00:00:00Z",
        "activation_manifest_sha256": "a" * 64,
        "index_descriptor": {"synthetic": True},
        "index_sha256": "b" * 64,
        "role": "LABELLED_SYNTHETIC_CALL_ROUTING_ONLY",
    }
    product = SimpleNamespace(
        index_descriptor=identity["index_descriptor"],
        index_sha256=identity["index_sha256"],
    )
    frame, context, calls = object(), object(), []

    def verify(**kwargs):
        calls.append(kwargs)
        return product

    monkeypatch.setattr(native, "verify_activated_native_prefix", verify)
    monkeypatch.setattr(native, "open_verified_prefix_frame", lambda observed: frame)
    monkeypatch.setattr(forward, "_native_product_identity", lambda *args, **kwargs: identity)
    config = PipelineConfig(research_mode=True)
    optional = {"pin_consumed_activation": True} if pin_consumed else {}
    if side == "writer":
        observed = forward._reopen_bound_native_product(
            context=context, product=product, expected_identity=identity,
            config=config, production_contract_required=False, **optional,
        )
        assert observed is product
    else:
        assert causal._reopen_frozen_transport_source(
            context, identity, config, **optional,
        ) == (frame, product)
    assert len(calls) == 1
    assert calls[0]["expected_activation_manifest_sha256"] == "a" * 64
    assert calls[0]["require_current_activation"] is False
    assert calls[0]["production_contract_required"] is False
    if pin_consumed:
        assert calls[0]["pinned_activation_manifest_sha256"] == "a" * 64
    else:
        assert "pinned_activation_manifest_sha256" not in calls[0]


@pytest.fixture(scope="module")
def declared_green_stop_inputs(retained_declared_green_stop, tmp_path_factory):
    """Copy authenticated issued closure; never append to the original GREEN."""
    import shutil

    retained = retained_declared_green_stop
    env = _clone_declared_green_stop(
        tmp_path_factory.mktemp("declared-account-green-stop"), retained,
    )
    # The existing mutation fixture deliberately omits the committed marker and
    # body. This reader test restores their exact authenticated bytes.
    paths = set(retained.capsule["commit_files"])
    paths.update(item["relative_path"] for item in retained.capsule["commit_descriptors"])
    for relative in paths:
        source, target = retained.context.root / relative, env.context.root / relative
        assert source.resolve(strict=True).is_relative_to(retained.context.root)
        assert target.resolve().is_relative_to(env.context.root)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        assert target.read_bytes() == source.read_bytes()
    env.retained = retained
    return env


def test_retained_genuine_green_stop_remains_nonadmissible_before_account_engine(
    declared_green_stop_inputs, monkeypatch,
):
    """The real cold STOP proves refusal, not a thirteen-OWNER account."""
    env = declared_green_stop_inputs
    relatives = set(env.retained.capsule["commit_files"])
    relatives.update(item["relative_path"]
                     for item in env.retained.capsule["commit_descriptors"])
    commit_paths = [root / name for root in (env.context.root, env.retained.context.root)
                    for name in relatives]
    committed_bytes = {path: path.read_bytes() for path in commit_paths}

    def no_engine(**kwargs):
        pytest.fail("A retained STOP must not enter the account engine")

    monkeypatch.setattr(forward, "_run_declared_cell_account", no_engine)
    artifact = forward.produce_declared_cell_account_artifact(
        env.context, precommit=env.precommit, process=env.process,
        frozen_pipeline_config=env.frozen,
    )
    assert type(artifact) is causal.DeclaredCellRawAccountArtifact
    projection = causal.read_declared_cell_account_artifact(
        env.context, artifact, process=env.process,
        frozen_pipeline_config=env.frozen,
    )
    assert projection.admissible is False
    assert projection.account is None
    assert projection.owner_records == ()
    assert projection.progress_status == "STOP"
    assert projection.reason_code == "DECLARED_CELL_OWNER_PREFIX_STOP"
    assert all(getattr(projection, name) is False for name in (
        "qualification_authority", "capital_authority", "live_authority", "order_authority",
    ))
    for path, original in committed_bytes.items():
        assert path.read_bytes() == original
    with pytest.raises(causal.CausalEvidenceProductionError) as caught:
        projection.require_interval_qualification()
    assert caught.value.code == "DECLARED_CELL_INTERVAL_QUALIFICATION_NOT_IMPLEMENTED"


def test_declared_dispatch_requires_saved_input_capabilities():
    artifact = causal.DeclaredCellRawAccountArtifact({})
    with pytest.raises(causal.CausalEvidenceProductionError) as caught:
        causal._load_replay_ledgers(object(), artifact, {})
    assert caught.value.code == "DECLARED_CELL_ACCOUNT_INPUTS_REQUIRED"


def test_declared_dispatch_preserves_existing_frozen_tier_archive_branch(monkeypatch):
    """Routing proof only; existing v1-v3 byte validators remain their own tests."""
    context, anchor = object(), object()
    artifact = causal.FrozenTierRawAccountsArtifact({"label": "SYNTHETIC_ROUTE_ONLY"})
    calls = []
    projection = causal.FrozenTierRawAccountsProjection(
        cell_id="synthetic-archive", account_tiers=("P75", "P95"),
        descriptor_sha256="1" * 64, accounts={},
    )

    def old_reader(*args):
        calls.append(args)
        return projection

    def no_declared_reader(*args, **kwargs):
        pytest.fail("The existing raw-account artifact must retain archive dispatch")

    monkeypatch.setattr(causal, "_read_frozen_tier_raw_accounts", old_reader)
    monkeypatch.setattr(causal, "read_declared_cell_account_artifact", no_declared_reader)
    with pytest.raises(causal.CausalEvidenceProductionError) as caught:
        causal._load_replay_ledgers(context, artifact, anchor)
    assert caught.value.code == "FROZEN_TIER_INTERVAL_QUALIFICATION_NOT_IMPLEMENTED"
    assert calls == [(context, artifact, anchor)]


def _synthetic_continuous_account_case(case_id):
    """Reuse predeclared prices and financing; never authenticate an OWNER."""
    backtester, arguments, expected = _continuous_case(case_id)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(pm_core, "NUMBA_AVAILABLE", False)
        patch.setattr(forward, "Backtester", lambda config: backtester)
        result = forward._run_declared_cell_account(
            config=backtester.config,
            **{name: arguments[name] for name in (
                "features", "symbol", "timeframe", "spec", "warmup_bars",
                "entry_schedule", "entry_spread_observations",
            )},
        )
        _assert_continuous_expected(result, expected)
        frame = arguments["features"]
        owners = arguments["entry_schedule"].owners
        observations = arguments["entry_spread_observations"]
        matched = pd.Series(frame.index, index=frame.index)
        account = forward._declared_account_payload(
            result=forward._raw_json(result), frame=frame, owner_inputs=owners,
            spec=arguments["spec"], config=backtester.config, observations=observations,
            matched_closes=matched, timeframe=arguments["timeframe"],
            warmup_bars=arguments["warmup_bars"],
        )
    return SimpleNamespace(account=account, backtester=backtester, reconcile={
        "frame": frame, "owner_inputs": owners, "spec": arguments["spec"],
        "config": backtester.config, "observations": observations,
        "matched_closes": matched, "timeframe": arguments["timeframe"],
        "schedule": arguments["entry_schedule"],
    })


@pytest.fixture(scope="module")
def synthetic_continuous_account():
    return _synthetic_continuous_account_case("N07_LONG_STRADDLE_P75_TO_P95")


def test_declared_account_reconciles_labelled_synthetic_mark_open(synthetic_continuous_account):
    env = synthetic_continuous_account
    assert forward._reconcile_declared_cell_account(env.account, **env.reconcile) is None


@pytest.mark.parametrize("case_id", [
    "N10_NO_TRADE_MANAGES_HELD_LONG_STOP",
    "N14_GAP_EXIT_OLD_P75_NEW_P95_SAME_BAR",
    "N22_GAP_DEPLETION_STAYS_NEGATIVE",
    "N16_PREOPEN_SWAP_THEN_OLD_GAP_EXIT_THEN_NEW_SIZE",
])
def test_declared_account_reconciles_frozen_carry_gap_depletion_and_swap_cases(
    case_id, monkeypatch,
):
    """N16 alone supplies swap-ON arithmetic; the other goldens stay unchanged."""
    env = _synthetic_continuous_account_case(case_id)
    monkeypatch.setattr(forward, "Backtester", lambda config: env.backtester)
    assert forward._reconcile_declared_cell_account(env.account, **env.reconcile) is None
    assert env.account["financing_model"]["enabled"] is (
        case_id == "N16_PREOPEN_SWAP_THEN_OLD_GAP_EXIT_THEN_NEW_SIZE")


def test_declared_account_refuses_flattening_mandatory_source_entries(
    synthetic_continuous_account,
):
    env = synthetic_continuous_account
    account = deepcopy(env.account)
    result = account["engine_result"]
    assert result["terminal"]["open_position"] is not None
    assert result["spread_admission"]["known_cap_pass_opportunities"] > 0
    admission = deepcopy(result["spread_admission"])
    source_bindings = deepcopy(account["owner_input_bindings"])
    financing = deepcopy(account["financing_model"])
    assert financing["enabled"] is False
    initial = float(env.reconcile["config"].initial_capital)
    result.update({
        "trades": [], "total_trades": 0, "closed_trade_gross_profit": 0.0,
        "cash": initial, "final_equity": initial, "net_account_profit": 0.0,
        "equity_curve": [initial] * len(result["equity_curve"]),
        "max_drawdown_pct": 0.0, "first_depletion_bar": None,
    })
    result["terminal"].update({
        "valuation_status": "FLAT_CASH_ONLY", "cash": initial, "marked_equity": initial,
        "bid": None, "ask": None, "unrealized_gross": 0.0,
        "open_position": None, "first_depletion_bar": None,
    })
    assert result["spread_admission"] == admission
    assert account["owner_input_bindings"] == source_bindings
    assert account["financing_model"] == financing
    with pytest.raises(forward.ForwardOOSReplayProductionError) as caught:
        forward._reconcile_declared_cell_account(account, **env.reconcile)
    assert caught.value.code == "DECLARED_CELL_ENTRY_COMPLETENESS_INVALID"


@pytest.mark.parametrize("mutation,code", [
    ("source", "DECLARED_CELL_ACCOUNT_SOURCE_BINDING_MISMATCH"),
    ("terminal_policy", "DECLARED_CELL_ACCOUNT_TERMINAL_OR_AXIS_INVALID"),
    ("interior_equity", "DECLARED_CELL_EQUITY_RECONCILIATION_FAILED"),
    ("cash", "DECLARED_CELL_CASH_RECONCILIATION_FAILED"),
    ("terminal_mark", "DECLARED_CELL_TERMINAL_RECONCILIATION_FAILED"),
    ("depletion", "DECLARED_CELL_DEPLETION_RECONCILIATION_FAILED"),
])
def test_declared_account_refuses_synthetic_accounting_tamper(
    synthetic_continuous_account, mutation, code,
):
    env = synthetic_continuous_account
    account = deepcopy(env.account)
    result = account["engine_result"]
    if mutation == "source":
        account["source_frame_sha256"] = "0" * 64
    elif mutation == "terminal_policy":
        result["terminal"]["policy"] = "CLOSED_EOD"
    elif mutation == "interior_equity":
        result["equity_curve"][2] += 1.0
    elif mutation == "cash":
        result["cash"] += 1.0
    elif mutation == "terminal_mark":
        result["terminal"]["marked_equity"] += 1.0
    elif mutation == "depletion":
        assert result["first_depletion_bar"] is None
        result["first_depletion_bar"] = 0
    with pytest.raises(forward.ForwardOOSReplayProductionError) as caught:
        forward._reconcile_declared_cell_account(account, **env.reconcile)
    assert caught.value.code == code


def _synthetic_reopened_declared_prefix(tmp_path, monkeypatch, edition, *, missing_only=False):
    """Physical commits with an exact, labelled synthetic raw-authentication seam.

    The real metadata issuer, request validator, edition gate, commit writer,
    cold progress, account input reopener and account transport stay in use.
    Upstream raw OWNER and native/spec authentication are invented. The MISSING
    mode also emits only the thirteen predeclared absences through the actual
    driver. These finite seams claim no genuine OWNER or real native absence.
    Newly invented :01 bars support this structural writer projection only:
    real :00 bars fail the old roster's terminal guard. No clock policy follows.
    """
    from pathlib import Path
    import test_production_config_binding as config_fixture
    import pm_historical_anchor_orchestration as orchestration
    import pm_historical_production_adapter as adapter
    import pm_historical_search_producer as producer
    import pm_native_prefix_production as native
    from pm_regime import RegimeParams, regime_params_sha256
    from tests.test_declared_slot_owner_surface import _declared_environment
    from tests.test_forward_oos_replay_producer import _sealed_identity_only_product, _synthetic_profile
    from tests.test_native_prefix_production import _execution_config

    assert edition in ("v15", "v16")
    classification = "QA142_SYNTHETIC_COMMIT_AUTH_NOT_RAW_OWNER_PROOF"
    root = tmp_path / ("synthetic-reopened-" + edition)
    root.mkdir(parents=True)
    _execution_config(root, symbol="AUDUSD")  # Existing invented broker bytes.
    original_payload = config_fixture._payload

    def payload(path):
        value = original_payload(path)
        value["broker_specs_path"] = "broker_specs.json"
        value["pipeline"]["swap_model_enabled"] = False
        return value

    # Build the normal real issuer on a declared test payload, not a config stub.
    with monkeypatch.context() as payload_patch:
        payload_patch.setattr(config_fixture, "_payload", payload)
        env = _declared_environment(root, monkeypatch, issue=False)
    config = env.frozen.materialize(env.context)
    slots, cell = env.roster["slots"], env.arguments["cell_roster"][0]
    start = pd.Timestamp(slots[0]["activation_utc"])
    end = pd.Timestamp(slots[-1]["end_exclusive_utc"])
    cutoff = end.strftime("%Y-%m-%dT%H:%M:%SZ")
    # Newly invented bars: no existing bars are shifted to this nominal :01 grid.
    selection_cutoff = pd.Timestamp(slots[0]["native_cutoff_utc"])
    prefix_index = pd.date_range(end=selection_cutoff - pd.Timedelta(hours=1), periods=220, freq="h")
    index = prefix_index.append(pd.DatetimeIndex([
        start, start + pd.Timedelta(hours=1), end - pd.Timedelta(hours=2), end - pd.Timedelta(hours=1)]))
    frame = pd.DataFrame({"Open": 1.0, "High": 1.0002, "Low": 0.9998, "Close": 1.0,
        "Volume": 1.0, "Spread": 3.0, "_BAR_CLOSE_TS": index + pd.Timedelta(hours=1)}, index=index)
    fine_index = (index - pd.Timedelta(minutes=5)).append(pd.DatetimeIndex([end - pd.Timedelta(minutes=5)]))
    fine_frame = pd.DataFrame({"Open": 1.0, "High": 1.0002, "Low": 0.9998, "Close": 1.0,
        "Spread": 3.0, "_BAR_CLOSE_TS": fine_index + pd.Timedelta(minutes=5)}, index=fine_index)
    products = {tf: _sealed_identity_only_product(symbol=cell["symbol"], timeframe=tf, cutoff_utc=cutoff)
        for tf in ("H1", "M5")}
    identities = {tf: forward._native_product_identity(product, expected_symbol=cell["symbol"],
        expected_timeframe=tf, expected_cutoff_utc=cutoff,
        role="DECLARED_CELL_EXECUTION" if tf == "H1" else "DECLARED_CELL_SPREAD")
        for tf, product in products.items()}
    terms = Path(config.data_dir) / "native" / "terms"
    tiers = _synthetic_profile(terms, symbol=cell["symbol"], identity=identities["M5"])
    profile_path = terms / "spread_profile.json"
    (terms / "synthetic_frozen_profile.json").rename(profile_path)
    env.arguments["spread_profile_identity"] = {"profile_sha256": tiers["P75"].descriptor()["profile_sha256"]}
    issuer = (orchestration.precommit_declared_slot_owner_run_v2 if edition == "v16"
        else orchestration.precommit_declared_slot_owner_run)
    env.precommit = issuer(env.context, **env.arguments)
    parameters = RegimeParams().to_dict()
    winner_process = {"strategy_id": "EMACrossoverStrategy", "strategy_class": "EMACrossoverStrategy",
        "strategy_module": "pm_strategies"}
    selected = {"strategy_id": winner_process["strategy_id"], "process_candidate_id": "QA142_SYNTHETIC_FIXED_PROCESS",
        "process_sha256": forward._sha(winner_process), "encoded_parameters": canonicalize_parameter_mapping(
            forward.StrategyRegistry.get(winner_process["strategy_id"]).get_default_params())}
    owner = {"classification": classification,
        "owner_id": orchestration.declared_slot_owner_id(cell_id=cell["cell_id"], slot_id=slots[0]["slot_id"],
            winner_decision_sha256="f" * 64, process_receipt_sha256=env.process.receipt_sha256),
        "cell_id": cell["cell_id"], "slot_id": slots[0]["slot_id"], "selected_tier": "P75",
        "selected_tier_contract": {"frozen_spread_tier": tiers["P75"].descriptor()},
        "selected_artifacts": {"winner_process": winner_process}, "selected_attempt": selected,
        "regime_detector_record": {"parameters_canonical_json": json.dumps(parameters, sort_keys=True, separators=(",", ":")),
            "parameters_sha256": regime_params_sha256(parameters)}}
    count = {**cell, "fit_start_utc": prefix_index[0].isoformat(),
        "fit_end_exclusive_utc": selection_cutoff.isoformat(), "source_bar_count": len(prefix_index),
        "fit_frame_sha256": forward._frame_sha256(frame.loc[prefix_index]),
        "latest_closed_bar_utc": selection_cutoff.isoformat()}
    count["receipt_sha256"] = forward._sha(count)
    known, authentication_calls, commit_entries = {}, [], []

    def synthetic_completion_auth(context, *, precommit, process, completion, artifact_roster, frozen_pipeline_config):
        """Finite invented raw-auth corpus; reject unknown, extra or changed bytes."""
        assert context is env.context and process is env.process and frozen_pipeline_config is env.frozen
        row = known.get(completion.get("request_sha256"))
        assert row is not None and forward._canonical_bytes(completion) == row["completion_bytes"]
        assert list(artifact_roster) == row["artifacts"]
        assert completion["classification"] == classification
        sink = spool.open_symbol_sink(context, cell["symbol"])
        request = orchestration.DeclaredSlotProducerRequest(
            orchestration._declared_json(completion["request_body"]), precommit, sink, orchestration._DECLARED_SLOT_SEAL)
        orchestration._register_declared_request(request, context, live=False)
        try:
            body = orchestration.verify_declared_slot_producer_request(context, request,
                precommit=precommit, process=process)
            assert request.receipt_sha256 == completion["request_sha256"]
            assert body["cell_id"] == cell["cell_id"] and body["slot_id"] == completion["slot_id"]
            for entry in artifact_roster:
                descriptor = entry["descriptor"]
                digest, size, _ = spool._stream_file(context.root, descriptor["relative_path"],
                    maximum_bytes=descriptor["size_bytes"], expected_size=descriptor["size_bytes"], collect=False)
                assert digest == descriptor["sha256"] and size == descriptor["size_bytes"]
        finally:
            orchestration._release_declared_slot_producer_request(request, context)
        authentication_calls.append(completion["request_sha256"])
        return deepcopy(completion)

    monkeypatch.setattr(adapter, "verify_declared_slot_completion", synthetic_completion_auth)

    def prepare_completion(request, slot, status):
        body = orchestration.verify_declared_slot_producer_request(env.context, request,
            precommit=env.precommit, process=env.process)
        assert body["slot_id"] == slot["slot_id"] and body["anchor"]["ordinal"] == slot["ordinal"]
        assert body["cell_id"] == cell["cell_id"] and body["precommit_sha256"] == env.precommit.receipt_sha256
        assert request.receipt_sha256 not in known
        sink = request.symbol_sink(cell["symbol"])
        completion = producer._declared_base_completion(request, status=status,
            reason_code="SYNTHETIC_ABSENCE" if status == "MISSING" else None if status == "OWNER" else "SYNTHETIC_NO_TRADE",
            source_entry=None)
        completion["classification"] = classification
        completion["owner"] = owner if status == "OWNER" else None
        artifacts = []
        if status == "OWNER":
            namespace = request.to_dict()["artifact_namespace"]
            schema = adapter.raw_winner.V16_FROZEN_PLAN_SCHEMA if edition == "v16" else adapter.raw_winner.CURRENT_FROZEN_PLAN_SCHEMA
            frozen_entry = sink.put_opaque(namespace + "/invented-frozen-receipt", "synthetic_frozen_schema_only",
                (orchestration._declared_json({"schema_version": schema, "classification": classification}),),
                format=adapter.raw_winner.ASCII_JSON_FORMAT)
            cross_entry = sink.put_json(namespace + "/invented-cross", "synthetic_owner_mapping_not_raw_cross_proof",
                {"classification": classification, "mapping": [{"winner_process_candidate_id": selected["process_candidate_id"],
                    "process_sha256": selected["process_sha256"], "source_count_receipt": count}]})
            completion["frozen_plan_receipt"] = frozen_entry["descriptor"]
            completion["horizon_winner_cross_binding"] = cross_entry["descriptor"]
            artifacts = [frozen_entry, cross_entry]
        known[request.receipt_sha256] = {"completion_bytes": forward._canonical_bytes(completion),
            "completion": deepcopy(completion), "artifacts": deepcopy(artifacts), "slot": slot}
        return completion, artifacts

    driver_calls = []
    if missing_only:
        def emit_declared_missing(context, *, request, process, frozen_pipeline_config):
            """Invented source absence only, keyed to this driver's real issued requests."""
            assert context is env.context and process is env.process and frozen_pipeline_config is env.frozen
            body = orchestration.verify_declared_slot_producer_request(context, request,
                precommit=env.precommit, process=process)
            orchestration._verify_declared_request_identity(request, context, require_live=True)
            ordinal = len(driver_calls) + 1
            assert 1 <= ordinal <= 13 and body["anchor"]["ordinal"] == ordinal
            assert body["slot_id"] == slots[ordinal - 1]["slot_id"]
            completion, artifacts = prepare_completion(request, slots[ordinal - 1], "MISSING")
            assert artifacts == []
            driver_calls.append(body["slot_id"])
            return completion

        monkeypatch.setattr(producer, "execute_declared_slot_owner_search", emit_declared_missing)
        run_receipt = producer.execute_declared_cell_owner_slots(context=env.context, precommit=env.precommit,
            process=env.process, frozen_pipeline_config=env.frozen, cell_id=cell["cell_id"]).to_dict()
        assert driver_calls == [slot["slot_id"] for slot in slots]
        assert run_receipt["completed_slots"] == 13 and run_receipt["terminal_stop"] is False
        progress = producer.reopen_declared_cell_owner_progress(env.context, precommit=env.precommit,
            process=env.process, cell_id=cell["cell_id"], frozen_pipeline_config=env.frozen)
        commit_entries.extend(progress.slot_commit_artifacts)
    else:
        previous = None
        for slot in slots:
            sink = spool.open_symbol_sink(env.context, cell["symbol"])
            request = orchestration.declared_slot_producer_request(env.context, precommit=env.precommit,
                process=env.process, cell_id=cell["cell_id"], slot_id=slot["slot_id"],
                attempt_id="synthetic-carrier", sink=sink)
            completion, artifacts = prepare_completion(request, slot, "OWNER" if slot["ordinal"] == 1 else "NO_TRADE")
            try:
                _, descriptor = producer._commit_declared_slot_completion(env.context,
                    precommit=env.precommit, process=env.process, cell_id=cell["cell_id"], slot=slot,
                    completion=completion, artifact_roster=artifacts, previous_commit_sha256=previous,
                    frozen_pipeline_config=env.frozen)
                previous = descriptor["sha256"]
                commit_entries.append(producer._declared_commit_artifact(slot, descriptor))
            finally:
                orchestration._release_declared_slot_producer_request(request, env.context)
            if slot["ordinal"] == 1:
                # Read the actual one-OWNER prefix before any later commits exist.
                env.one_owner_prefix = forward._reopen_declared_cell_inputs(env.context,
                    precommit=env.precommit, process=env.process, frozen_pipeline_config=env.frozen)
                assert env.one_owner_prefix["progress_status"] == "INCOMPLETE"
                assert len(env.one_owner_prefix["commits"]) == len(env.one_owner_prefix["owner_records"]) == 1
                assert [row["status"] for row in env.one_owner_prefix["slot_schedule"]] == ["OWNER"] + ["MISSING"] * 12
        sink = spool.open_symbol_sink(env.context, cell["symbol"])
        for entry in commit_entries:
            sink.register_artifact(entry["artifact_id"], entry["artifact_type"], entry["descriptor"])
        sink.seal()
    spec = pm_core.InstrumentSpec(cell["symbol"], 5, point=0.00001, tick_size=0.00001,
        tick_value=1.25, pip_value=12.5, min_lot=0.01, max_lot=500.0, volume_step=0.01, asset_class="fx")

    native_calls = []

    def verify_native(**kwargs):
        assert kwargs["context"] is env.context and kwargs["expected_symbol"] == cell["symbol"]
        product = products[kwargs["expected_timeframe"]]
        assert kwargs["expected_index_sha256"] == product.index_sha256
        assert kwargs["index_descriptor"] == product.index_descriptor and kwargs["expected_cutoff_utc"] == cutoff
        assert kwargs["pinned_activation_manifest_sha256"] == identities["M5"]["activation_manifest_sha256"]
        assert kwargs["expected_activation_manifest_sha256"] == identities["M5"]["activation_manifest_sha256"]
        assert kwargs["production_contract_required"] is False and kwargs["require_current_activation"] is False
        assert Path(kwargs["data_dir"]) == Path(config.data_dir)
        native_calls.append(kwargs["expected_timeframe"])
        return product

    def open_native(product):
        assert any(product is value for value in products.values())
        return (frame if product is products["H1"] else fine_frame).copy(deep=True)

    def freeze_spec(*, product, config):
        assert any(product is value for value in products.values())
        assert config._deployment_config_path == str(env.config_path.resolve())
        deployment = forward._deployment_config(config)
        receipt = {"classification": classification, "deployment_config_sha256": deployment.config_sha256,
            "deployment_config_size_bytes": deployment.config_size_bytes,
            "broker_specs_relative_path": deployment.broker_relative_path,
            "broker_specs_sha256": deployment.broker_sha256, "broker_specs_size_bytes": deployment.broker_size_bytes,
            "native_prefix_index_sha256": product.index_sha256,
            "grants_search_authority": False, "grants_trading_authority": False}
        receipt["receipt_sha256"] = forward._sha(receipt)
        return spec, receipt

    monkeypatch.setattr(native, "verify_activated_native_prefix", verify_native)
    monkeypatch.setattr(native, "open_verified_prefix_frame", open_native)
    monkeypatch.setattr(forward.search_execution, "_freeze_bound_execution_spec", freeze_spec)
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", False)
    env.known, env.authentication_calls, env.commit_entries = known, authentication_calls, commit_entries
    env.products, env.profile_path, env.classification = products, profile_path, classification
    env.native_calls = native_calls
    env.driver_calls = driver_calls
    return env


@pytest.mark.parametrize("edition", ["v15", "v16"])
def test_synthetic_issued_prefix_reopens_thirteen_commits_into_one_account(tmp_path, monkeypatch, edition):
    """Real joins on invented :01 bars; real :00 bars fail this old roster's end guard.

    This is structural projection, with finite synthetic raw/native authentication,
    not genuine OWNER evidence, clock policy or an admissible qualification claim.
    """
    import pm_historical_search_producer as producer
    env = _synthetic_reopened_declared_prefix(tmp_path, monkeypatch, edition)
    before = {entry["descriptor"]["relative_path"]:
        (env.context.root / entry["descriptor"]["relative_path"]).read_bytes() for entry in env.commit_entries}
    inputs = forward._reopen_declared_cell_inputs(env.context, precommit=env.precommit,
        process=env.process, frozen_pipeline_config=env.frozen)
    assert inputs["progress_status"] == "COMPLETE" and len(inputs["commits"]) == 13
    assert [row["status"] for row in inputs["slot_schedule"]] == ["OWNER"] + ["NO_TRADE"] * 12
    assert len(inputs["owner_records"]) == 1
    assert env.one_owner_prefix["progress_status"] == "INCOMPLETE"
    assert len(env.one_owner_prefix["commits"]) == 1
    record = inputs["owner_records"][0]
    cross = forward._read_descriptor_json(env.context,
        inputs["commits"][0]["completion"]["horizon_winner_cross_binding"], code="QA142_SYNTHETIC_OWNER_CROSS")
    selected = record["owner"]["selected_attempt"]
    matches = [row for row in cross["mapping"]
        if row["winner_process_candidate_id"] == selected["process_candidate_id"]
        and row["process_sha256"] == selected["process_sha256"]]
    assert len(matches) == 1 and matches[0]["source_count_receipt"] == record["source_count_receipt"]
    count = record["source_count_receipt"]
    assert count["receipt_sha256"] == forward._sha({key: value for key, value in count.items() if key != "receipt_sha256"})
    assert cross["classification"] == env.classification
    expected_edition = producer.historical.DECLARED_WINNER_TRANSPORT_V16 if edition == "v16" else producer.historical.DECLARED_WINNER_TRANSPORT_V15
    assert producer.historical.declared_slot_transport_edition(inputs["parent"]) == expected_edition
    assert all(producer.historical.declared_slot_transport_edition(row["completion"]["request_body"]) == expected_edition
        for row in inputs["commits"])
    assert env.native_calls == []
    run, calls = forward._run_declared_cell_account, []

    def run_once(**kwargs):
        calls.append(kwargs)
        return run(**kwargs)

    monkeypatch.setattr(forward, "_run_declared_cell_account", run_once)
    artifact = forward.produce_declared_cell_account_artifact(env.context, precommit=env.precommit,
        process=env.process, frozen_pipeline_config=env.frozen, execution_product=env.products["H1"],
        spread_source_product=env.products["M5"], frozen_profile_path=env.profile_path)
    body = json.loads((env.context.root / artifact.descriptor["relative_path"]).read_bytes())
    projection = causal.read_declared_cell_account_artifact(env.context, artifact,
        process=env.process, frozen_pipeline_config=env.frozen)
    assert len(calls) == 1 and calls[0]["config"].discovery_risk_per_trade_pct == 0.25
    assert env.native_calls == ["H1", "M5", "H1", "M5"]
    assert projection.admissible and projection.verified_body == body
    assert projection.account == body["account"] and len(projection.slot_schedule) == 13
    assert len(projection.owner_records) == 1
    assert body["owner_records"][0]["owner"]["classification"] == env.classification
    for name in ("net_account_profit", "final_equity", "terminal", "first_depletion_bar", "max_drawdown_pct"):
        assert getattr(projection, name) == body["account"]["engine_result"][name]
    assert all(getattr(projection, name) is False for name in (
        "qualification_authority", "capital_authority", "live_authority", "order_authority"))
    for relative, raw in before.items():
        assert (env.context.root / relative).read_bytes() == raw
    # The explicit raw-auth seam must not accept arbitrary or altered completions.
    row = next(iter(env.known.values()))
    changed = deepcopy(row["completion"])
    changed["extra_invented_field"] = True
    with pytest.raises(AssertionError):
        producer.historical_adapter.verify_declared_slot_completion(env.context,
            precommit=env.precommit, process=env.process, completion=changed,
            artifact_roster=row["artifacts"], frozen_pipeline_config=env.frozen)
    with pytest.raises(causal.CausalEvidenceProductionError) as caught:
        projection.require_interval_qualification()
    assert caught.value.code == "DECLARED_CELL_INTERVAL_QUALIFICATION_NOT_IMPLEMENTED"


@pytest.mark.parametrize("edition", ["v15", "v16"])
def test_synthetic_issued_prefix_crossed_edition_hits_real_commit_and_cold_gate(tmp_path, monkeypatch, edition):
    """Coherent owned-byte mutation: real edition gates, not synthetic auth refusal."""
    import pm_historical_anchor_orchestration as orchestration
    import pm_historical_search_producer as producer
    env = _synthetic_reopened_declared_prefix(tmp_path, monkeypatch, edition, missing_only=True)
    row = next(iter(env.known.values()))
    crossed = deepcopy(row["completion"])
    request_body = crossed["request_body"]
    if edition == "v15":
        request_body["schema_version"] = orchestration.DECLARED_SLOT_REQUEST_SCHEMA_V2
        request_body["winner_transport_edition"] = orchestration.DECLARED_WINNER_TRANSPORT_V16
    else:
        request_body["schema_version"] = orchestration.DECLARED_SLOT_REQUEST_SCHEMA
        request_body.pop("winner_transport_edition")
    crossed["request_sha256"] = hashlib.sha256(orchestration._declared_json(request_body)).hexdigest()
    calls_before = list(env.authentication_calls)
    with pytest.raises(producer.HistoricalSearchProducerError) as caught:
        producer._commit_declared_slot_completion(env.context, precommit=env.precommit,
            process=env.process, cell_id=env.arguments["cell_roster"][0]["cell_id"], slot=row["slot"],
            completion=crossed, artifact_roster=row["artifacts"], previous_commit_sha256=None,
            frozen_pipeline_config=env.frozen)
    assert caught.value.code == "DECLARED_SLOT_TRANSPORT_EDITION_MISMATCH"
    original = producer._read_declared_commit(env.context,
        producer._declared_commit_path(env.precommit, crossed["cell_id"], 1))[0]
    original["completion"] = crossed
    descriptor = spool.write_canonical_json_artifact(env.context, original)
    pointer = {"schema_version": producer._DECLARED_SLOT_POINTER_SCHEMA, "commit_descriptor": descriptor}
    path = env.context.root / producer._declared_commit_path(env.precommit, crossed["cell_id"], 1)
    # This is a corruption fixture in its own temporary tree, never a saved GREEN.
    path.write_bytes(forward._canonical_bytes(pointer))
    with pytest.raises(forward.ForwardOOSReplayProductionError) as caught:
        forward._reopen_declared_cell_inputs(env.context, precommit=env.precommit,
            process=env.process, frozen_pipeline_config=env.frozen)
    assert caught.value.code == "DECLARED_CELL_OWNER_PROGRESS_INVALID"
    assert caught.value.__cause__.code == "DECLARED_SLOT_TRANSPORT_EDITION_MISMATCH"
    assert env.authentication_calls == calls_before


@pytest.mark.parametrize("edition", ["v15", "v16"])
def test_synthetic_issued_missing_only_prefix_is_incomplete_without_account(tmp_path, monkeypatch, edition):
    """Real driver/cold thirteen-MISSING prefix; its source absences are invented."""
    def forbidden_engine(**kwargs):
        pytest.fail("A MISSING-only prefix cannot enter the account engine")

    monkeypatch.setattr(forward, "_run_declared_cell_account", forbidden_engine)
    env = _synthetic_reopened_declared_prefix(tmp_path, monkeypatch, edition, missing_only=True)
    inputs = forward._reopen_declared_cell_inputs(env.context, precommit=env.precommit,
        process=env.process, frozen_pipeline_config=env.frozen)
    assert inputs["progress_status"] == "INCOMPLETE" and len(inputs["commits"]) == 13
    assert [row["status"] for row in inputs["slot_schedule"]] == ["MISSING"] * 13
    assert inputs["owner_records"] == [] and inputs["progress"].terminal_stop is False
    assert env.driver_calls == [row["slot_id"] for row in inputs["slot_schedule"]]
    artifact = forward.produce_declared_cell_account_artifact(env.context, precommit=env.precommit,
        process=env.process, frozen_pipeline_config=env.frozen)
    projection = causal.read_declared_cell_account_artifact(env.context, artifact,
        process=env.process, frozen_pipeline_config=env.frozen)
    assert projection.progress_status == "INCOMPLETE" and projection.admissible is False
    assert projection.reason_code == "DECLARED_CELL_OWNER_PREFIX_INCOMPLETE"
    assert projection.account is None and projection.owner_records == ()
    assert all(getattr(projection, name) is False for name in (
        "qualification_authority", "capital_authority", "live_authority", "order_authority"))
    assert all(getattr(projection, name) is None for name in (
        "net_account_profit", "final_equity", "terminal", "first_depletion_bar", "max_drawdown_pct"))
    assert env.native_calls == []


@pytest.fixture
def synthetic_declared_transport(tmp_path, monkeypatch):
    """Real writer/manifest/reader, with explicitly synthetic prefix inputs.

    The prefix-authentication boundary is replaced here. Authentic prefix
    refusal is tested separately using the retained GREEN witness.
    """
    context = spool.freeze_generation_context(
        tmp_path / "synthetic-not-owner-authentication",
        generation_id="QA142_SYNTHETIC_TRANSPORT_NO_OWNER_AUTHORITY",
        native_universe_sha256="a" * 64, config_sha256="b" * 64,
    )
    config = PipelineConfig(
        research_mode=True, initial_capital=10_000.0,
        discovery_risk_per_trade_pct=1.0, use_spread=True,
        backtest_spread_mode="conservative_max",
    )
    start = pd.Timestamp("2025-01-01T00:00:00Z")
    slots = [{
        "slot_id": f"SYNTHETIC-STOP-{i + 1:02d}", "ordinal": i + 1,
        "activation_utc": (start + pd.Timedelta(days=28 * i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_exclusive_utc": (start + pd.Timedelta(days=28 * (i + 1))).strftime("%Y-%m-%dT%H:%M:%SZ"),
    } for i in range(13)]
    cell = {"cell_id": "e" * 64, "symbol": "QA142", "timeframe": "H1", "regime": "TREND"}
    commit = spool.write_canonical_json_artifact(context, {
        "classification": "INVENTED_TRANSPORT_ONLY_NOT_DECLARED_COMMIT",
        "qualification_authority": False,
    })
    summary = {
        "slot_id": slots[0]["slot_id"], "ordinal": 1, "status": "STOP",
        "request_sha256": "f" * 64, "completed_at_utc": slots[0]["activation_utc"],
        "commit_sha256": commit["sha256"],
    }
    progress = SimpleNamespace(
        slot_summaries=(summary,),
        slot_commit_artifacts=({"artifact_id": "synthetic-not-declared-commit",
            "artifact_type": "synthetic_not_declared_commit", "descriptor": commit},),
    )
    schedule = [{
        "slot_id": slot["slot_id"], "ordinal": slot["ordinal"],
        "start_utc": slot["activation_utc"], "end_exclusive_utc": slot["end_exclusive_utc"],
        "status": "STOP" if i == 0 else "MISSING", "owner_id": None,
    } for i, slot in enumerate(slots)]
    inputs = {
        "parent": {"cells": [cell], "slot_roster": {"slots": slots}},
        "config": config, "progress": progress, "commits": [], "owner_records": [],
        "slot_schedule": schedule, "progress_status": "STOP",
        "reason_code": "DECLARED_CELL_OWNER_PREFIX_STOP",
    }
    precommit, process = (SimpleNamespace(receipt_sha256=digit * 64) for digit in ("c", "d"))
    frozen = SimpleNamespace(
        active_config_path=tmp_path / "synthetic-config-not-authenticated.json",
        config_sha256=context.config_sha256, effective_config_sha256="0" * 64,
    )
    monkeypatch.setattr(forward, "_reopen_declared_cell_inputs", lambda *args, **kwargs: inputs)
    monkeypatch.setattr(forward.historical, "reopen_declared_slot_owner_run_precommit",
                        lambda *args, **kwargs: precommit)

    real_run = forward._run_declared_cell_account

    def no_engine(**kwargs):
        pytest.fail("A synthetic STOP must not enter the account engine")

    monkeypatch.setattr(forward, "_run_declared_cell_account", no_engine)
    artifact = forward.produce_declared_cell_account_artifact(
        context, precommit=precommit, process=process, frozen_pipeline_config=frozen,
    )
    body = json.loads((context.root / artifact.descriptor["relative_path"]).read_bytes())
    return SimpleNamespace(
        context=context, precommit=precommit, process=process, frozen=frozen,
        artifact=artifact, body=body, inputs=inputs, real_run=real_run,
    )


@pytest.fixture
def synthetic_declared_complete_inputs(synthetic_declared_transport, tmp_path, monkeypatch):
    """Invented authentication, real frame/profile/feature/deployment/account joins.

    The sparse UTC frame has an exact aligned H364 close. It is not a native
    continuity witness or thirteen authenticated OWNER selections.
    """
    from tests.test_forward_oos_replay_producer import (
        _sealed_identity_only_product,
        _synthetic_profile,
    )
    from tests.test_native_prefix_production import _execution_config
    from pm_regime import RegimeParams, regime_params_sha256
    import pm_native_prefix_production as native

    env = synthetic_declared_transport
    inputs, cell = env.inputs, env.inputs["parent"]["cells"][0]
    slots = inputs["parent"]["slot_roster"]["slots"]
    start, end = (pd.Timestamp(slots[0]["activation_utc"]),
                  pd.Timestamp(slots[-1]["end_exclusive_utc"]))
    cutoff = end.strftime("%Y-%m-%dT%H:%M:%SZ")
    prefix_index = pd.date_range(end=start - pd.Timedelta(hours=1), periods=220, freq="h")
    index = prefix_index.append(pd.DatetimeIndex([
        start, start + pd.Timedelta(hours=1), end - pd.Timedelta(hours=2),
        end - pd.Timedelta(hours=1),
    ]))
    frame = pd.DataFrame({
        "Open": 1.0, "High": 1.0002, "Low": 0.9998, "Close": 1.0,
        "Volume": 1.0, "Spread": 3.0,
        "_BAR_CLOSE_TS": index + pd.Timedelta(hours=1),
    }, index=index)
    fine_index = (index - pd.Timedelta(minutes=5)).append(
        pd.DatetimeIndex([end - pd.Timedelta(minutes=5)]))
    fine_frame = pd.DataFrame({
        "Open": 1.0, "High": 1.0002, "Low": 0.9998, "Close": 1.0,
        "Spread": 3.0, "_BAR_CLOSE_TS": fine_index + pd.Timedelta(minutes=5),
    }, index=fine_index)
    products = {timeframe: _sealed_identity_only_product(
        symbol=cell["symbol"], timeframe=timeframe, cutoff_utc=cutoff,
    ) for timeframe in ("H1", "M5")}
    identities = {timeframe: forward._native_product_identity(
        product, expected_symbol=cell["symbol"], expected_timeframe=timeframe,
        expected_cutoff_utc=cutoff,
        role="DECLARED_CELL_EXECUTION" if timeframe == "H1" else "DECLARED_CELL_SPREAD",
    ) for timeframe, product in products.items()}
    tiers = _synthetic_profile(
        tmp_path / "invented-profile", symbol=cell["symbol"],
        identity=identities["M5"],
    )
    profile_path = tmp_path / "invented-profile" / "synthetic_frozen_profile.json"
    inputs["parent"]["spread_profile_identity"] = {"profile_sha256": tiers["P75"].descriptor()["profile_sha256"]}
    config_path = _execution_config(tmp_path / "invented-deployment", symbol=cell["symbol"])
    config_body = json.loads(config_path.read_text(encoding="utf-8"))
    config_body.update({"pipeline": {"initial_capital": 10_000.0}, "position": {}})
    config_path.write_text(json.dumps(config_body, sort_keys=True), encoding="utf-8")
    config = inputs["config"]
    config.swap_model_enabled = False
    config._deployment_config_path = str(config_path)
    env.frozen.active_config_path = config_path
    spec = pm_core.InstrumentSpec(
        cell["symbol"], 5, point=0.00001, tick_size=0.00001,
        tick_value=1.25, pip_value=12.5, min_lot=0.01,
        max_lot=500.0, volume_step=0.01, asset_class="fx",
    )
    parameters = RegimeParams().to_dict()
    process = {"strategy_id": "EMACrossoverStrategy",
        "strategy_class": "EMACrossoverStrategy", "strategy_module": "pm_strategies"}
    owner = {
        "owner_id": "LABELLED_SYNTHETIC_OWNER_NOT_AUTHENTICATED",
        "cell_id": cell["cell_id"], "slot_id": slots[0]["slot_id"],
        "selected_tier": "P75",
        "selected_tier_contract": {"frozen_spread_tier": tiers["P75"].descriptor()},
        "selected_artifacts": {"winner_process": process},
        "selected_attempt": {"strategy_id": process["strategy_id"],
            "process_sha256": forward._sha(process), "encoded_parameters": canonicalize_parameter_mapping(
                forward.StrategyRegistry.get(process["strategy_id"]).get_default_params())},
        "regime_detector_record": {
            "parameters_canonical_json": json.dumps(parameters, sort_keys=True, separators=(",", ":")),
            "parameters_sha256": regime_params_sha256(parameters),
        },
    }
    prefix = frame.loc[prefix_index]
    count = {**cell, "fit_start_utc": prefix_index[0].isoformat(),
        "fit_end_exclusive_utc": start.isoformat(), "source_bar_count": len(prefix),
        "fit_frame_sha256": forward._frame_sha256(prefix),
        "latest_closed_bar_utc": start.isoformat()}
    count["receipt_sha256"] = forward._sha(count)
    inputs["owner_records"] = [{"owner": owner, "source_count_receipt": count}]
    summaries, artifacts = [], []
    for i, slot in enumerate(slots):
        status = "OWNER" if i == 0 else "NO_TRADE"
        descriptor = spool.write_canonical_json_artifact(env.context, {
            "classification": "INVENTED_COMPLETE_PREFIX_AUTHENTICATION_BOUNDARY",
            "slot_id": slot["slot_id"], "status": status,
            "qualification_authority": False,
        })
        summaries.append({"slot_id": slot["slot_id"], "ordinal": slot["ordinal"],
            "status": status, "request_sha256": "f" * 64,
            "completed_at_utc": slot["activation_utc"], "commit_sha256": descriptor["sha256"]})
        artifacts.append({"artifact_id": f"synthetic-commit/{i + 1:02d}",
            "artifact_type": "invented_not_authenticated_commit", "descriptor": descriptor})
        inputs["slot_schedule"][i].update({
            "status": status, "owner_id": owner["owner_id"] if i == 0 else None,
        })
    inputs.update({"progress_status": "COMPLETE", "reason_code": None,
        "progress": SimpleNamespace(slot_summaries=tuple(summaries),
            slot_commit_artifacts=tuple(artifacts))})

    def verify_native(**kwargs):
        product = next(item for item in products.values()
                       if item.index_sha256 == kwargs["expected_index_sha256"])
        assert kwargs["pinned_activation_manifest_sha256"] == identities["M5"]["activation_manifest_sha256"]
        return product

    def freeze_spec(*, product, config):
        deployment = forward._deployment_config(config)
        receipt = {
            "classification": "INVENTED_NATIVE_EXECUTION_SPEC_AUTHENTICATION_BOUNDARY",
            "deployment_config_sha256": deployment.config_sha256,
            "deployment_config_size_bytes": deployment.config_size_bytes,
            "broker_specs_relative_path": deployment.broker_relative_path,
            "broker_specs_sha256": deployment.broker_sha256,
            "broker_specs_size_bytes": deployment.broker_size_bytes,
            "native_prefix_index_sha256": product.index_sha256,
            "grants_search_authority": False, "grants_trading_authority": False,
        }
        receipt["receipt_sha256"] = forward._sha(receipt)
        return spec, receipt

    monkeypatch.setattr(native, "verify_activated_native_prefix", verify_native)
    monkeypatch.setattr(native, "open_verified_prefix_frame",
        lambda product: (frame if product.index["timeframe"] == "H1" else fine_frame).copy(deep=True))
    monkeypatch.setattr(forward.search_execution, "_freeze_bound_execution_spec", freeze_spec)
    monkeypatch.setattr(forward, "_run_declared_cell_account", env.real_run)
    monkeypatch.setattr(pm_core, "NUMBA_AVAILABLE", False)
    env.execution_frame, env.fine_frame, env.spec = frame, fine_frame, spec
    env.products, env.identities, env.profile_path = products, identities, profile_path
    return env


def test_labelled_synthetic_complete_writer_to_reader_keeps_actual_transport_joins(
    synthetic_declared_complete_inputs, monkeypatch,
):
    """Authentication is invented; this is not authentic thirteen-slot evidence."""
    env = synthetic_declared_complete_inputs
    run, prepare = forward._run_declared_cell_account, forward._declared_cell_native_frames
    run_calls, preparations = [], []
    deployment = forward._deployment_config(env.inputs["config"])
    fixed_bytes = {path: path.read_bytes() for path in (
        deployment.config_path, deployment.broker_path, env.profile_path,
    )}

    def run_once(**kwargs):
        run_calls.append(kwargs)
        return run(**kwargs)

    def prepare_actual(**kwargs):
        result = prepare(**kwargs)
        preparations.append((kwargs["profile"].sha256, result))
        return result

    monkeypatch.setattr(forward, "_run_declared_cell_account", run_once)
    monkeypatch.setattr(forward, "_declared_cell_native_frames", prepare_actual)
    artifact = forward.produce_declared_cell_account_artifact(
        env.context, precommit=env.precommit, process=env.process,
        frozen_pipeline_config=env.frozen, execution_product=env.products["H1"],
        spread_source_product=env.products["M5"], frozen_profile_path=env.profile_path,
    )
    body = json.loads((env.context.root / artifact.descriptor["relative_path"]).read_bytes())
    projection = causal.read_declared_cell_account_artifact(
        env.context, artifact, process=env.process, frozen_pipeline_config=env.frozen,
    )
    assert projection.progress_status == "COMPLETE" and projection.admissible is True
    assert projection.qualification_authority is False
    assert len(projection.slot_schedule) == 13 and len(projection.owner_records) == 1
    assert projection.account == body["account"]
    assert len(run_calls) == 1 and len(preparations) == 2
    assert run_calls[0]["config"].initial_capital == 10_000.0
    assert run_calls[0]["config"].discovery_risk_per_trade_pct == 0.25
    assert run_calls[0]["entry_schedule"].owners[0].owner_id == projection.owner_records[0]["owner"]["owner_id"]
    profile_descriptor = body["frozen_profile_descriptor"]
    assert profile_descriptor["sha256"] == preparations[0][0] == preparations[1][0]
    assert env.context.root / profile_descriptor["relative_path"] != env.profile_path
    assert (env.context.root / profile_descriptor["relative_path"]).read_bytes() == fixed_bytes[env.profile_path]
    for _, prepared in preparations:
        features, owners, _, observations, matched, warmup = prepared
        assert len(owners) == 1 and owners[0].spread_tier.descriptor()["profile_sha256"] == profile_descriptor["sha256"]
        assert warmup == 220 and observations.notna().all()
        assert pd.Timestamp(features["_BAR_CLOSE_TS"].iloc[-1]) == pd.Timestamp(body["measurement"]["end_exclusive_utc"])
        pd.testing.assert_index_equal(features.index, env.execution_frame.index)
        pd.testing.assert_index_equal(pd.DatetimeIndex(matched), features.index, check_names=False)
    for path, original in fixed_bytes.items():
        assert path.read_bytes() == original
    for role, path in (("config", deployment.config_path), ("broker", deployment.broker_path)):
        descriptor = body["deployment_source_descriptors"][role]
        assert (env.context.root / descriptor["relative_path"]).read_bytes() == fixed_bytes[path]
    with pytest.raises(causal.CausalEvidenceProductionError) as caught:
        projection.require_interval_qualification()
    assert caught.value.code == "DECLARED_CELL_INTERVAL_QUALIFICATION_NOT_IMPLEMENTED"


@pytest.mark.parametrize("source_case", ["truncated", "later_bar_without_exact_unaligned_close"])
def test_declared_terminal_boundary_requires_exact_source_close(
    synthetic_declared_complete_inputs, source_case,
):
    """Invented endpoints never retime the real Sunday 00:01 production roster."""
    env = synthetic_declared_complete_inputs
    inputs, frame = deepcopy(env.inputs), env.execution_frame.copy(deep=True)
    if source_case == "truncated":
        frame = frame.iloc[:-1].copy()
    else:
        for slot in inputs["parent"]["slot_roster"]["slots"]:
            for key in ("activation_utc", "end_exclusive_utc"):
                slot[key] = (pd.Timestamp(slot[key]) + pd.Timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        end = pd.Timestamp(env.inputs["parent"]["slot_roster"]["slots"][-1]["end_exclusive_utc"])
        later = frame.iloc[[-1]].copy()
        later.index = pd.DatetimeIndex([end])
        later["_BAR_CLOSE_TS"] = pd.DatetimeIndex([end + pd.Timedelta(hours=1)])
        frame = pd.concat((frame, later))
    profile = forward.spread_profile.load_spread_profile(env.profile_path)
    with pytest.raises(forward.ForwardOOSReplayProductionError) as caught:
        forward._declared_cell_native_frames(
            inputs=inputs, execution_frame=frame, fine_frame=env.fine_frame,
            profile=profile, spec=env.spec, execution_identity=env.identities["H1"],
            spread_identity=env.identities["M5"],
        )
    assert caught.value.code == "DECLARED_CELL_TERMINAL_BOUNDARY_UNAVAILABLE"


def test_declared_disk_transport_and_dispatch_do_not_qualify_synthetic_stop(
    synthetic_declared_transport,
):
    env = synthetic_declared_transport
    projection = causal.read_declared_cell_account_artifact(
        env.context, env.artifact, process=env.process, frozen_pipeline_config=env.frozen,
    )
    assert env.artifact.descriptor["format"] == spool.CANONICAL_JSON_FORMAT
    assert projection.descriptor_sha256 == env.artifact.descriptor["sha256"]
    assert projection.progress_status == "STOP" and not projection.admissible
    assert projection.account is None and not projection.owner_records
    assert len(projection.slot_schedule) == 13
    assert [row["status"] for row in projection.slot_schedule] == ["STOP"] + ["MISSING"] * 12
    measurement = env.body["measurement"]
    assert measurement["calendar_days"] == 364 and measurement["refit_cycles"] == 13
    assert measurement["cycle_days"] == 28 and measurement["risk_per_trade_pct"] == 0.25
    assert measurement["terminal_policy"] == "MARK_OPEN"
    assert pd.Timestamp(measurement["end_exclusive_utc"]) - pd.Timestamp(
        measurement["start_utc"]) == pd.Timedelta(days=364)
    assert env.body["genesis"] == {
        "mode": forward.NEW_SIMULATED_FLAT_GENESIS,
        "initial_cash": 10_000.0, "initial_inventory": [],
    }
    assert all(getattr(projection, name) is False for name in (
        "qualification_authority", "capital_authority", "live_authority", "order_authority",
    ))
    with pytest.raises(causal.CausalEvidenceProductionError) as caught:
        causal._load_replay_ledgers(
            env.context, env.artifact, {}, declared_process=env.process,
            declared_frozen_pipeline_config=env.frozen,
        )
    assert caught.value.code == "DECLARED_CELL_INTERVAL_QUALIFICATION_NOT_IMPLEMENTED"


@pytest.mark.parametrize("damage,code", [
    ("raw_bytes", "DECLARED_CELL_RAW_ACCOUNT_ARTIFACT_INTEGRITY_MISMATCH"),
    ("manifest_bytes", "DECLARED_CELL_RAW_MANIFEST_INVALID"),
])
def test_declared_reader_authenticates_physical_artifact_and_manifest(
    synthetic_declared_transport, damage, code,
):
    env = synthetic_declared_transport
    if damage == "raw_bytes":
        path = env.context.root / env.artifact.descriptor["relative_path"]
    else:
        path = env.context.root / spool._manifest_relative_path(
            forward._declared_cell_transport_symbol(env.body), spool.GENERAL_MANIFEST_DOMAIN,
        )
    original = path.read_bytes()
    path.write_bytes(original[:-1] + b"?")
    with pytest.raises(causal.CausalEvidenceProductionError) as caught:
        causal.read_declared_cell_account_artifact(
            env.context, env.artifact, process=env.process, frozen_pipeline_config=env.frozen,
        )
    assert caught.value.code == code


@pytest.mark.parametrize("damage,code", [
    ("commit", "DECLARED_CELL_TRANSPORT_BINDING_MISMATCH"),
    ("config", "DECLARED_CELL_TRANSPORT_BINDING_MISMATCH"),
    ("source", "DECLARED_CELL_INCOMPLETE_ACCOUNT_FORBIDDEN"),
    ("account", "DECLARED_CELL_INCOMPLETE_ACCOUNT_FORBIDDEN"),
])
def test_declared_transport_rejects_logical_join_tamper_at_synthetic_prefix_boundary(
    synthetic_declared_transport, damage, code,
):
    env = synthetic_declared_transport
    body = deepcopy(env.body)
    if damage == "commit":
        body["declared_commit_artifacts"][0]["descriptor"]["sha256"] = "0" * 64
    elif damage == "config":
        body["pipeline_config_binding"]["effective_config_sha256"] = "1" * 64
    elif damage == "source":
        body["execution_source"] = {"classification": "INVENTED_NOT_NATIVE"}
    elif damage == "account":
        body["account"] = {"classification": "INVENTED_NOT_AN_ACCOUNT"}

    def no_source(*args, **kwargs):
        pytest.fail("A partial declared prefix cannot reopen account inputs")

    with pytest.raises(forward.ForwardOOSReplayProductionError) as caught:
        forward.verify_declared_cell_account_transport(
            env.context, body, process=env.process, frozen_pipeline_config=env.frozen,
            reopen_source=no_source,
        )
    assert caught.value.code == code



