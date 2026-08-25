"""Cross-seam attacks on the bounded raw-Winner canary planner boundary.

The orchestration-side issuer/verify/consume attacks live in
tests/test_historical_anchor_orchestration.py.  This module attacks the
planner seam: only an issuer-authenticated canary request plus the
executor-facade capability objects may open a canary cell plan, and the
canary capability can never leak into ordinary production planning.
"""

from __future__ import annotations

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

import pm_authoritative_search_execution as search_execution
import pm_book_multiplicity as book
import pm_historical_anchor_orchestration as orchestration
import pm_production_search_planner as search_planner
import tools.run_raw_winner_canary as canary_cli
from tests.test_historical_anchor_orchestration import _canary_prerequisites


def _native_authority_verification() -> dict[str, object]:
    return {
        "schema_version": "fxpm.production_native_authority_verification_scope.v1",
        "scope": "9" * 64,
        "full_verification_count": 1,
        "identity_revalidation_attempt_count": 5,
        "identity_revalidation_count": 5,
        "identity_revalidation_failure_count": 0,
        "active_pointer_sha256": "8" * 64,
        "activation_manifest_sha256": "2" * 64,
        "pid": 1234,
        "thread_id": 5678,
        "poison_reason": None,
        "closed": True,
        "poisoned": False,
    }


class _FakeNativeAuthorityScope:
    def __init__(self) -> None:
        self.calls = 0
        self.closed = False
        self.exit_called = False

    def __enter__(self) -> "_FakeNativeAuthorityScope":
        return self

    def __exit__(self, *_args: object) -> None:
        self.exit_called = True
        self.close()

    def record_call(self) -> None:
        assert not self.closed
        self.calls += 1

    def close(self) -> None:
        self.closed = True

    def stats(self) -> dict[str, object]:
        stats = _native_authority_verification()
        stats["full_verification_count"] = min(self.calls, 1)
        stats["identity_revalidation_attempt_count"] = max(self.calls - 1, 0)
        stats["identity_revalidation_count"] = max(self.calls - 1, 0)
        stats["closed"] = self.closed
        return stats


def _issued_request(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    context, process, causal_precommit, _plan = _canary_prerequisites(
        tmp_path, monkeypatch
    )
    request = orchestration.issue_bounded_raw_winner_canary_request(
        context,
        data_dir=tmp_path,
        anchor_plan_receipt=object(),
        search_process=process,
        causal_precommit=causal_precommit,
        symbol=book.PINNED_SYMBOL_ORDER[0],
        timeframe=book.TIMEFRAME_ORDER[0],
    )
    return context, process, causal_precommit, request


def test_planner_rejects_non_canary_request_object(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A planning request or arbitrary object cannot open the canary builder."""
    context, process, causal_precommit, _request = _issued_request(
        tmp_path, monkeypatch
    )
    for substitute in (object(), {"schema_version": "planning"}, None):
        with pytest.raises(Exception, match="CANARY|PRODUCER_REQUEST"):
            search_planner.build_bounded_raw_winner_canary_observed_cell_plan(
                context,
                process=process,
                request=substitute,
                causal_precommit_payload=causal_precommit.to_dict(),
                source_session=object(),
                plan_permit=object(),
            )


def test_planner_rejects_tampered_causal_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The raw payload must re-hash to the receipt bound into the request."""
    context, process, causal_precommit, request = _issued_request(tmp_path, monkeypatch)
    tampered = causal_precommit.to_dict()
    tampered["evidence_access_deadline_utc"] = "2099-01-01T00:00:00Z"
    with pytest.raises(
        search_planner.ProductionSearchPlannerError,
        match="CANARY_CAUSAL",
    ):
        search_planner.build_bounded_raw_winner_canary_observed_cell_plan(
            context,
            process=process,
            request=request,
            causal_precommit_payload=tampered,
            source_session=object(),
            plan_permit=object(),
        )


def test_planner_requires_executor_owned_source_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A caller-forged source session/permit can never satisfy the capability."""
    context, process, causal_precommit, request = _issued_request(tmp_path, monkeypatch)

    class _FakeSession:
        pass

    for fake in (object(), _FakeSession(), None):
        with pytest.raises(Exception, match="CANARY"):
            search_planner.build_bounded_raw_winner_canary_observed_cell_plan(
                context,
                process=process,
                request=request,
                causal_precommit_payload=causal_precommit.to_dict(),
                source_session=fake,
                plan_permit=object(),
            )


def test_source_session_cannot_be_directly_constructed_or_verified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Direct construction misses the executor's closure HMAC and object state."""
    fields = getattr(
        search_execution.BoundedRawWinnerCanarySourceSession, "__slots__", ()
    ) or [
        name
        for name in vars(search_execution.BoundedRawWinnerCanarySourceSession).get(
            "__dataclass_fields__", {}
        )
    ]
    assert fields, "session must declare bindable fields"
    # Constructing with junk must not produce something the executor accepts:
    # the closure key was never applied, so any downstream verification fails.
    try:
        forged = object.__new__(search_execution.BoundedRawWinnerCanarySourceSession)
    except TypeError:
        return
    context, process, causal_precommit, request = _issued_request(tmp_path, monkeypatch)
    with pytest.raises(Exception, match="CANARY"):
        search_planner.build_bounded_raw_winner_canary_observed_cell_plan(
            context,
            process=process,
            request=request,
            causal_precommit_payload=causal_precommit.to_dict(),
            source_session=forged,
            plan_permit=object(),
        )


def test_manual_request_consumption_burns_real_executor_facade(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context, process, causal_precommit, request = _issued_request(tmp_path, monkeypatch)
    orchestration.consume_bounded_raw_winner_canary_request(context, request)

    with pytest.raises(Exception, match="CANARY_REQUEST_ALREADY_CONSUMED"):
        search_execution.execute_bounded_raw_winner_canary(
            context=context,
            request=request,
            process=process,
            causal_precommit_payload=causal_precommit.to_dict(),
            frozen_pipeline_config=object(),
        )


def test_request_consumption_is_atomic_under_concurrency(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context, _process, _causal_precommit, request = _issued_request(
        tmp_path, monkeypatch
    )

    def consume() -> str:
        try:
            orchestration.consume_bounded_raw_winner_canary_request(context, request)
            return "accepted"
        except Exception as exc:  # exact typed text is asserted below
            return str(exc)

    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(lambda _index: consume(), range(8)))
    assert outcomes.count("accepted") == 1
    assert all(
        value == "accepted" or "CANARY_REQUEST_ALREADY_CONSUMED" in value
        for value in outcomes
    )


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        (
            {
                "status": "RAW_WINNER_VERIFIED",
                "activation_manifest_sha256": "2" * 64,
                "native_authority_verification": _native_authority_verification(),
                "raw_winner_completed": True,
                "standalone_winner_verifier_accepted": True,
                "failure_code": None,
            },
            True,
        ),
        (
            {
                "status": "RAW_WINNER_VERIFIED_ALL_PROCESS_REJECTED",
                "activation_manifest_sha256": "2" * 64,
                "native_authority_verification": _native_authority_verification(),
                "raw_winner_completed": True,
                "standalone_winner_verifier_accepted": True,
                "failure_code": None,
            },
            True,
        ),
        (
            {
                "status": "RAW_WINNER_TECHNICAL_FAILURE",
                "activation_manifest_sha256": "2" * 64,
                "native_authority_verification": _native_authority_verification(),
                "raw_winner_completed": False,
                "standalone_winner_verifier_accepted": False,
                "failure_code": "RAW_EVIDENCE_INCOMPLETE",
            },
            False,
        ),
    ],
)
def test_cli_mechanical_success_requires_verified_raw_completion(
    body: dict, expected: bool
) -> None:
    assert canary_cli._is_mechanical_success(body) is expected


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("full_verification_count", 2),
        ("identity_revalidation_attempt_count", 4),
        ("identity_revalidation_count", 4),
        ("identity_revalidation_failure_count", 1),
        ("active_pointer_sha256", "7"),
        ("activation_manifest_sha256", "7" * 64),
        ("closed", False),
        ("poisoned", True),
    ],
)
def test_cli_mechanical_success_rejects_tampered_scope_metrics(
    field: str, value: object
) -> None:
    verification = _native_authority_verification()
    verification[field] = value
    body = {
        "status": "RAW_WINNER_VERIFIED",
        "activation_manifest_sha256": "2" * 64,
        "native_authority_verification": verification,
        "raw_winner_completed": True,
        "standalone_winner_verifier_accepted": True,
        "failure_code": None,
    }
    assert canary_cli._is_mechanical_success(body) is False


def _mock_cli_dependencies(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    body: dict,
) -> tuple[Path, list[Path], _FakeNativeAuthorityScope]:
    import pm_authority_spool as authority_spool
    import pm_cap_calibration as cap
    import pm_causal_evidence_producer as causal_producer
    import pm_historical_production_adapter as historical_adapter
    import pm_native_activation as native_activation
    import pm_portfolio_replay as portfolio_replay
    import pm_production_anchor_plan as anchor_plan
    import pm_production_config_binding as config_binding

    config_path = tmp_path / "config.json"
    config_path.write_text("{}\n", encoding="utf-8")
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    observed_data_roots: list[Path] = []
    frozen_context_calls: list[tuple[Path, object]] = []
    native_scope = _FakeNativeAuthorityScope()
    monkeypatch.setattr(canary_cli, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        canary_cli.tempfile,
        "gettempdir",
        lambda: str(tmp_path / "host-temp"),
    )
    monkeypatch.setattr(
        config_binding,
        "load_effective_pipeline_config",
        lambda _path: ({}, SimpleNamespace(data_dir=data_dir)),
    )

    def resolve(observed: Path, *_args, **_kwargs):
        native_scope.record_call()
        observed_data_roots.append(Path(observed))
        return SimpleNamespace(valid=True, errors=[])

    monkeypatch.setattr(native_activation, "resolve_active_native_universe", resolve)

    def portfolio_contract(observed: Path, *_args, **_kwargs):
        native_scope.record_call()
        observed_data_roots.append(Path(observed))
        return {"activation_id": "test"}, []

    monkeypatch.setattr(
        portfolio_replay, "active_native_portfolio_contract", portfolio_contract
    )
    monkeypatch.setattr(
        anchor_plan,
        "derive_production_anchor_plan",
        lambda observed: (
            native_scope.record_call(),
            observed_data_roots.append(Path(observed)),
            object(),
        )[-1],
    )
    monkeypatch.setattr(
        anchor_plan,
        "verify_production_anchor_plan_receipt",
        lambda _observed, _receipt: (
            native_scope.record_call(),
            {"anchor_plan": []},
        )[-1],
    )
    monkeypatch.setattr(
        native_activation,
        "production_native_authority_verification_scope",
        lambda: native_scope,
    )
    context = SimpleNamespace(config_sha256=cap.canonical_json_sha256({}))

    def freeze_context(root, **kwargs):
        frozen_context_calls.append((Path(root), kwargs["limits"]))
        return context

    monkeypatch.setattr(
        authority_spool, "freeze_generation_context", freeze_context
    )
    process = SimpleNamespace(receipt_sha256="a" * 64)
    monkeypatch.setattr(
        search_planner, "freeze_production_search_process", lambda *_a, **_k: process
    )
    causal = SimpleNamespace(to_dict=lambda: {})
    monkeypatch.setattr(
        causal_producer,
        "freeze_causal_process_precommit_v9",
        lambda **_kwargs: causal,
    )
    request = object()
    def issue(*_args, **kwargs):
        assert kwargs["native_authority_verification_scope"] is native_scope
        native_scope.record_call()
        native_scope.record_call()
        native_scope.close()
        return request

    monkeypatch.setattr(
        orchestration, "issue_bounded_raw_winner_canary_request", issue
    )
    frozen_config = object()

    def freeze_config(*_args, **_kwargs):
        assert native_scope.closed
        assert native_scope.calls == 6
        return frozen_config

    monkeypatch.setattr(
        config_binding,
        "freeze_production_pipeline_config",
        freeze_config,
    )
    receipt = object()

    def execute(**_kwargs):
        assert native_scope.closed
        assert native_scope.calls == 6
        return receipt

    monkeypatch.setattr(
        search_execution,
        "execute_bounded_raw_winner_canary",
        execute,
    )
    monkeypatch.setattr(
        historical_adapter,
        "verify_bounded_raw_winner_canary_receipt",
        lambda *_args, **_kwargs: {
            **body,
            "activation_manifest_sha256": "2" * 64,
            "native_authority_verification": native_scope.stats(),
        },
    )
    return config_path, observed_data_roots, native_scope, frozen_context_calls


@pytest.mark.parametrize(
    ("status", "expected_exit"),
    [
        ("RAW_WINNER_VERIFIED", 0),
        ("RAW_WINNER_VERIFIED_ALL_PROCESS_REJECTED", 0),
        ("RAW_WINNER_TECHNICAL_FAILURE", 2),
    ],
)
def test_cli_main_uses_config_data_root_and_verified_receipt_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status: str,
    expected_exit: int,
) -> None:
    successful = status != "RAW_WINNER_TECHNICAL_FAILURE"
    body = {
        "status": status,
        "raw_winner_completed": successful,
        "standalone_winner_verifier_accepted": successful,
        "failure_code": None if successful else "RAW_EVIDENCE_INCOMPLETE",
    }
    config_path, observed, native_scope, frozen_context_calls = _mock_cli_dependencies(
        tmp_path, monkeypatch, body
    )

    result = canary_cli.main(["--config", str(config_path)])

    assert result == expected_exit
    assert observed
    assert all(path == tmp_path / "data" for path in observed)
    assert all(path.name != "native" for path in observed)
    assert native_scope.calls == 6
    assert native_scope.stats()["full_verification_count"] == 1
    assert native_scope.stats()["identity_revalidation_count"] == 5
    assert native_scope.closed is True
    assert native_scope.exit_called is True
    assert len(frozen_context_calls) == 1
    root, limits = frozen_context_calls[0]
    assert root.parent.parent == tmp_path / "host-temp" / "fxpm-raw-winner-canary"
    assert limits.max_artifact_bytes == canary_cli.CANARY_SPOOL_MAX_BYTES
    assert limits.max_symbol_bytes == canary_cli.CANARY_SPOOL_MAX_BYTES
    assert limits.max_generation_bytes == canary_cli.CANARY_SPOOL_MAX_BYTES
    assert limits.min_free_bytes == canary_cli.CANARY_SPOOL_MIN_FREE_BYTES


def test_cli_rejects_diagnostic_root_outside_bounded_base(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    body = {
        "status": "RAW_WINNER_VERIFIED",
        "raw_winner_completed": True,
        "standalone_winner_verifier_accepted": True,
        "failure_code": None,
    }
    config_path, _observed, native_scope, _frozen_context_calls = _mock_cli_dependencies(
        tmp_path, monkeypatch, body
    )
    with pytest.raises(SystemExit) as caught:
        canary_cli.main(
            [
                "--config",
                str(config_path),
                "--diagnostic-root",
                str(tmp_path / "outside-canary-base"),
            ]
        )
    assert caught.value.code == 2
    assert "CANARY_DIAGNOSTIC_ROOT_OUTSIDE_BOUNDED_BASE" in capsys.readouterr().out
    assert native_scope.closed is True
    assert native_scope.exit_called is True


def test_cli_malformed_config_failure_is_typed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text('{"pipeline":', encoding="utf-8")
    monkeypatch.setattr(canary_cli, "PROJECT_ROOT", tmp_path)

    with pytest.raises(SystemExit) as caught:
        canary_cli.main(["--config", str(config_path)])

    assert caught.value.code == 2
    output = capsys.readouterr().out
    assert "CANARY_CLI_CONFIG_INVALID" in output
    assert "ACTIVE_CONFIG_READ_FAILED" in output
