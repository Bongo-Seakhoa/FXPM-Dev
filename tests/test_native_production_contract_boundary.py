import json
from pathlib import Path

from pm_native_activation import (
    ACTIVE_POINTER_SCHEMA,
    ACTIVATION_MANIFEST_SCHEMA,
    NATIVE_PRODUCTION_TIMEFRAMES,
    resolve_active_native_artifact,
    resolve_active_native_universe,
    sha256_file,
)


PRODUCTION_CONTRACT_ERROR = (
    "NATIVE_UNIVERSE_PRODUCTION_EXPLICIT_CONTRACT_REQUIRED"
)


def _write_legacy_six_timeframe_active(data_dir: Path) -> None:
    """Write a hash-valid legacy ACTIVE with no universe contract."""

    native_root = data_dir / "native"
    activation_root = native_root / "activations"
    artifact_root = native_root / "legacy"
    activation_root.mkdir(parents=True)
    artifact_root.mkdir()

    bars_path = artifact_root / "bars.csv"
    bars_path.write_text(
        "time,Open,High,Low,Close,Volume\n"
        "2025-01-01T00:00:00Z,1,1,1,1,1\n",
        encoding="utf-8",
    )
    sidecar_path = artifact_root / "sidecar.json"
    sidecar_path.write_text("{}\n", encoding="utf-8")

    bars_sha256 = sha256_file(bars_path)
    sidecar_sha256 = sha256_file(sidecar_path)
    entries = {
        "EURUSD": {
            timeframe: {
                "status": "active",
                "symbol": "EURUSD",
                "timeframe": timeframe,
                "bars_relative_path": "legacy/bars.csv",
                "bars_sha256": bars_sha256,
                "sidecar_relative_path": "legacy/sidecar.json",
                "sidecar_sha256": sidecar_sha256,
            }
            for timeframe in NATIVE_PRODUCTION_TIMEFRAMES
        }
    }
    activation_id = "legacy-six-timeframe-no-contract"
    manifest = {
        "schema_version": ACTIVATION_MANIFEST_SCHEMA,
        "status": "active",
        "activation_id": activation_id,
        "entries": entries,
    }
    manifest_path = activation_root / f"{activation_id}.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    pointer = {
        "schema_version": ACTIVE_POINTER_SCHEMA,
        "activation_id": activation_id,
        "manifest_relative_path": f"activations/{activation_id}.json",
        "manifest_sha256": sha256_file(manifest_path),
    }
    (native_root / "ACTIVE.json").write_text(
        json.dumps(pointer, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def test_legacy_no_contract_is_readable_but_never_production_eligible(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    _write_legacy_six_timeframe_active(data_dir)

    forensic_universe = resolve_active_native_universe(
        data_dir,
        ["EURUSD"],
        required_timeframes=NATIVE_PRODUCTION_TIMEFRAMES,
    )
    forensic_artifact = resolve_active_native_artifact(
        data_dir,
        "EURUSD",
        "M5",
    )
    assert forensic_universe.valid, forensic_universe.errors
    assert not forensic_universe.explicit_contract
    assert forensic_artifact.valid, forensic_artifact.errors

    production_universe = resolve_active_native_universe(
        data_dir,
        ["EURUSD"],
        required_timeframes=NATIVE_PRODUCTION_TIMEFRAMES,
        production_contract_required=True,
    )
    production_artifact = resolve_active_native_artifact(
        data_dir,
        "EURUSD",
        "M5",
        production_contract_required=True,
    )
    assert not production_universe.valid
    assert PRODUCTION_CONTRACT_ERROR in production_universe.errors
    assert not production_artifact.valid
    assert PRODUCTION_CONTRACT_ERROR in production_artifact.errors
