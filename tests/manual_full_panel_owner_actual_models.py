"""MANUAL explicit target only; no automatic full-model run is authorized.

Requires Original-approved coherent-source runtime scope. This file is deliberately
outside pytest's default test_*.py pattern. It retains every original trial/fold.
"""
from tests.test_full_panel_owner_integration import _run_fresh_full_panel_owner_fixture
from tests.test_authoritative_search_execution import _route_a_owned_invented_native_source


def test_fresh_full_panel_owner_actual_models_manual(tmp_path, monkeypatch,
        _route_a_owned_invented_native_source, record_property):
    _run_fresh_full_panel_owner_fixture(tmp_path, monkeypatch,
        _route_a_owned_invented_native_source, record_property, instrument_models=False)
