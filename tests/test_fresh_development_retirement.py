"""INVENTED contract tags only; no process, source or trading authority."""
import copy

import pytest

import pm_historical_scheduler as scheduler


@pytest.mark.parametrize("schema", [scheduler.DECLARED_DEVELOPMENT_CELL_SCHEDULER_SCHEMA,
    scheduler.DECLARED_M5_DEVELOPMENT_SCHEDULER_SCHEMA])
def test_fresh_frontdoor_does_not_reissue_a_legacy_development_schema(monkeypatch, schema):
    contract = {"schema_version": schema, "cells": []}
    before = copy.deepcopy(contract)

    def forbidden(*args, **kwargs):
        pytest.fail("Legacy-schema rejection issued a process or source window")

    monkeypatch.setattr(scheduler, "_declared_initial_process", forbidden)
    monkeypatch.setattr(scheduler, "_declared_utc_now", forbidden)
    with pytest.raises(scheduler.HistoricalSchedulerError,
            match="^DECLARED_FRESH_ORDINARY_CONTRACT_REQUIRED$"):
        scheduler.compile_fresh_declared_contract(contract)
    assert contract == before
