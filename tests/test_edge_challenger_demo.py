import inspect

import pm_main


def test_legacy_edge_demo_execution_path_is_removed():
    source = inspect.getsource(pm_main.FXPortfolioManagerApp.run_trading)
    constructor_source = inspect.getsource(pm_main.LiveTrader.__init__)
    assert "get_edge_challenger_configs" not in source
    assert "EDGE_CHALLENGER_DEMO" not in source
    assert "PAPER_ONLY" not in source
    assert "authorize_demo_probation_execution" not in source
    assert "authorize_demo_probation_execution" in constructor_source


def test_legacy_demo_constant_is_not_exposed():
    assert not hasattr(pm_main, "EDGE_DEMO_EXECUTION_BOOK")
    assert pm_main.CURRENT_REFIT_DEMO_EXECUTION_BOOK == (
        "PROCESS_VALIDATED_CURRENT_REFIT_DEMO"
    )
