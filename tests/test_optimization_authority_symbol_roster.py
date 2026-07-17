import pm_optimization_authority as authority


def test_production_mixed_case_symbols_are_valid_canonical_names() -> None:
    assert authority._valid_requested_symbol("Platinum")
    assert authority._valid_requested_symbol("Palladium")
    assert authority._valid_requested_symbol("COCA-COLA")


def test_requested_symbol_syntax_rejects_paths_and_whitespace() -> None:
    for value in (" Platinum", "Platinum ", "../EURUSD", "EUR/USD", "EUR|USD"):
        assert not authority._valid_requested_symbol(value)
