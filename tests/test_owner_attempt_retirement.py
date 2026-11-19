"""INVENTED routing fixture; no market/model or measured memory/performance authority."""
import gc
import json
import weakref

import pm_authoritative_search_execution as execution
from test_authoritative_search_execution import _attempt_boundary_invented_continuation


def test_full_panel_releases_closed_attempt_payloads_before_staging_next(tmp_path, monkeypatch):
    case = _attempt_boundary_invented_continuation(tmp_path, monkeypatch, attempt_count=2)
    pair_check = execution._route_a_pair_check
    write_stage = execution._write_staged_attempt
    prepare_owned = execution._FullPanelOwnerPreparation.prepare_owned
    signal_refs, pairs, written, replayed = [], [], [], []

    def checked_pair(**kwargs):
        pairs.append(tuple(sorted(kwargs["signals_by_tier"])))
        signal_refs.extend(weakref.ref(value) for value in kwargs["signals_by_tier"].values())
        return pair_check(**kwargs)

    def checked_stage(**kwargs):
        row = write_stage(**kwargs)
        gc.collect()
        assert len(pairs) == (row.ordinal + 1) * 12
        assert len(signal_refs) == (row.ordinal + 1) * 24
        assert all(reference() is None for reference in signal_refs[:row.ordinal * 24])
        written.append((row, json.loads(row.path.read_text(encoding="ascii"))))
        return row

    def checked_prepare_owned(self, *, prepare_attempt, **kwargs):
        def replay_current(ordinal):
            first = prepare_attempt(ordinal)
            if ordinal == 0:
                before = (sum(event[0] == "probe" for event in case.events),
                    sum(event[0] == "run" for event in case.events), len(pairs), len(signal_refs))
                assert before == (24, 24, 12, 24)
                repeated = prepare_attempt(ordinal)
                assert repeated == first and repeated is not first
                assert repeated[0] is not first[0] and repeated[1] is not first[1]
                assert (sum(event[0] == "probe" for event in case.events),
                    sum(event[0] == "run" for event in case.events), len(pairs), len(signal_refs)) == before
                replayed.append(ordinal)
            return first

        return prepare_owned(self, prepare_attempt=replay_current, **kwargs)

    monkeypatch.setattr(execution, "_route_a_pair_check", checked_pair)
    monkeypatch.setattr(execution, "_write_staged_attempt", checked_stage)
    monkeypatch.setattr(execution._FullPanelOwnerPreparation, "prepare_owned", checked_prepare_owned)
    result = case.run()
    gc.collect()

    assert type(result) is execution._PreparedRouteAFullPanelStage
    assert replayed == [0] and all(reference() is None for reference in signal_refs)
    assert len(result.rows) == len(case.staged) == len(written) == 2
    assert result.rows == tuple(row for row, _ in written)
    assert [row.ordinal for row in result.rows] == [0, 1]
    assert len(pairs) == 24 and set(pairs) == {("P75", "P95")}
    assert sum(event[0] == "probe" for event in case.events) == 48
    assert sum(event[0] == "run" for event in case.events) == 48
    assert case.reads == {"config": 2, "stream": 4}

    identities = json.loads(result.unit_identities_bytes.decode("ascii"))
    assert [unit["raw_attempt_id"] for unit in identities] == ["INVENTED-RAW", "INVENTED-RAW-1"]
    assert [unit["ordinal"] for unit in identities] == [0, 1]
    for unit, (row, envelope) in zip(identities, written, strict=True):
        evidence = {"attempt_id": unit["raw_attempt_id"],
            "folds": [{"fixture": ordinal} for ordinal in range(4)]}
        facts = [{"fixture_provenance": "INVENTED_NOT_KERNEL_EVIDENCE"} for _ in range(12)]
        assert envelope["attempt_evidence"] == result.unit_body(unit) == evidence
        assert envelope["matched_base_facts"] == result.matched_base_facts(row.ordinal) == facts
        assert envelope["declared_unit_binding_sha256"] == result.declared_unit_binding_sha256
        assert envelope["preparation_contract_sha256"] == result.preparation_contract_sha256
