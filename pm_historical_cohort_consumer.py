"""Inert Source337 cohort declaration for the isolated historical consumer."""

from pathlib import Path
from types import MappingProxyType

import pm_authority_spool as spool
import pm_historical_persisted_exec as persisted
import pm_signal_opportunities as opportunities
from pm_core import Backtester
from pm_historical_exec_unit_projection import _project_batch, _uses_base_stops_without_session


_EXECUTION_PINS = (
    "generation_context_sha256", "producer_request_sha256", "executor_release_sha256",
    "producer_source_sha256", "horizon_plan_receipt_sha256", "cell_plan_receipt_sha256",
    "source_group_sha256", "regime_fit_sha256", "frame_sha256", "fitted_state_reference_sha256",
    "execution_spec_sha256", "frozen_receipt_sha256", "cost_precommitments_sha256",
    "fold_plan_sha256", "attempt_roster_sha256",
)


class CohortConsumerError(ValueError):
    """An isolated consumer declaration or dependent publication is invalid."""


class DeclaredCohortConsumer:
    """Describe one exact cell's use of an already published owned ledger."""

    def __init__(self, *, unit_context, unit_id, unit_contract_sha256, cell_id, cell_roots,
            receipt, receipt_sha256, expected, max_rows, max_bytes, expected_request_sha256):
        receipt_identity = receipt.get("identity") if type(receipt) is dict else None
        if (type(unit_context) is not spool.GenerationSpoolContext or cell_id not in cell_roots
                or type(receipt_identity) is not dict
                or receipt.get("schema_version") != persisted.stream.RECEIPT_SCHEMA
                or not all(spool._is_sha256(value) for value in (
                    unit_id, unit_contract_sha256, cell_id, receipt_sha256, expected_request_sha256,
                    receipt_identity.get("ledger_sha256"), receipt_identity.get("expected_roster_sha256")))
                or receipt_identity.get("unit_id") != unit_id
                or receipt_identity.get("unit_contract_sha256") != unit_contract_sha256
                or receipt_identity.get("generation_context_sha256") != unit_context.context_sha256
                or receipt_identity.get("authority_scope") != "INVENTED_SOFTWARE_ONLY"
                or receipt_identity.get("max_rows") != max_rows or receipt_identity.get("max_bytes") != max_bytes
                or persisted._sha(receipt) != receipt_sha256):
            raise CohortConsumerError("DECLARED_COHORT_RECEIPT_BINDING_INVALID")
        self._unit_context = unit_context
        self._cell_root = Path(cell_roots[cell_id])
        self._expected_request = expected_request_sha256
        self._identity = MappingProxyType({"unit_id": unit_id, "unit_contract_sha256": unit_contract_sha256,
            "cell_id": cell_id, "ledger_sha256": receipt_identity["ledger_sha256"],
            "receipt_sha256": receipt_sha256, "expected_roster_sha256": receipt_identity["expected_roster_sha256"]})
        self._read_arguments = {"unit_id": unit_id, "unit_contract_sha256": unit_contract_sha256,
            "cell_roots": dict(cell_roots), "expected": dict(expected), "expected_receipt_sha256": receipt_sha256,
            "max_rows": max_rows, "max_bytes": max_bytes}

    def identity(self):
        return dict(self._identity)

    def bind(self, *, cell_context, request_body, execution_binding, recorded_identity):
        """Accept the runtime's verified, frozen execution binding without reading the ledger."""
        if (type(cell_context) is not spool.GenerationSpoolContext or cell_context.root != self._cell_root
                or type(request_body) is not dict or type(execution_binding) is not dict
                or recorded_identity != self._identity or request_body.get("cell_id") != self._identity["cell_id"]
                or execution_binding.get("generation_context_sha256") != cell_context.context_sha256
                or execution_binding.get("producer_request_sha256") != self._expected_request
                or not all(spool._is_sha256(execution_binding.get(field)) for field in _EXECUTION_PINS)
                or persisted._sha(request_body) != self._expected_request):
            return None
        dependency_sha256 = persisted._sha({"schema_version": "fxpm.declared-cohort-cell-dependency.v1",
            "execution": {field: execution_binding[field] for field in _EXECUTION_PINS},
            "cohort": dict(self._identity)})
        return _BoundCohortConsumer(self._unit_context, self._identity, self._read_arguments,
            dependency_sha256)


class _BoundCohortConsumer:
    def __init__(self, unit_context, identity, read_arguments, dependency_sha256):
        self._unit_context = unit_context
        self._identity = identity
        self._read_arguments = read_arguments
        self._dependency_sha256 = dependency_sha256
        self._batches = None
        self._matched = False
        self._pin_receipt = None

    @property
    def used(self):
        return self._matched

    def project(self, *, _before_read=None, **view):
        inputs = view["evaluation_kwargs"]
        slot = view["view_slot"]
        expected = self._read_arguments["expected"].get(slot)
        try:
            compatible = (expected is not None
                and view["evaluate"] is opportunities.evaluate_independent_signal_r
                and type(inputs["pricing"]) is Backtester
                and _uses_base_stops_without_session(inputs["strategy"], inputs["spec"])
                and expected["compatibility_sha256"] == persisted._compatibility_sha(view))
        except (TypeError, ValueError):
            compatible = False
        if not compatible:
            return None
        if self._batches is None:
            if _before_read is not None:
                _before_read()
            self._batches = persisted.read_cohorts(self._unit_context, **self._read_arguments)
        result = _project_batch(self._batches[slot], view["features"], view["masked_signals"],
            view["regime"], cutoff=inputs["cutoff_utc"], start=inputs["signal_start_utc"])
        self._matched |= result is not None
        return result

    def pin(self, attempt_payload_sha256):
        if not self._matched or not spool._is_sha256(attempt_payload_sha256):
            raise CohortConsumerError("DECLARED_COHORT_DEPENDENCY_INVALID")
        if self._pin_receipt is None:
            # The attempt digest is verified by Exec. The immutable pin holds the whole cell dependency.
            self._pin_receipt = spool.publish_unit_projection_pin(self._unit_context,
                cell_id=self._identity["cell_id"], projection_sha256=self._dependency_sha256,
                ledger_sha256=self._identity["ledger_sha256"], receipt_sha256=self._identity["receipt_sha256"])
        return self._pin_receipt
