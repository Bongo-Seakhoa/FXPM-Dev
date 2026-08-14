"""The unscoped resolver memo must be an optimisation, never a semantic change.

Resolving the active native universe re-reads, re-hashes and cross-resolution-
audits every leaf: measured 2026-08-08 at 12 GB across 567 bars.csv files and
1711.6 seconds per call, even with an empty symbol list. Callers outside a
ProductionNativeAuthorityVerificationScope paid that every time, which is why
six live-readiness tests consumed 99.96% of a 3h24m suite.

The memo pins results to the authority identity - the ACTIVE.json pointer bytes
and the sha256 of the manifest they name. These tests fix the contract that
makes that safe: it must recompute the instant the authority moves, and it must
never pin a universe that is invalid, mismatched, or captured while the
authority was changing. A cache that survives an authority change is not a
speedup, it is a system trading on evidence that no longer exists.
"""

import unittest
from pathlib import Path

import pm_native_activation as activation
from pm_native_activation import (
    ActiveNativeUniverse,
    _ActiveAuthorityIdentity,
    clear_unscoped_native_universe_memo,
)


MANIFEST_SHA = "a" * 64
OTHER_SHA = "b" * 64
RECEIPT_SHA = "d" * 64
OTHER_RECEIPT_SHA = "e" * 64
BUILD_DIGEST = "f" * 64
OTHER_BUILD_DIGEST = "1" * 64


def _identity(
    manifest_sha: str = MANIFEST_SHA,
    pointer: bytes = b"{}",
    *,
    receipt_sha: str = RECEIPT_SHA,
    build_digest: str = BUILD_DIGEST,
) -> _ActiveAuthorityIdentity:
    return _ActiveAuthorityIdentity(
        data_dir=Path("."),
        active_pointer_bytes=pointer,
        active_pointer_sha256="c" * 64,
        activation_manifest_sha256=manifest_sha,
        semantic_receipt_sha256=receipt_sha,
        semantic_verifier_build_digest=build_digest,
    )


def _universe(
    manifest_sha: str = MANIFEST_SHA, valid: bool = True
) -> ActiveNativeUniverse:
    # `valid` is derived: not errors and bool(activation_id).
    return ActiveNativeUniverse(
        activation_id="act-1",
        activation_manifest_sha256=manifest_sha,
        errors=[] if valid else ["NATIVE_ACTIVATION_INVALID"],
    )


class UnscopedUniverseMemoTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_unscoped_native_universe_memo()
        self.addCleanup(clear_unscoped_native_universe_memo)
        self.calls = 0
        # Capture the real module functions ONCE, before any test patches them,
        # and restore unconditionally. An earlier version registered its cleanup
        # with the already-patched value, so the stub survived the test and
        # every later authority test resolved against it - 7 passing alone, 22
        # failing downstream. Measured and reported by Codex in CX-013.
        self._real_identity = activation._read_active_authority_identity
        self._real_resolve = activation._resolve_active_native_universe_uncached
        self.addCleanup(
            setattr, activation, "_read_active_authority_identity", self._real_identity
        )
        self.addCleanup(
            setattr,
            activation,
            "_resolve_active_native_universe_uncached",
            self._real_resolve,
        )

    def _install(self, identities, universe_factory):
        """Patch the identity reader and the expensive resolver."""
        seq = list(identities)

        def fake_identity(_data_dir):
            return seq.pop(0) if len(seq) > 1 else seq[0]

        def fake_resolve(*_args, **_kwargs):
            self.calls += 1
            return universe_factory()

        activation._read_active_authority_identity = fake_identity
        activation._resolve_active_native_universe_uncached = fake_resolve

    def _call(self):
        return activation._resolve_unscoped_memoized(
            Path("."),
            None,
            required_timeframes=("M5",),
            verify_exclusion_evidence=True,
            production_contract_required=False,
        )

    def test_second_call_with_unchanged_authority_skips_verification(self) -> None:
        self._install([_identity()], _universe)
        first = self._call()
        second = self._call()
        self.assertEqual(self.calls, 1, "the 12 GB verification ran twice")
        self.assertEqual(
            first.activation_manifest_sha256, second.activation_manifest_sha256
        )

    def test_changed_authority_forces_reverification(self) -> None:
        """ACTIVE.json moving must invalidate immediately."""
        identities = [_identity(), _identity(), _identity(OTHER_SHA, b'{"x":1}')]
        self._install(identities, lambda: _universe(MANIFEST_SHA))
        self._call()
        activation._read_active_authority_identity = lambda _d: _identity(
            OTHER_SHA, b'{"x":1}'
        )
        self._call()
        self.assertEqual(self.calls, 2, "a changed authority reused a stale universe")

    def test_changed_receipt_identity_forces_reverification(self) -> None:
        """A coherent receipt replacement is part of the memo identity."""
        self._install([_identity()], _universe)
        self._call()
        activation._read_active_authority_identity = lambda _d: _identity(
            receipt_sha=OTHER_RECEIPT_SHA
        )
        self._call()
        self.assertEqual(
            self.calls, 2, "a changed semantic receipt reused a stale universe"
        )

    def test_changed_verifier_build_forces_reverification(self) -> None:
        """A verifier semantic change invalidates an otherwise identical authority."""
        self._install([_identity()], _universe)
        self._call()
        activation._read_active_authority_identity = lambda _d: _identity(
            build_digest=OTHER_BUILD_DIGEST
        )
        self._call()
        self.assertEqual(
            self.calls, 2, "a changed verifier build reused a stale universe"
        )

    def test_invalid_universe_is_never_pinned(self) -> None:
        self._install([_identity()], lambda: _universe(valid=False))
        self._call()
        self._call()
        self.assertEqual(self.calls, 2, "an invalid universe was cached")

    def test_manifest_mismatch_is_never_pinned(self) -> None:
        """A universe whose sha disagrees with the pointer must not be reused."""
        self._install([_identity()], lambda: _universe(manifest_sha=OTHER_SHA))
        self._call()
        self._call()
        self.assertEqual(self.calls, 2, "a mismatched universe was cached")

    def test_authority_moving_during_verification_is_never_pinned(self) -> None:
        """Identity read before and after must agree, or nothing is stored."""
        seq = [_identity(), _identity(OTHER_SHA, b'{"x":1}'), _identity(), _identity()]

        def drifting_identity(_data_dir):
            return seq.pop(0) if seq else _identity()

        def fake_resolve(*_a, **_k):
            self.calls += 1
            return _universe()

        # setUp captured the real functions and registered their restoration,
        # so patching here cannot leak into any later test.
        activation._read_active_authority_identity = drifting_identity
        activation._resolve_active_native_universe_uncached = fake_resolve
        self._call()
        self._call()
        self.assertEqual(self.calls, 2, "a universe captured mid-change was cached")

    def test_callers_cannot_mutate_the_pinned_universe(self) -> None:
        self._install([_identity()], _universe)
        first = self._call()
        first.cell_statuses["EURUSD"] = {"M5": "TAMPERED"}
        second = self._call()
        self.assertEqual(self.calls, 1)
        self.assertNotIn(
            "EURUSD", second.cell_statuses, "a caller mutated the memoized universe"
        )

    def test_unreadable_authority_falls_back_to_full_verification(self) -> None:
        def boom(_data_dir):
            raise RuntimeError("NATIVE_AUTHORITY_SCOPE_ACTIVE_POINTER_READ_FAILED")

        def fake_resolve(*_a, **_k):
            self.calls += 1
            return _universe()

        activation._read_active_authority_identity = boom
        activation._resolve_active_native_universe_uncached = fake_resolve
        self._call()
        self._call()
        self.assertEqual(self.calls, 2, "an unpinnable authority was cached anyway")

    def test_generator_symbols_are_not_consumed_by_key_construction(self) -> None:
        """configured_symbols is typed Iterable, so a generator is legal.

        Building the memo key used to exhaust it, leaving the resolver an empty
        iterator and silently resolving an EMPTY symbol universe - a wrong
        answer with no error. Measured by Codex in CX-013.
        """
        seen = []

        def fake_identity(_data_dir):
            return _identity()

        def fake_resolve(_data_dir, configured_symbols, **_kw):
            seen.append(
                None if configured_symbols is None else list(configured_symbols)
            )
            self.calls += 1
            return _universe()

        activation._read_active_authority_identity = fake_identity
        activation._resolve_active_native_universe_uncached = fake_resolve

        def symbols():
            yield "EURUSD"
            yield "GBPUSD"

        activation._resolve_unscoped_memoized(
            Path("."),
            symbols(),
            required_timeframes=("M5",),
            verify_exclusion_evidence=True,
            production_contract_required=False,
        )
        self.assertEqual(
            seen,
            [["EURUSD", "GBPUSD"]],
            "the resolver received an exhausted iterator and would have resolved "
            "an empty universe",
        )


if __name__ == "__main__":
    unittest.main()
