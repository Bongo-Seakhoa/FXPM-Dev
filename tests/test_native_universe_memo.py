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


def _identity(manifest_sha: str = MANIFEST_SHA, pointer: bytes = b"{}") -> _ActiveAuthorityIdentity:
    return _ActiveAuthorityIdentity(
        data_dir=Path("."),
        active_pointer_bytes=pointer,
        active_pointer_sha256="c" * 64,
        activation_manifest_sha256=manifest_sha,
    )


def _universe(manifest_sha: str = MANIFEST_SHA, valid: bool = True) -> ActiveNativeUniverse:
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

    def _install(self, identities, universe_factory):
        """Patch the identity reader and the expensive resolver."""
        seq = list(identities)

        def fake_identity(_data_dir):
            return seq.pop(0) if len(seq) > 1 else seq[0]

        def fake_resolve(*_args, **_kwargs):
            self.calls += 1
            return universe_factory()

        self._orig_identity = activation._read_active_authority_identity
        self._orig_resolve = activation._resolve_active_native_universe_uncached
        activation._read_active_authority_identity = fake_identity
        activation._resolve_active_native_universe_uncached = fake_resolve
        self.addCleanup(setattr, activation, "_read_active_authority_identity", self._orig_identity)
        self.addCleanup(
            setattr, activation, "_resolve_active_native_universe_uncached", self._orig_resolve
        )

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
        self.assertEqual(first.activation_manifest_sha256, second.activation_manifest_sha256)

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

        activation._read_active_authority_identity = drifting_identity
        activation._resolve_active_native_universe_uncached = fake_resolve
        self.addCleanup(
            setattr, activation, "_read_active_authority_identity",
            activation._read_active_authority_identity,
        )
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

        orig_i = activation._read_active_authority_identity
        orig_r = activation._resolve_active_native_universe_uncached
        activation._read_active_authority_identity = boom
        activation._resolve_active_native_universe_uncached = fake_resolve
        self.addCleanup(setattr, activation, "_read_active_authority_identity", orig_i)
        self.addCleanup(setattr, activation, "_resolve_active_native_universe_uncached", orig_r)
        self._call()
        self._call()
        self.assertEqual(self.calls, 2, "an unpinnable authority was cached anyway")


if __name__ == "__main__":
    unittest.main()
