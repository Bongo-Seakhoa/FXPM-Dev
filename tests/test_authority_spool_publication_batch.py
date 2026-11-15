"""O1 cut 1: batch publication keeps published bytes, names and the final quota state identical to per-object publication."""
import hashlib
import json
import os
import threading
import unittest
from contextlib import ExitStack
from pathlib import Path

import pytest

import pm_authority_spool as spool
import pm_authority_spool_quota as quota

FORMAT = "fxpm-ascii-json-v1"
SIZE = 4689


def payload(i, size=SIZE):
    head = json.dumps({"i": f"{i:08d}", "pad": ""}, separators=(",", ":"))
    return json.dumps({"i": f"{i:08d}", "pad": "x" * (size - len(head))}, separators=(",", ":")).encode("ascii")


def tree(root):
    out = {}
    for base, _, names in os.walk(Path(root) / "objects"):
        for name in names:
            path = Path(base) / name
            out[path.relative_to(root).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    return out


def state(root):
    return json.loads((Path(root) / quota.QUOTA_STATE_NAME).read_bytes())


class PublicationBatchTests(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def _scratch(self, tmp_path):
        self.tmp = tmp_path                                       # pytest's own base directory, no system temp
        self.bases = {}

    def context(self, name, **limits):
        """A context with internally consistent limits; records the committed bytes the context file itself accounts for."""
        default = spool.SpoolLimits()
        generation = limits.get("max_generation_bytes", default.max_generation_bytes)
        artifact = limits.setdefault("max_artifact_bytes", min(default.max_artifact_bytes, generation))
        limits.setdefault("max_symbol_bytes", min(default.max_symbol_bytes, generation))
        for key in ("max_manifest_bytes", "max_json_bytes", "stream_chunk_bytes"):
            limits.setdefault(key, min(getattr(default, key), artifact))
        ctx = spool.freeze_generation_context(self.tmp / name, generation_id="cut1", native_universe_sha256="0" * 64,
                                              config_sha256="1" * 64, limits=spool.SpoolLimits(**limits))
        self.bases[ctx.root] = state(ctx.root)["committed_bytes"]
        return ctx

    def capped(self, name, objects, slack=0):
        """A context whose generation cap leaves room for exactly `objects` payloads plus `slack` bytes."""
        probe = self.context(name + "_probe", max_generation_bytes=50_000)
        cap = self.bases[probe.root] + objects * SIZE + slack
        ctx = self.context(name, max_generation_bytes=cap)
        self.assertEqual(self.bases[ctx.root] + objects * SIZE + slack, cap)
        return ctx

    def assertQuota(self, ctx, published_bytes):
        s = state(ctx.root)
        self.assertEqual((s["committed_bytes"], s["reservations"]), (self.bases[ctx.root] + published_bytes, []))

    def fixed_disk_free(self, value):
        original = quota._disk_free
        quota._disk_free = lambda root: value
        self.addCleanup(setattr, quota, "_disk_free", original)

    def test_batch_matches_per_object_publication(self):
        payloads = [payload(i) for i in range(40)]
        self.assertEqual({len(p) for p in payloads}, {SIZE})
        plain = self.context("plain"); batch = self.context("batch")
        expected = [spool.write_opaque_artifact(plain, [p], format=FORMAT) for p in payloads]
        with spool.publication_batch(batch):
            actual = [spool.write_opaque_artifact(batch, [p], format=FORMAT) for p in payloads]
        self.assertEqual(actual, expected)
        self.assertEqual(tree(batch.root), tree(plain.root))
        self.assertEqual(len(tree(batch.root)), 40)
        self.assertQuota(plain, 40 * SIZE); self.assertQuota(batch, 40 * SIZE)
        self.assertEqual(state(batch.root), state(plain.root))    # whole final quota state, limits included
        self.assertEqual(state(batch.root)["min_free_bytes"], 0)  # a disabled free-space reserve stays disabled
        self.assertEqual(list((batch.root / "tmp").iterdir()), [])
        self.assertEqual(spool._PUBLICATION_BATCHES, {})

    def test_sink_entries_match(self):
        plain = self.context("plain"); batch = self.context("batch")
        a = spool.open_symbol_sink(plain, "EURUSD"); b = spool.open_symbol_sink(batch, "EURUSD")
        expected = [a.put_opaque(f"ns/{i}", "kind", (payload(i),), format=FORMAT) for i in range(12)]
        with spool.publication_batch(batch):
            actual = [b.put_opaque(f"ns/{i}", "kind", (payload(i),), format=FORMAT) for i in range(12)]
        self.assertEqual(actual, expected)
        self.assertEqual(b.artifact_roster(), a.artifact_roster())
        self.assertQuota(batch, 12 * SIZE)

    def _assert_reused_mutable_chunk_parity(self, *, memory_view, overflow):
        limit = 64
        chunk_size = 32 if overflow else 16
        expected = (b"A" * chunk_size + b"B" * chunk_size
                    + b"C" * chunk_size + b"D" * chunk_size)

        def chunks():
            reusable = bytearray(chunk_size)
            exported = memoryview(reusable) if memory_view else reusable
            for value in b"ABCD":
                reusable[:] = bytes([value]) * chunk_size
                yield exported
            reusable[:] = b"\xff" * chunk_size

        self.assertEqual(len(expected) > limit, overflow)
        plain = self.context("mutable_plain", stream_chunk_bytes=limit)
        batch = self.context("mutable_batch", stream_chunk_bytes=limit)
        ordinary = spool.write_opaque_artifact(plain, chunks(), format=FORMAT)
        with spool.publication_batch(batch):
            buffered = spool.write_opaque_artifact(batch, chunks(), format=FORMAT)
        self.assertEqual(buffered, ordinary)
        self.assertEqual(ordinary["sha256"], hashlib.sha256(expected).hexdigest())
        self.assertEqual(ordinary["size_bytes"], len(expected))
        self.assertEqual((plain.root / ordinary["relative_path"]).read_bytes(), expected)
        self.assertEqual((batch.root / buffered["relative_path"]).read_bytes(), expected)
        self.assertEqual(tree(batch.root), tree(plain.root))
        self.assertQuota(plain, len(expected))
        self.assertQuota(batch, len(expected))
        self.assertEqual(list((batch.root / "tmp").iterdir()), [])
        self.assertEqual(spool._PUBLICATION_BATCHES, {})

    def test_reused_bytearray_matches_ordinary_at_small_buffer_limit(self):
        self._assert_reused_mutable_chunk_parity(memory_view=False, overflow=False)

    def test_reused_memoryview_matches_ordinary_at_small_buffer_limit(self):
        self._assert_reused_mutable_chunk_parity(memory_view=True, overflow=False)

    def test_reused_bytearray_matches_ordinary_after_buffer_overflow(self):
        self._assert_reused_mutable_chunk_parity(memory_view=False, overflow=True)

    def test_reused_memoryview_matches_ordinary_after_buffer_overflow(self):
        self._assert_reused_mutable_chunk_parity(memory_view=True, overflow=True)

    def _assert_oversized_mutable_chunk_has_bounded_extra_publication_peak(self, *, memory_view):
        import tracemalloc

        limit = 64 * 1024
        backing = bytearray(b"B") * (2 * 1024 * 1024)
        oversized = memoryview(backing) if memory_view else backing
        prefix, suffix = b"A" * 17, b"Z" * 17
        expected = prefix + bytes(backing) + suffix
        ordinary_chunks = iter((prefix, oversized, suffix))
        batch_chunks = iter((prefix, oversized, suffix))
        plain = self.context("oversized_plain", stream_chunk_bytes=limit)
        batch = self.context("oversized_batch", stream_chunk_bytes=limit)

        def publish_and_peak(context, chunks, *, batched):
            owned_tracing = not tracemalloc.is_tracing()
            if owned_tracing:
                tracemalloc.start()
            before = tracemalloc.get_traced_memory()[0]
            tracemalloc.reset_peak()
            try:
                if batched:
                    with spool.publication_batch(context):
                        descriptor = spool.write_opaque_artifact(context, chunks, format=FORMAT)
                else:
                    descriptor = spool.write_opaque_artifact(context, chunks, format=FORMAT)
                peak = tracemalloc.get_traced_memory()[1] - before
            finally:
                if owned_tracing:
                    tracemalloc.stop()
            return descriptor, peak

        ordinary, ordinary_peak = publish_and_peak(plain, ordinary_chunks, batched=False)
        buffered, batch_peak = publish_and_peak(batch, batch_chunks, batched=True)
        self.assertEqual(buffered, ordinary)
        self.assertEqual(ordinary["sha256"], hashlib.sha256(expected).hexdigest())
        self.assertEqual(ordinary["size_bytes"], len(expected))
        self.assertEqual((plain.root / ordinary["relative_path"]).read_bytes(), expected)
        self.assertEqual((batch.root / buffered["relative_path"]).read_bytes(), expected)
        self.assertQuota(plain, len(expected))
        self.assertQuota(batch, len(expected))
        self.assertLessEqual(batch_peak - ordinary_peak, 4 * limit)

    def test_oversized_bytearray_streams_with_bounded_extra_publication_peak(self):
        self._assert_oversized_mutable_chunk_has_bounded_extra_publication_peak(memory_view=False)

    def test_oversized_memoryview_streams_with_bounded_extra_publication_peak(self):
        self._assert_oversized_mutable_chunk_has_bounded_extra_publication_peak(memory_view=True)

    def test_existing_and_repeated_objects_write_nothing_new(self):
        ctx = self.context("dedup")
        first = spool.write_opaque_artifact(ctx, [payload(1)], format=FORMAT)
        with spool.publication_batch(ctx):
            again = spool.write_opaque_artifact(ctx, [payload(1)], format=FORMAT)
            new = spool.write_opaque_artifact(ctx, [payload(2)], format=FORMAT)
            repeat = spool.write_opaque_artifact(ctx, [payload(2)], format=FORMAT)
        self.assertEqual((again, repeat), (first, new))
        self.assertQuota(ctx, 2 * SIZE)
        self.assertEqual(len(tree(ctx.root)), 2)

    def test_large_object_inside_a_batch_uses_the_ordinary_path(self):
        big = [b"a" * 700_000, b"b" * 700_000]
        plain = self.context("plain"); batch = self.context("batch")
        expected = spool.write_opaque_artifact(plain, big, format=FORMAT)
        with spool.publication_batch(batch):
            spool.write_opaque_artifact(batch, [payload(7)], format=FORMAT)
            actual = spool.write_opaque_artifact(batch, big, format=FORMAT)
        self.assertEqual(actual, expected)
        self.assertQuota(batch, 1_400_000 + SIZE)

    def test_exception_inside_a_batch_commits_what_was_published(self):
        ctx = self.context("boom")
        with self.assertRaisesRegex(RuntimeError, "boom"):
            with spool.publication_batch(ctx):
                spool.write_opaque_artifact(ctx, [payload(1)], format=FORMAT)
                spool.write_opaque_artifact(ctx, [payload(2)], format=FORMAT)
                raise RuntimeError("boom")
        self.assertQuota(ctx, 2 * SIZE)
        self.assertEqual(len(tree(ctx.root)), 2)
        self.assertEqual(spool._PUBLICATION_BATCHES, {})

    def test_generation_cap_refuses_inside_a_batch_and_keeps_earlier_objects(self):
        ctx = self.capped("cap", 3, slack=100)
        with self.assertRaises(spool.AuthoritySpoolError) as caught:
            with spool.publication_batch(ctx):
                for i in range(6):
                    spool.write_opaque_artifact(ctx, [payload(i)], format=FORMAT)
        self.assertIn("SPOOL_QUOTA_GENERATION_BYTES_EXCEEDED", str(caught.exception))
        self.assertIsInstance(caught.exception.__cause__, quota.AuthoritySpoolQuotaError)
        self.assertFalse(spool.is_resource_capacity_error(caught.exception))
        self.assertQuota(ctx, 3 * SIZE)
        self.assertEqual(len(tree(ctx.root)), 3)

    def test_small_remaining_quota_is_used_exactly(self):
        ctx = self.capped("exact", 3)
        with spool.publication_batch(ctx):
            for i in range(3):
                spool.write_opaque_artifact(ctx, [payload(i)], format=FORMAT)
        self.assertQuota(ctx, 3 * SIZE)
        self.assertEqual(state(ctx.root)["committed_bytes"], ctx.limits.max_generation_bytes)

    def test_free_space_reserve_breach_is_a_capacity_error(self):
        free = 10**9
        self.fixed_disk_free(free)
        ctx = self.context("floor", min_free_bytes=free - 2_000)   # the context file fits, one payload does not
        with self.assertRaises(spool.AuthoritySpoolError) as caught:
            with spool.publication_batch(ctx):
                spool.write_opaque_artifact(ctx, [payload(1)], format=FORMAT)
        self.assertIn("SPOOL_QUOTA_FREE_SPACE_RESERVE_BREACHED", str(caught.exception))
        self.assertTrue(spool.is_resource_capacity_error(caught.exception))
        self.assertEqual(len(tree(ctx.root)), 0)
        self.assertQuota(ctx, 0)

    def test_free_space_headroom_smaller_than_the_window_is_still_usable(self):
        free = 10**9
        self.fixed_disk_free(free)
        headroom = 100_000
        self.assertLess(headroom, spool._BATCH_RESERVATION_WINDOW_BYTES)
        ctx = self.context("headroom", min_free_bytes=free - headroom)
        with spool.publication_batch(ctx):
            for i in range(5):
                spool.write_opaque_artifact(ctx, [payload(i)], format=FORMAT)
        self.assertQuota(ctx, 5 * SIZE)

    def test_abandoned_batch_is_recovered_by_the_existing_rescan(self):
        ctx = self.context("dead")
        stack = ExitStack(); stack.enter_context(spool.publication_batch(ctx))
        for i in range(5):
            spool.write_opaque_artifact(ctx, [payload(i)], format=FORMAT)
        key = spool._publication_batch_key(ctx)
        scope = spool._PUBLICATION_BATCHES.pop(key)
        scope.account._lease._guard.close(remove=False)          # the process is gone: record stays, guard is dead
        self.assertEqual(len(state(ctx.root)["reservations"]), 1)
        spool.write_opaque_artifact(ctx, [payload(99)], format=FORMAT)   # an ordinary publication recovers the root
        self.assertQuota(ctx, 6 * SIZE)
        with self.assertRaises(spool.AuthoritySpoolError):       # the dead batch cannot commit a second time
            stack.close()
        self.assertQuota(ctx, 6 * SIZE)
        self.assertEqual(spool._PUBLICATION_BATCHES, {})

    def test_nested_scope_joins_the_outer_batch(self):
        ctx = self.context("nested")
        with spool.publication_batch(ctx):
            with spool.publication_batch(ctx):
                spool.write_opaque_artifact(ctx, [payload(1)], format=FORMAT)
            self.assertEqual(len(state(ctx.root)["reservations"]), 1)
            spool.write_opaque_artifact(ctx, [payload(2)], format=FORMAT)
        self.assertQuota(ctx, 2 * SIZE)

    def test_input_refusals_are_unchanged(self):
        ctx = self.context("bad")
        with spool.publication_batch(ctx):
            with self.assertRaisesRegex(spool.AuthoritySpoolError, "ARTIFACT_EMPTY"):
                spool.write_opaque_artifact(ctx, [], format=FORMAT)
            with self.assertRaisesRegex(spool.AuthoritySpoolError, "ARTIFACT_CHUNK_TYPE_INVALID"):
                spool.write_opaque_artifact(ctx, ["text"], format=FORMAT)
        self.assertQuota(ctx, 0)
        small = self.context("small", max_artifact_bytes=100)
        with spool.publication_batch(small):
            with self.assertRaisesRegex(spool.AuthoritySpoolError, "ARTIFACT_SIZE_BOUND_EXCEEDED"):
                spool.write_opaque_artifact(small, [payload(1)], format=FORMAT)
        self.assertQuota(small, 0)

    def test_content_address_conflict_is_refused(self):
        ctx = self.context("conflict")
        d = spool.write_opaque_artifact(ctx, [payload(1)], format=FORMAT)
        target = ctx.root / d["relative_path"]; os.chmod(target, 0o600); target.write_bytes(b"tampered")
        with spool.publication_batch(ctx):
            with self.assertRaisesRegex(spool.AuthoritySpoolError, "CONTENT_ADDRESS_CONFLICT"):
                spool.write_opaque_artifact(ctx, [payload(1)], format=FORMAT)

    def test_two_threads_each_hold_their_own_batch(self):
        ctx = self.context("threads"); errors = []

        def work(offset):
            try:
                with spool.publication_batch(ctx):
                    for i in range(25):
                        spool.write_opaque_artifact(ctx, [payload(offset + i)], format=FORMAT)
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)

        threads = [threading.Thread(target=work, args=(k * 100,)) for k in range(2)]
        for t in threads: t.start()
        for t in threads: t.join()
        self.assertEqual(errors, [])
        self.assertQuota(ctx, 50 * SIZE)
        self.assertEqual(len(tree(ctx.root)), 50)

    def test_unused_reservation_never_exceeds_the_batch_window(self):
        ctx = self.context("window")
        self.assertLess(spool._BATCH_RESERVATION_WINDOW_BYTES, spool._RESERVATION_WINDOW_BYTES)
        with spool.publication_batch(ctx):
            for i in range(3):
                spool.write_opaque_artifact(ctx, [payload(i)], format=FORMAT)
            record = state(ctx.root)["reservations"][0]
            self.assertEqual(record["reserved_bytes"], spool._BATCH_RESERVATION_WINDOW_BYTES)
        self.assertQuota(ctx, 3 * SIZE)

    def test_ordinary_publication_from_another_thread_while_a_batch_is_open(self):
        ctx = self.context("mixed"); out = []
        with spool.publication_batch(ctx):
            spool.write_opaque_artifact(ctx, [payload(1)], format=FORMAT)
            other = threading.Thread(target=lambda: out.append(spool.write_opaque_artifact(ctx, [payload(2)], format=FORMAT)))
            other.start(); other.join()
            self.assertEqual(len(state(ctx.root)["reservations"]), 1)
        self.assertEqual(len(out), 1)
        self.assertQuota(ctx, 2 * SIZE)

    # Regressions for Root E5725.

    def test_context_drift_refuses_the_next_object_before_any_write(self):
        ctx = self.context("drift")
        path = ctx.root / spool.CONTEXT_RELATIVE_PATH
        original = path.read_bytes()
        self.assertEqual(original.count(b'"cut1"'), 1)
        with spool.publication_batch(ctx):
            spool.write_opaque_artifact(ctx, [payload(1)], format=FORMAT)
            os.chmod(path, 0o600); path.write_bytes(original.replace(b'"cut1"', b'"cut2"'))   # same length, different content
            with self.assertRaises(spool.AuthoritySpoolError) as caught:
                spool.write_opaque_artifact(ctx, [payload(2)], format=FORMAT)
            self.assertNotIn("SPOOL_QUOTA", str(caught.exception))
            self.assertEqual(len(tree(ctx.root)), 1)
            self.assertEqual(list((ctx.root / "tmp").iterdir()), [])
            self.assertEqual(state(ctx.root)["reservations"][0]["materialized_bytes"], 0)   # nothing reported for object 2
            path.write_bytes(original)
            spool.write_opaque_artifact(ctx, [payload(3)], format=FORMAT)
        self.assertQuota(ctx, 2 * SIZE)
        self.assertEqual(len(tree(ctx.root)), 2)

    def test_directory_replaced_by_a_link_is_refused_before_any_write(self):
        ctx = self.context("link")
        outside = self.tmp / "outside"; outside.mkdir()
        tmp_dir = ctx.root / "tmp"

        def link(target, where):
            if os.name == "nt":
                import _winapi
                _winapi.CreateJunction(str(target), str(where))
            else:
                os.symlink(target, where, target_is_directory=True)

        def unlink(where):
            os.rmdir(where) if os.name == "nt" else os.unlink(where)

        with spool.publication_batch(ctx):
            first = spool.write_opaque_artifact(ctx, [payload(1)], format=FORMAT)
            os.rmdir(tmp_dir); link(outside, tmp_dir)                       # the temporary directory becomes a link
            with self.assertRaisesRegex(spool.AuthoritySpoolError, "REPARSE|NOT_SAFE"):
                spool.write_opaque_artifact(ctx, [payload(2)], format=FORMAT)
            self.assertEqual(list(outside.iterdir()), [])                    # nothing was written through the link
            unlink(tmp_dir); tmp_dir.mkdir()
            parent = (ctx.root / first["relative_path"]).parent              # an object directory becomes a link
            fresh = next(i for i in range(10, 5000) if (ctx.root / spool._content_relative_path(
                hashlib.sha256(payload(i)).hexdigest(), FORMAT)).parent == parent)
            moved = self.tmp / "moved"; os.rename(parent, moved); link(moved, parent)
            before = sorted(p.name for p in moved.iterdir())
            with self.assertRaisesRegex(spool.AuthoritySpoolError, "REPARSE|NOT_SAFE"):
                spool.write_opaque_artifact(ctx, [payload(fresh)], format=FORMAT)
            self.assertEqual(sorted(p.name for p in moved.iterdir()), before)
            self.assertEqual(list(tmp_dir.iterdir()), [])
            unlink(parent); os.rename(moved, parent)
            spool.write_opaque_artifact(ctx, [payload(3)], format=FORMAT)
        self.assertQuota(ctx, 2 * SIZE)
        self.assertEqual(len(tree(ctx.root)), 2)

    def test_declining_free_space_does_not_double_count_published_bytes(self):
        ctx = self.context("declining")
        free_at_start = 100_000

        def declining(root):
            objects = Path(root) / "objects"
            return free_at_start - sum(p.stat().st_size for p in objects.rglob("*") if p.is_file())

        original = quota._disk_free
        quota._disk_free = declining
        self.addCleanup(setattr, quota, "_disk_free", original)
        fits = free_at_start // SIZE
        self.assertEqual(fits, 21)
        with self.assertRaises(spool.AuthoritySpoolError) as caught:
            with spool.publication_batch(ctx):
                for i in range(fits + 1):
                    spool.write_opaque_artifact(ctx, [payload(i)], format=FORMAT)
        self.assertTrue(spool.is_resource_capacity_error(caught.exception))
        self.assertEqual(len(tree(ctx.root)), fits)                          # every object that fits is published
        self.assertQuota(ctx, fits * SIZE)

    def test_failure_after_the_link_leaves_the_record_for_the_rescan(self):
        ctx = self.context("afterlink")
        real = spool._publish_temp_exclusive

        def link_then_fail(root, temp, relative_path):
            real(root, temp, relative_path)
            raise OSError("directory sync failed")

        spool._publish_temp_exclusive = link_then_fail
        self.addCleanup(setattr, spool, "_publish_temp_exclusive", real)
        with self.assertRaisesRegex(OSError, "directory sync failed"):
            with spool.publication_batch(ctx):
                spool.write_opaque_artifact(ctx, [payload(1)], format=FORMAT)
        spool._publish_temp_exclusive = real
        self.assertEqual(len(tree(ctx.root)), 1)                             # the object is linked
        self.assertEqual(len(state(ctx.root)["reservations"]), 1)            # and its record was not committed away
        self.assertEqual(list((ctx.root / "tmp").iterdir()), [])
        spool.write_opaque_artifact(ctx, [payload(2)], format=FORMAT)        # the next publication recovers by rescan
        self.assertQuota(ctx, 2 * SIZE)

    # Regression for Root E5737.

    def test_caller_that_catches_and_continues_is_refused_after_the_batch_is_poisoned(self):
        ctx = self.context("continued")
        real = spool._publish_temp_exclusive

        def link_then_fail(root, temp, relative_path):
            real(root, temp, relative_path)
            raise OSError("directory sync failed")

        self.addCleanup(setattr, spool, "_publish_temp_exclusive", real)
        with spool.publication_batch(ctx):
            spool.write_opaque_artifact(ctx, [payload(0)], format=FORMAT)
            spool._publish_temp_exclusive = link_then_fail
            with self.assertRaisesRegex(OSError, "directory sync failed"):
                spool.write_opaque_artifact(ctx, [payload(1)], format=FORMAT)    # linked, then the failure
            spool._publish_temp_exclusive = real
            self.assertEqual(len(tree(ctx.root)), 2)
            with self.assertRaisesRegex(spool.AuthoritySpoolError, "PUBLICATION_BATCH_POISONED"):
                spool.write_opaque_artifact(ctx, [payload(2)], format=FORMAT)    # a new object is refused
            with self.assertRaisesRegex(spool.AuthoritySpoolError, "PUBLICATION_BATCH_POISONED"):
                spool.write_opaque_artifact(ctx, [payload(0)], format=FORMAT)    # so is a repeat of a published one
            scope = spool._PUBLICATION_BATCHES[spool._publication_batch_key(ctx)]
            with self.assertRaisesRegex(quota.AuthoritySpoolQuotaError, "SPOOL_QUOTA_BATCH_POISONED"):
                scope.account.reserve(1)                                         # the quota account itself refuses
            with self.assertRaisesRegex(quota.AuthoritySpoolQuotaError, "SPOOL_QUOTA_BATCH_POISONED"):
                scope.account.published(1)
            self.assertEqual(len(tree(ctx.root)), 2)                             # nothing new reached the disk
            self.assertEqual(list((ctx.root / "tmp").iterdir()), [])
            record = state(ctx.root)["reservations"][0]
            self.assertGreaterEqual(record["reserved_bytes"], 2 * SIZE)          # both linked objects stay covered
            big = spool.write_opaque_artifact(ctx, [b"a" * 700_000, b"b" * 700_000], format=FORMAT)   # the ordinary path still works
            self.assertEqual(big["size_bytes"], 1_400_000)
        self.assertEqual(len(state(ctx.root)["reservations"]), 1)                # the poisoned record was not committed
        spool.write_opaque_artifact(ctx, [payload(3)], format=FORMAT)            # the next publication recovers by rescan
        self.assertQuota(ctx, 3 * SIZE + 1_400_000)
        self.assertEqual(len(tree(ctx.root)), 4)

    def test_outside_a_batch_nothing_changes(self):
        ctx = self.context("outside")
        d = spool.write_opaque_artifact(ctx, [payload(1)], format=FORMAT)
        self.assertEqual(d["sha256"], hashlib.sha256(payload(1)).hexdigest())
        self.assertEqual(spool._PUBLICATION_BATCHES, {})
        self.assertQuota(ctx, SIZE)
