"""Import-clean workers for the spawned-child tests of the W5.1 stage monitor.

A spawned child unpickles its worker by importing this module, so this module must not import pm_stage_monitor: the
probe below proves the monitor route stays absent in a child when the environment variable is unset.  The allowlist
extension runs only when the test process asked for it through FXPM_STAGE_MONITOR_TEST_ALLOW, and only adds these three
workers by (module, attribute) reference, which the governor resolves to this module's own objects.
"""

import os
import sys

import pm_memory_governor as governor


def leaf(n):
    return n * 2


def outer(n):
    return leaf(n) + leaf(n)


def spawned_probe():
    """What the child sees: whether the monitor module is loaded and whether the route variable is set."""
    return {"monitor_loaded": "pm_stage_monitor" in sys.modules,
            "route_variable": os.environ.get("FXPM_STAGE_TIMERS_DIR") or ""}


def spawned_ok(n):
    return outer(n)


def spawned_boom():
    raise ValueError("spawned boom")


if os.environ.get("FXPM_STAGE_MONITOR_TEST_ALLOW") == "1":
    governor._STAGE_MONITOR_WORKER_REFS = governor._STAGE_MONITOR_WORKER_REFS + (
        (__name__, "spawned_ok"), (__name__, "spawned_boom"), (__name__, "spawned_probe"))
