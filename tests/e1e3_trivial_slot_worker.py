"""A labelled stand-in slot worker for dispatcher-order tests: no product import, no slot work, just a result name."""
import time


def trivial_slot_worker(spec):
    time.sleep(float(spec.get("seconds", 0.3)))
    return f"{int(spec['ordinal']):064x}"
