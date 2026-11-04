"""Classify every missing M5 slot of XAGUSD in the pre-cutoff interval by slot-of-week regularity.

Read-only diagnostic on the published native M5 file. A slot-of-week (weekday, hour, minute) that is missing in at
least 95 percent of the weeks it could occur in is a calendar closure (weekend or daily break). A weekday whose
every slot is missing is a full-day closure (holiday). Everything else is irregular. Also checks the H1 target
rule: for each hour with at least one M5 bar, is the M5 bar that closes at the hour start present.
"""
import sys
import time

import numpy as np
import pandas as pd

PATH = r"C:\Users\Bongo\OneDrive\Desktop\FxPM-Dev\data\native\runs\production_native_v5r2_20260714\XAGUSD\M5\bars.csv"
START = pd.Timestamp("2016-05-12T08:00:00Z")
END = pd.Timestamp("2024-07-14T08:00:00Z")  # exclusive, the note's available interval
CUTOFF = pd.Timestamp("2024-07-14T08:05:00Z")

t0 = time.perf_counter()
times = pd.read_csv(PATH, usecols=["time"])["time"]
stamps = pd.to_datetime(times, utc=True)
stamps = stamps[(stamps >= START) & (stamps < END)]
present = np.sort(stamps.values.astype("datetime64[m]").astype(np.int64))
print(f"read and filter {time.perf_counter()-t0:.1f}s; bars in interval {len(present)}")

grid = np.arange(int(START.value // 60_000_000_000), int(END.value // 60_000_000_000), 5, dtype=np.int64)
is_present = np.isin(grid, present)
missing = grid[~is_present]
print(f"expected slots {len(grid)}, present {is_present.sum()}, missing {len(missing)}, "
      f"missing share {len(missing)/len(grid):.4f}")

# slot-of-week in minutes since Monday 00:00 UTC (1970-01-01 was a Thursday, weekday 3)
def slot_of_week(mins):
    return (mins + 3 * 1440) % (7 * 1440)

grid_sow = slot_of_week(grid)
miss_sow = slot_of_week(missing)
occ = pd.Series(grid_sow).value_counts()
mis = pd.Series(miss_sow).value_counts()
rate = (mis.reindex(occ.index).fillna(0) / occ)
closure_slots = set(rate[rate >= 0.95].index)
print(f"slot-of-week positions {len(occ)}, closure positions (missing >=95% of weeks) {len(closure_slots)}")

# describe the closure positions as weekday/time ranges
closure_sorted = sorted(closure_slots)
ranges = []
for s in closure_sorted:
    if ranges and s == ranges[-1][1] + 5:
        ranges[-1][1] = s
    else:
        ranges.append([s, s])
names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
for a, b in ranges:
    print(f"  closure {names[a//1440]} {a%1440//60:02d}:{a%60:02d} to {names[(b+5)//1440 % 7]} {(b+5)%1440//60:02d}:{(b+5)%60:02d} UTC "
          f"({(b-a)//5+1} slots)")

in_closure = np.isin(miss_sow, list(closure_slots))
missing_closure = int(in_closure.sum())
irregular = missing[~in_closure]
print(f"missing in closure positions {missing_closure}, irregular missing slots {len(irregular)}")

# full-day closures among irregular: days (UTC) with every non-closure slot missing
day_of = irregular // 1440
grid_open = grid[~np.isin(grid_sow, list(closure_slots))]
open_per_day = pd.Series(grid_open // 1440).value_counts()
miss_per_day = pd.Series(day_of).value_counts()
full_days = [d for d, n in miss_per_day.items() if n >= open_per_day.get(d, 10**9)]
full_day_slots = int(miss_per_day.reindex(full_days).sum()) if full_days else 0
print(f"full-day closures (holidays) {len(full_days)} days, {full_day_slots} slots")
for d in sorted(full_days)[:40]:
    print("  holiday", pd.Timestamp(int(d) * 1440, unit="m", tz="UTC").date())
residual = irregular[~np.isin(day_of, full_days)]
print(f"residual irregular slots {len(residual)}")

# contiguous runs among the residual
if len(residual):
    breaks = np.where(np.diff(residual) != 5)[0]
    starts = np.concatenate(([0], breaks + 1))
    ends = np.concatenate((breaks, [len(residual) - 1]))
    runs = [(residual[s], residual[e]) for s, e in zip(starts, ends)]
    lengths = np.array([(e - s) // 5 + 1 for s, e in runs])
    print(f"residual runs {len(runs)}; by length: 1 slot {int((lengths==1).sum())}, 2-6 {int(((lengths>=2)&(lengths<=6)).sum())}, "
          f"7-12 {int(((lengths>=7)&(lengths<=12)).sum())}, 13-288 {int(((lengths>=13)&(lengths<=288)).sum())}, >288 {int((lengths>288).sum())}")
    longest = sorted(runs, key=lambda r: r[1] - r[0], reverse=True)[:12]
    for s, e in longest:
        print(f"  run {pd.Timestamp(int(s)*60, unit='s', tz='UTC')} to {pd.Timestamp(int(e)*60+300, unit='s', tz='UTC')} ({(e-s)//5+1} slots)")
    by_year = pd.Series(pd.to_datetime(residual * 60, unit="s", utc=True).year).value_counts().sort_index()
    print("residual slots by year:", dict(by_year))

# H1 target rule
hours = grid[grid % 60 == 0]
present_set = set(present.tolist())
has_bar = np.array([any((h + k) in present_set for k in range(0, 60, 5)) for h in hours])
targets = hours[has_bar]
prior_missing = np.array([(h - 5) not in present_set for h in targets])
prior_sow = slot_of_week(targets[prior_missing] - 5)
prior_in_closure = np.isin(prior_sow, list(closure_slots))
print(f"H1 targets with any M5 bar {len(targets)}; exact-prior M5 close missing {int(prior_missing.sum())}; "
      f"of those at a closure position {int(prior_in_closure.sum())}, elsewhere {int((~prior_in_closure).sum())}")
print(f"total {time.perf_counter()-t0:.1f}s")
