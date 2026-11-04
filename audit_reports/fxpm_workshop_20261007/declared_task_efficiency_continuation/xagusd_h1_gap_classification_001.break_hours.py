"""Where do the residual 7-12 slot runs start, by UTC minute of day and by month? (daily break, DST shift)"""
import numpy as np
import pandas as pd

PATH = r"C:\Users\Bongo\OneDrive\Desktop\FxPM-Dev\data\native\runs\production_native_v5r2_20260714\XAGUSD\M5\bars.csv"
START = pd.Timestamp("2016-05-12T08:00:00Z")
END = pd.Timestamp("2024-07-14T08:00:00Z")
stamps = pd.to_datetime(pd.read_csv(PATH, usecols=["time"])["time"], utc=True)
stamps = stamps[(stamps >= START) & (stamps < END)]
present = np.sort(stamps.values.astype("datetime64[m]").astype(np.int64))
grid = np.arange(int(START.value // 60_000_000_000), int(END.value // 60_000_000_000), 5, dtype=np.int64)
missing = grid[~np.isin(grid, present)]
sow = (missing + 3 * 1440) % (7 * 1440)
weekend = (sow >= 5 * 1440)
weekday_missing = missing[~weekend]
breaks = np.where(np.diff(weekday_missing) != 5)[0]
starts = np.concatenate(([0], breaks + 1))
ends = np.concatenate((breaks, [len(weekday_missing) - 1]))
runs = [(weekday_missing[s], weekday_missing[e]) for s, e in zip(starts, ends)]
lengths = np.array([(e - s) // 5 + 1 for s, e in runs])
sel = [(s, e) for (s, e), n in zip(runs, lengths) if 7 <= n <= 13]
ts = pd.to_datetime(np.array([s for s, _ in sel]) * 60, unit="s", utc=True)
df = pd.DataFrame({"start": ts, "len": [(e - s) // 5 + 1 for s, e in sel]})
df["hhmm"] = df["start"].dt.strftime("%H:%M")
df["month"] = df["start"].dt.month
print("runs of 7-13 slots:", len(df))
print("start times (top):")
print(df["hhmm"].value_counts().head(8).to_string())
print("start time by month (mode):")
print(df.groupby("month")["hhmm"].agg(lambda s: s.value_counts().index[0]).to_string())
print("lengths:", df["len"].value_counts().sort_index().to_dict())
# how many trading days exist per year (weekdays with any bar)
days = pd.Series(pd.to_datetime(present * 60, unit="s", utc=True).normalize().unique())
print("weekdays with bars:", int((days.dt.weekday < 5).sum()), "runs/year:", round(len(df) / 8.17, 1))
