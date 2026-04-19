import asyncio
import re
import sys
from datetime import datetime
from pathlib import Path

import polars as pl

version = "1.00"

# ============================================================
# Configuration
# ============================================================

LOG_FILE = "cu-lan-ho-live.log"

LEVEL_LABEL = {"I": "INFO ", "D": "DEBUG", "W": "WARN ", "E": "ERROR"}

# Pattern for a log line header:
#   TIMESTAMP [COMPONENT] [LEVEL] MESSAGE
#   2026-01-19T08:55:05.524757 [CU      ] [I] Built in Release mode...
LOG_PATTERN = re.compile(
    r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})'  # 1: timestamp
    r'\s+\[([A-Z0-9_-]+)\s*\]'                          # 2: component
    r'\s+\[([IDWE])\]'                                  # 3: level
    r'\s+(.*)'                                           # 4: message
)

# ============================================================
# Shared asyncio queue
# ============================================================

queue: asyncio.Queue = asyncio.Queue()

# ============================================================
# Shared DataFrame
# ============================================================

log_df = pl.DataFrame(schema={
    "timestamp": pl.Datetime(time_unit="us"),
    "component": pl.String,
    "level":     pl.String,
    "message":   pl.String,
})

# Obj. 4: aggregated bytes per (1s window, UE)
ue_volume_df = pl.DataFrame(schema={
    "window": pl.Datetime(time_unit="us"),
    "ue":     pl.Int64,
    "bytes":  pl.Int64,
})

# ============================================================
# Producer: tail log file → queue
# ============================================================

async def tail_file_producer(path: str, data_queue: asyncio.Queue) -> None:
    """
    Waits for the log file to exist, then reads new lines as they are
    appended (tail -f behaviour). Each raw line is put into the queue.
    Handles file rotation: if the file shrinks (log-sim.py rewrites it)
    the reader seeks back to the beginning automatically.
    """
    file = Path(path)

    print(f"Waiting for log file: {path}")
    while not file.exists():
        await asyncio.sleep(0.5)

    with open(path, "r", encoding="utf-8") as f:

        f.seek(0, 2)  # start at end of file — only read new lines

        print(f"Opened: {path}")

        while True:
            line = f.readline()

            if line:
                await data_queue.put(line)
            else:
                await asyncio.sleep(0.1)

# ============================================================
# Consumer: queue → parse → DataFrame
# ============================================================

async def consumer(data_queue: asyncio.Queue) -> None:
    """
    Reads raw lines from the queue, parses those that match the srsRAN
    log header pattern, and appends them to the shared Polars DataFrame.
    """
    global log_df

    FLUSH_EVERY = 200
    log_buf = []
    lc = 0  # total lines received

    while True:
        line = await data_queue.get()
        lc += 1

        m = LOG_PATTERN.match(line)
        if m:
            ts_str, component, level, message = m.groups()
            ts = datetime.fromisoformat(ts_str)

            log_buf.append({
                "timestamp": ts,
                "component": component,
                "level":     level,
                "message":   message.strip(),
            })

            label = LEVEL_LABEL.get(level, level)
            if ("[SDAP" in f"[{component:<8}]"):
                print(f"{ts_str}  [{component:<8}]  [{label}]  {message.strip()}")

        if lc % FLUSH_EVERY == 0:
            if log_buf:
                log_df = pl.concat([log_df, pl.DataFrame(log_buf, schema=log_df.schema)], how="vertical")
                log_buf = []
            await asyncio.sleep(0)  # yield to event loop

        data_queue.task_done()

# ============================================================
# Obj. 4: Aggregator — every 1s, sum bytes per UE
# ============================================================

async def aggregator() -> None:
    global ue_volume_df

    last_processed = 0

    while True:
        await asyncio.sleep(1)

        new_rows = log_df[last_processed:]
        last_processed = len(log_df)

        agg_rows = (
            new_rows
            .with_columns([
                pl.col("message").str.extract(r'ue=(\d+)', 1).cast(pl.Int64).alias("ue"),
                pl.col("message").str.extract(r'pdu_len=(\d+)', 1).cast(pl.Int64).alias("pdu_len"),
            ])
            .drop_nulls(["ue", "pdu_len"])
        )

        if len(agg_rows) == 0:
            continue

        agg = (
            agg_rows
            .with_columns(
                (pl.col("timestamp").cast(pl.Int64) // 1_000_000 * 1_000_000)
                .cast(pl.Datetime(time_unit="us"))
                .alias("window")
            )
            .group_by(["window", "ue"])
            .agg(pl.col("pdu_len").sum().alias("bytes"))
            .sort(["window", "ue"])
        )

        ue_volume_df = pl.concat([ue_volume_df, agg], how="vertical")

        for row in agg.iter_rows(named=True):
            print(f"  ◆ Agg  window={row['window']}  ue={row['ue']}  bytes={row['bytes']:,}")

# ============================================================
# Main
# ============================================================

async def main() -> None:
    print("─" * 60)
    print(f"srsRAN CU Log Reader  v{version}")
    print("─" * 60)
    print(f"Log file : {LOG_FILE}")
    print(f"Platform : {sys.platform}")
    print()

    tasks = [
        asyncio.create_task(tail_file_producer(LOG_FILE, queue)),
        asyncio.create_task(consumer(queue)),
        asyncio.create_task(aggregator()),
    ]

    try:
        while True:
            await asyncio.sleep(1)
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        for t in tasks:
            t.cancel()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n... script gracefully stopped")
        print(f"\nLog rows captured : {len(log_df)}")
        print(f"Agg rows (1s/UE)  : {len(ue_volume_df)}")
        if len(ue_volume_df) > 0:
            print(f"Total bytes       : {ue_volume_df['bytes'].sum():,}")
            print(ue_volume_df)
