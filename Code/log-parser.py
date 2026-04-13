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

LOG_FILE = "cu-lan-ho.log"

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

        print(f"Opened: {path}")
        f.seek(0, 2)  # Start at end — only process new lines
        pos = f.tell()

        while True:
            line = f.readline()

            if not line:
                await asyncio.sleep(0.1)

                # Detect file rotation / rewrite (log-sim.py opens with 'w')
                current_size = file.stat().st_size
                if current_size < pos:
                    f.seek(0)
                    pos = 0

                continue

            pos = f.tell()
            await data_queue.put(line)

# ============================================================
# Consumer: queue → parse → DataFrame
# ============================================================

async def consumer(data_queue: asyncio.Queue) -> None:
    """
    Reads raw lines from the queue, parses those that match the srsRAN
    log header pattern, and appends them to the shared Polars DataFrame.
    """
    global log_df

    lc = 0  # total lines received
    pc = 0  # parsed (matched) lines

    while True:
        line = await data_queue.get()
        lc += 1

        m = LOG_PATTERN.match(line)
        if m:
            pc += 1
            ts_str, component, level, message = m.groups()
            ts = datetime.fromisoformat(ts_str)

            new_row = pl.DataFrame([{
                "timestamp": ts,
                "component": component,
                "level":     level,
                "message":   message.strip(),
            }])
            log_df = pl.concat([log_df, new_row], how="vertical")

            label = LEVEL_LABEL.get(level, level)
            print(f"{ts_str}  [{component:<8}]  [{label}]  {message.strip()}")

        data_queue.task_done()

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

    asyncio.create_task(tail_file_producer(LOG_FILE, queue))
    asyncio.create_task(consumer(queue))

    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n... script gracefully stopped")
        print(f"\nRows captured : {len(log_df)}")
        if len(log_df) > 0:
            print(log_df)
