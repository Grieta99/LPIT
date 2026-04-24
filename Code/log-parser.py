import asyncio
import re
import sys
from datetime import datetime
from pathlib import Path

import math

import polars as pl
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go

version = "1.01"

# ============================================================
# Configuration
# ============================================================

LOG_FILE = "cu-lan-ho-live.log"
DASH_PORT = 8062

LEVEL_LABEL = {"I": "INFO ", "D": "DEBUG", "W": "WARN ", "E": "ERROR"}

# Pattern for a log line header:
#   TIMESTAMP [COMPONENT] [LEVEL] MESSAGE
#   2026-01-19T08:55:05.524757 [CU      ] [I] Built in Release mode...

LOG_PATTERN = re.compile(#mascara para extraccion de datos lives — solo DL SDAP
    r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+).*?\[SDAP\s*\].*?ue=(\d+).*?DL: TX PDU.*?pdu_len=(\d+)"
)

# ^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})\s+\[([SDAP\s*]+)\s*\]\s+\[([IDWE])\]\s+(.*)

# ============================================================
# Shared asyncio queue
# ============================================================

queue: asyncio.Queue = asyncio.Queue()

# ============================================================
# Shared DataFrame
# ============================================================

log_df = pl.DataFrame(schema={
    "timestamp": pl.Datetime(time_unit="us"), ##instante del paq
    "ue": pl.Int32,
    "pdu_len":     pl.Int32,  # DL only
})

# Obj. 4: aggregated bytes per (1s window, UE)
ue_volume_df = pl.DataFrame(schema={
    "window": pl.Datetime(time_unit="us"),
    "ue":     pl.Int32,
    "bytes":  pl.Int32,
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
    global ue_volume_df
    
    #print("consumer_1")

    FLUSH_EVERY = 200
    Tant        = datetime(2000, 4, 24, 12, 0, 0)
    Tant_pq     = None   # Obj6: set on first packet, then save every 30s log time
    last_processed = 0
    log_buf = []
    lc = 0  # total lines received

    while True:
        #print("consumer_2")
        line = await data_queue.get()
        lc += 1
  

        m = LOG_PATTERN.match(line)###modificar patron de line
        if m:
            ts_str, ue, pdu_len = m.groups()
            ts = datetime.fromisoformat(ts_str)

            log_buf.append({
                "timestamp": ts,
                "ue": int(ue),
                "pdu_len":     int(pdu_len),
            })
            #Obj2 SDAP DL
            print("AGG1", f"{ts_str}  [{ue}]  [{pdu_len}] ") #AGG Verifico los datos leidos
            
            #Obj4
            if (abs((ts - Tant).total_seconds()) >= 1): #AGG Compara el tiempo actual con el tiempo en el que se cargo el ultimo valor al dataframe de salida
                Tant = ts
                print("1S")
                # Flush buffer so log_df is up to date before aggregating
                if log_buf:
                    log_df = pl.concat([log_df, pl.DataFrame(log_buf, schema=log_df.schema)], how="vertical")
                    log_buf = []
                try:
                    new_rows = log_df[last_processed:]  # slice from last processed onward
                    last_processed = len(log_df)

                    if len(new_rows) > 0:
                        agg = (
                            new_rows
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
                except Exception as e:
                    print(f"Don't worry be happy (aggregator): {e}")

            #Obj6: cada 30s de log time guardar ue_volume_df en Parquet
            if Tant_pq is None:
                Tant_pq = ts  # anchor to first real log timestamp
            elif abs((ts - Tant_pq).total_seconds()) >= 30:
                Tant_pq = ts
                if len(ue_volume_df) > 0:
                    try:
                        fname = f"ue_volumes_{ts.strftime('%Y%m%dT%H%M%S')}.parquet"
                        ue_volume_df.write_parquet(fname)
                        print(f"  ▶ Parquet saved: {fname}  ({len(ue_volume_df)} rows)")
                    except Exception as e:
                        print(f"Parquet save error: {e}")

        if lc % FLUSH_EVERY == 0:
            if log_buf:
                log_df = pl.concat([log_df, pl.DataFrame(log_buf, schema=log_df.schema)], how="vertical")
                log_buf = []
            await asyncio.sleep(0.1)  # yield to event loop

        data_queue.task_done()


# ============================================================
# Obj. 5: Dash app — volume per UE, 5s circular window
# ============================================================

N_SLOTS    = 5  # number of 1s windows shown at once
UE_PALETTE = ["#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A", "#19D3F3", "#FF6692", "#B6E880"]

app = Dash(__name__)

# Layout: title, debug line, grouped bar chart, 1s refresh timer
app.layout = html.Div([
    html.H3("Volume per UE  —  5s window"),
    html.Div(id="debug", style={"fontFamily": "monospace", "fontSize": "13px", "marginBottom": "8px"}),
    dcc.Graph(id="vol-graph"),
    dcc.Interval(id="interval", interval=1000, n_intervals=0),
])

@app.callback(
    Output("vol-graph", "figure"),
    Output("debug", "children"),
    Input("interval", "n_intervals"),  # fires every 1s
)
def update_graph(_):
    fig = go.Figure()

    if len(ue_volume_df) > 0:
        # Keep only the N_SLOTS most recent 1s windows (sliding window)
        last_windows = sorted(ue_volume_df["window"].unique().to_list())[-N_SLOTS:]
        view = ue_volume_df.filter(pl.col("window").is_in(last_windows))

        # Only plot UEs that transferred at least 1 byte in the visible window
        active_ues = sorted(
            view.filter(pl.col("bytes") > 0)["ue"].unique().to_list()
        )

        x = [str(w)[11:19] for w in last_windows]  # HH:MM:SS labels

        for i, ue_id in enumerate(active_ues):
            ue_data = view.filter(pl.col("ue") == ue_id)
            # Align bytes to the window order; 0 if this UE had no traffic in that window
            y = [
                ue_data.filter(pl.col("window") == w)["bytes"].sum()
                for w in last_windows
            ]
            fig.add_trace(go.Bar(
                x=x,
                y=y,
                name=f"UE {ue_id}",
                marker_color=UE_PALETTE[i % len(UE_PALETTE)],
            ))

    # Switch to log scale when max/min ratio > 100x to avoid small UEs being invisible
    all_bytes = ue_volume_df["bytes"] if len(ue_volume_df) > 0 else pl.Series([0])
    b_max = all_bytes.max() or 1
    b_min = all_bytes.filter(all_bytes > 0).min() or 1
    use_log = b_max > 100 * b_min

    # Fix Y range based on all data seen so far to prevent axis jumping on each refresh
    yaxis_range = (
        [math.log10(max(b_min * 0.5, 1)), math.log10(b_max * 2)]
        if use_log else
        [0, b_max * 1.1]
    )

    fig.update_layout(
        barmode="group",
        xaxis_title="Time",
        yaxis_title="DL Bytes / 1s (SDAP)",
        yaxis_type="log" if use_log else "linear",
        yaxis_range=yaxis_range,
        legend_title="UE",
    )

    debug = f"log rows: {len(log_df)}  |  agg rows: {len(ue_volume_df)}"
    return fig, debug

# ============================================================
# Main
# ============================================================

async def main() -> None:
    print("─" * 60)
    print(f"srsRAN CU Log Reader  v{version}")
    print("─" * 60)
    print(f"Log file  : {LOG_FILE}")
    print(f"Platform  : {sys.platform}")
    print(f"Dashboard : http://127.0.0.1:{DASH_PORT}")
    print()

    tasks = [
        asyncio.create_task(tail_file_producer(LOG_FILE, queue)),
        asyncio.create_task(consumer(queue)),
    ]

    try:
        await asyncio.to_thread(app.run, host="127.0.0.1", port=DASH_PORT, debug=False, use_reloader=False)
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        for t in tasks:
            t.cancel()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

    print("\n... script gracefully stopped")
    print(f"\nLog rows captured : {len(log_df)}")
    print(f"Agg rows (1s/UE)  : {len(ue_volume_df)}")
    if len(ue_volume_df) > 0:
        print(f"Total DL bytes    : {ue_volume_df['bytes'].sum():,}")
        print(ue_volume_df)

    import os
    os._exit(0)
