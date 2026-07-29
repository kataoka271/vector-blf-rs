"""Genie AI integration: async query runner and app-action interpreter."""

import concurrent.futures
import re
import time as _time
from collections.abc import Callable
from dataclasses import dataclass, field

import pandas as pd

from .config import _LOCAL_DEV, cfg

_genie_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="genie")
# Maps request_id -> (Future, created_at_epoch)
_genie_futures: dict[str, tuple[concurrent.futures.Future, float]] = {}

_GENIE_FUTURE_TTL_SEC = 180.0  # prune entries older than this so _genie_futures doesn't
# grow unbounded across a long session; comfortably past the 120s pending-timeout plus a
# grace window for a few overlapping poll ticks to still observe completion.


def _prune_stale_genie_futures() -> None:
    """Drop _genie_futures entries older than the TTL.

    poll_genie_result no longer pops entries on first observed completion (that broke
    idempotency -- see poll_genie_result), so this is the only cleanup path now. Cheap:
    the dict holds at most a handful of entries (one in-flight Genie request per active
    browser tab).
    """
    now = _time.time()
    stale_ids = [rid for rid, (_, created_at) in _genie_futures.items() if now - created_at > _GENIE_FUTURE_TTL_SEC]
    for rid in stale_ids:
        _genie_futures.pop(rid, None)
    if stale_ids:
        print(f"[genie] pruned {len(stale_ids)} stale future(s): {stale_ids}", flush=True)


_ANOMALY_WINDOW_SEC = 30.0  # auto time-window half-width around anomaly timestamps
_MAX_EXTRACTED_SIGNALS = 12  # cap so a broad Genie query doesn't overplot the chart

_RE_SECONDS = re.compile(
    r"(?:between\s+)?(\d+(?:\.\d+)?)\s*s(?:ec(?:ond)?s?)?\s+(?:to|and|-|--)\s+(\d+(?:\.\d+)?)\s*s",
    re.IGNORECASE,
)
_RE_CLOCK = re.compile(r"(\d{1,2}:\d{2}(?::\d{2})?)\s*(?:to|-|--)\s*(\d{1,2}:\d{2}(?::\d{2})?)")
_RE_LAST = re.compile(r"last\s+(\d+(?:\.\d+)?)\s*(second|sec|minute|min)s?", re.IGNORECASE)


def build_context_prefix(
    filenames: list[str] | None,
    sources: list[str] | None,
    channels: list[str] | None,
    signals: list[str] | None = None,
) -> str:
    """Summarize the sidebar's current file/source/channel/signal selection as a Genie context prefix.

    Values are passed through as raw column/key values (not the sidebar's display
    labels, which relabel SOMEIP as ETH) so Genie can map them unambiguously onto
    blf_gold_signals columns.
    """
    parts = []
    if filenames:
        parts.append(f"file(s) (_source_file column) {', '.join(filenames)}")
    if sources:
        parts.append(f"source(s) (signal_source column) {', '.join(sources)}")
    if channels:
        # Each value concatenates signal_source + channel number, e.g. "CAN1" means
        # signal_source='CAN' AND channel=1; "SOMEIP2" means signal_source='SOMEIP' AND channel=2.
        parts.append(f"channel(s) (signal_source+channel key) {', '.join(channels)}")
    if signals:
        # Each value is signal_source+channel+"::"+signal_name, e.g. "SOMEIP2::foo" means
        # signal_source='SOMEIP' AND channel=2 AND signal_name='foo'.
        parts.append(f'selected signal(s) (signal_source+channel+"::"+signal_name key) {", ".join(signals)}')
    if not parts:
        return ""
    return "Currently viewing " + "; ".join(parts) + "."


def _genie_query(space_id: str, content: str, conversation_id: str | None, user_token: str) -> dict:
    """Run a Genie Space query in an executor thread. Returns result dict."""
    print(f"[_genie_query] start space_id={space_id} conversation_id={conversation_id}", flush=True)
    if _LOCAL_DEV:
        _time.sleep(1.5)
        mock_text = (
            f"Based on your query '{content}', I identified signals CAN1::EngineSpeed_rpm "
            "and CAN1::VehicleSpeed_kph between 50s and 150s."
        )
        return {
            "status": "done",
            "text": mock_text,
            "sql_rows": [],
            "conversation_id": conversation_id or "mock-conv-001",
        }

    assert cfg is not None
    try:
        from databricks.sdk import WorkspaceClient

        try:
            w = WorkspaceClient(token=user_token, host=cfg.host, auth_type="pat")
        except Exception as exc:
            print(f"[_genie_query] failed to create WorkspaceClient: {exc}", flush=True)
            w = WorkspaceClient(config=cfg)

        if conversation_id:
            msg = w.genie.create_message_and_wait(space_id, conversation_id, content=content)
        else:
            msg = w.genie.start_conversation_and_wait(space_id, content=content)
            conversation_id = str(msg.conversation_id)

        text = ""
        sql_rows: list[dict] = []

        if msg.attachments is None:
            raise ValueError("Genie response has no attachments")

        for att in msg.attachments:
            if att.text:
                if not att.text.content:
                    raise ValueError("Genie response attachment has no text content")
                text += att.text.content
            if att.query:
                result = w.genie.get_message_attachment_query_result(
                    space_id=space_id,
                    conversation_id=str(msg.conversation_id),
                    message_id=str(msg.message_id),
                    attachment_id=str(att.attachment_id),
                )
                sr = result.statement_response
                if not sr:
                    raise ValueError("Genie response attachment has no statement_response")
                if not (sr.manifest and sr.manifest.schema and sr.manifest.schema.columns):
                    raise ValueError("Genie response attachment has no schema columns")
                if not (sr.result and sr.result.data_array):
                    raise ValueError("Genie response attachment has no data rows")
                cols = [c.name for c in sr.manifest.schema.columns]
                sql_rows = [dict(zip(cols, row)) for row in sr.result.data_array]
        print(f"[_genie_query] done status=done conversation_id={msg.conversation_id}", flush=True)
        print(f"[_genie_query] text={text}", flush=True)
        print(f"[_genie_query] sql_rows={pd.DataFrame.from_records(sql_rows)}", flush=True)

        return {"status": "done", "text": text, "sql_rows": sql_rows, "conversation_id": str(msg.conversation_id)}
    except Exception as exc:
        print(f"[_genie_query] error: {exc}", flush=True)
        return {"status": "error", "text": str(exc), "sql_rows": [], "conversation_id": conversation_id or ""}


def _extract_signals(text: str, sql_rows: list[dict], all_signals: list[dict]) -> list[str]:
    """Extract signal keys matching known signals from Genie response."""
    all_keys = {f"{r['signal_source']}{r['channel']}::{r['signal_name']}": r for r in all_signals}
    all_names: dict[str, str] = {}
    all_names_by_source: dict[tuple[str, str], str] = {}
    for r in all_signals:
        key = f"{r['signal_source']}{r['channel']}::{r['signal_name']}"
        name_lower = r["signal_name"].lower()
        all_names[name_lower] = key
        all_names_by_source[(r["signal_source"].lower(), name_lower)] = key

    matched: list[str] = []

    # Priority 1: SQL result rows with signal_name column. sql_rows come from Genie's
    # own executed query against the real tables, so a row that already carries
    # signal_name + signal_source + channel is trusted directly rather than requiring
    # it to also appear in all_signals -- that cache is capped (see _fetch_all_signals)
    # and would otherwise make a real, valid signal outside the cap silently unmatchable.
    if sql_rows:
        counts: dict[str, int] = {}
        for row in sql_rows:
            sn = row.get("signal_name", "")
            src = row.get("signal_source")
            ch = row.get("channel")
            key = None
            if sn and src is not None and ch is not None:
                key = f"{src}{ch}::{sn}"
            elif sn and src is not None and (src.lower(), sn.lower()) in all_names_by_source:
                # No channel column in this row (Genie's query didn't select one) --
                # disambiguate via signal_source so a name that exists under multiple
                # sources (e.g. the same signal name on both CAN and SOMEIP) doesn't
                # silently collide in the name-only lookup below.
                key = all_names_by_source[(src.lower(), sn.lower())]
            elif sn and sn.lower() in all_names:
                key = all_names[sn.lower()]
            if key is None:
                continue
            counts[key] = counts.get(key, 0) + 1
        if counts:
            # Broad queries can return dozens of distinct signals -- more than a chart
            # can show. Rank by row frequency (a proxy for relevance when Genie's query
            # groups by signal, e.g. anomaly counts) and prefer signals the explanation
            # text actually names, since that's the closer match to what was asked.
            ranked = sorted(counts, key=lambda k: counts[k], reverse=True)
            text_lower = text.lower()
            mentioned = [k for k in ranked if k.split("::", 1)[1].lower() in text_lower]
            return (mentioned or ranked)[:_MAX_EXTRACTED_SIGNALS]

    # Priority 2: Exact key in text
    for key in all_keys:
        if key in text:
            matched.append(key)
    if matched:
        return list(dict.fromkeys(matched))

    # Priority 3: Case-insensitive signal_name substring
    text_lower = text.lower()
    for name_lower, key in all_names.items():
        if name_lower in text_lower:
            matched.append(key)
    return list(dict.fromkeys(matched))


def _extract_anomalies(sql_rows: list[dict]) -> list[dict]:
    """Extract point anomaly events from Genie SQL rows.

    Recognises rows that have a single timestamp column (timestamp_s or
    anomaly_timestamp_s) but NOT a min/max range pair (already consumed by
    _extract_time_range).  Returns a list of
    {"timestamp_s": float, "label": str, "signal_key": str | None}.
    signal_key is set when signal_name/signal_source/channel columns are present.
    """
    if not sql_rows:
        return []
    first = sql_rows[0]
    if "min_timestamp_s" in first or "max_timestamp_s" in first:
        return []
    ts_col = next((c for c in ["anomaly_timestamp_s", "timestamp_s"] if c in first), None)
    if ts_col is None:
        return []
    label_col = next((c for c in ["anomaly_type", "label", "type", "description"] if c in first), None)
    has_signal_cols = "signal_name" in first and "signal_source" in first and "channel" in first
    result = []
    for row in sql_rows:
        try:
            ts = float(row[ts_col])
        except (ValueError, TypeError):
            continue
        anom_type = str(row[label_col]) if label_col and row.get(label_col) else "Anomaly"
        signal_key: str | None = None
        if has_signal_cols and row.get("signal_name"):
            sn = str(row["signal_name"])
            src = str(row["signal_source"])
            ch = str(row["channel"])
            signal_key = f"{src}{ch}::{sn}"
            label = f"{sn}: {anom_type}"
        else:
            label = anom_type
        result.append({"timestamp_s": ts, "label": label, "signal_key": signal_key})
    return result


def _extract_time_range(text: str, sql_rows: list[dict], time_store: dict | None) -> tuple[float, float] | None:
    """Extract time range as (t_lo, t_hi) in timestamp_s units."""
    if not time_store:
        return None
    t_min = float(time_store.get("min", 0))
    t_max = float(time_store.get("max", 0))

    # Priority 1: SQL rows with timestamp columns
    if sql_rows and sql_rows[0]:
        ts_candidates = ["min_timestamp_s", "timestamp_s", "t_min"]
        te_candidates = ["max_timestamp_s", "timestamp_s", "t_max"]
        ts_col = next((c for c in ts_candidates if c in sql_rows[0]), None)
        te_col = next((c for c in te_candidates if c in sql_rows[0]), None)
        if ts_col and te_col:
            try:
                lo = min(float(r[ts_col]) for r in sql_rows if r.get(ts_col) is not None)
                hi = max(float(r[te_col]) for r in sql_rows if r.get(te_col) is not None)
                if lo < hi and t_min <= lo and hi <= t_max + 1:
                    return (lo, hi)
            except (ValueError, TypeError):
                pass

    # Priority 2: "last N seconds/minutes"
    m = _RE_LAST.search(text)
    if m:
        n = float(m.group(1))
        factor = 60.0 if "min" in m.group(2).lower() else 1.0
        duration = n * factor
        return (max(t_min, t_max - duration), t_max)

    # Priority 3: "50s to 100s"
    m = _RE_SECONDS.search(text)
    if m:
        lo = t_min + float(m.group(1))
        hi = t_min + float(m.group(2))
        return (max(t_min, lo), min(t_max, hi))

    # Priority 4: "10:05 to 10:35" clock time
    m = _RE_CLOCK.search(text)
    if m and time_store.get("t0"):
        try:
            t0 = pd.Timestamp(time_store["t0"])

            def _hms_to_s(hms: str) -> float:
                parts = hms.split(":")
                h, mn = int(parts[0]), int(parts[1])
                s = int(parts[2]) if len(parts) > 2 else 0
                return h * 3600 + mn * 60 + s

            t0_abs = t0.hour * 3600 + t0.minute * 60 + t0.second
            lo = t_min + (_hms_to_s(m.group(1)) - t0_abs)
            hi = t_min + (_hms_to_s(m.group(2)) - t0_abs)
            return (max(t_min, lo), min(t_max, hi))
        except Exception:
            pass

    return None


# ---------------------------------------------------------------------------
# App-action model
# ---------------------------------------------------------------------------


@dataclass
class GeniePreview:
    """App state delta produced by interpreting a Genie response.

    Each field is optional -- None / empty list means "no change".
    """

    signals: list[str] = field(default_factory=list)
    anomalous_signals: list[str] = field(default_factory=list)
    t_lo: float | None = None
    t_hi: float | None = None
    explanation: str = ""
    anomalies: list[dict] = field(default_factory=list)
    # Keys in `signals` that no currently selected file contains, mapped to the file
    # they should be read from instead (see db.fetch_signal_data's extra_scope).
    extra_scope: dict[str, list[str]] = field(default_factory=dict)
    # Keys Genie named that the signal catalog has no row for -- dropped from `signals`
    # since no query could ever return them.
    unknown_signals: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "signals": self.signals,
            "anomalous_signals": self.anomalous_signals,
            "t_lo": self.t_lo,
            "t_hi": self.t_hi,
            "explanation": self.explanation,
            "anomalies": self.anomalies,
            "extra_scope": self.extra_scope,
            "unknown_signals": self.unknown_signals,
        }


def _extract_source_files(sql_rows: list[dict]) -> set[str]:
    """Collect the _source_file values Genie's own query returned, if it selected any."""
    return {str(r["_source_file"]) for r in sql_rows if r.get("_source_file")}


def _pick_scope_file(candidates: list[str], preferred: set[str]) -> list[str]:
    """Choose which file an out-of-scope signal is read from.

    Prefers a file Genie's own query touched; otherwise takes the first candidate in
    sorted order. Deliberately returns a single file: a signal logged across many files
    would otherwise merge into one trace whose samples jump between logs, and the file
    actually chosen is the one reported back in the chat.
    """
    match = sorted(preferred & set(candidates))
    return match[:1] or candidates[:1]


def interpret_genie_response(
    result: dict,
    all_signals: list[dict],
    time_store: dict | None,
    resolve_file_scope: "Callable[[list[str]], tuple[dict[str, list[str]], list[str]]] | None" = None,
) -> "GeniePreview | None":
    """Map a Genie API result to a set of app actions.

    Extracts signals, time range, and anomaly markers from the response, applies
    automatic time-windowing around anomalies when no explicit range is given, and
    returns a GeniePreview describing the intended state change.  Returns None when
    the response contains nothing actionable.

    `resolve_file_scope` is called with the extracted signal keys and returns
    (out_of_scope, unknown) -- see db._fetch_signal_file_scopes. It is injected rather
    than imported so this module stays free of DB access and directly testable.
    """
    if result["status"] != "done":
        return None

    text = result["text"] or ""
    sql_rows = result["sql_rows"]

    signals = _extract_signals(text, sql_rows, all_signals)
    time_range = _extract_time_range(text, sql_rows, time_store)
    anomalies = _extract_anomalies(sql_rows)
    anomalous_signals = [a["signal_key"] for a in anomalies if a.get("signal_key")]

    extra_scope: dict[str, list[str]] = {}
    unknown_signals: list[str] = []
    if signals and resolve_file_scope is not None:
        out_of_scope, unknown_signals = resolve_file_scope(signals)
        preferred = _extract_source_files(sql_rows)
        extra_scope = {k: _pick_scope_file(files, preferred) for k, files in out_of_scope.items()}
        unknown = set(unknown_signals)
        signals = [s for s in signals if s not in unknown]
        anomalous_signals = [s for s in anomalous_signals if s not in unknown]

    if time_range is None and anomalies and time_store:
        t_min = float(time_store.get("min", 0))
        t_max = float(time_store.get("max", 0))
        anom_ts = [a["timestamp_s"] for a in anomalies]
        lo = max(t_min, min(anom_ts) - _ANOMALY_WINDOW_SEC)
        hi = min(t_max, max(anom_ts) + _ANOMALY_WINDOW_SEC)
        if lo < hi:
            time_range = (lo, hi)

    if not signals and time_range is None and not anomalies and not unknown_signals:
        return None

    return GeniePreview(
        signals=signals,
        anomalous_signals=anomalous_signals,
        t_lo=time_range[0] if time_range else None,
        t_hi=time_range[1] if time_range else None,
        explanation=text,
        anomalies=anomalies,
        extra_scope=extra_scope,
        unknown_signals=unknown_signals,
    )
