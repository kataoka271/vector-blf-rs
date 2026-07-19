"""Genie AI integration: async query runner and app-action interpreter."""

import concurrent.futures
import re
import time as _time
from dataclasses import dataclass, field

import pandas as pd

from .config import _LOCAL_DEV, cfg

_genie_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="genie")
# Maps request_id -> (Future, created_at_epoch)
_genie_futures: dict[str, tuple[concurrent.futures.Future, float]] = {}

_ANOMALY_WINDOW_SEC = 30.0  # auto time-window half-width around anomaly timestamps

_RE_SECONDS = re.compile(
    r"(?:between\s+)?(\d+(?:\.\d+)?)\s*s(?:ec(?:ond)?s?)?\s+(?:to|and|-|--)\s+(\d+(?:\.\d+)?)\s*s",
    re.IGNORECASE,
)
_RE_CLOCK = re.compile(r"(\d{1,2}:\d{2}(?::\d{2})?)\s*(?:to|-|--)\s*(\d{1,2}:\d{2}(?::\d{2})?)")
_RE_LAST = re.compile(r"last\s+(\d+(?:\.\d+)?)\s*(second|sec|minute|min)s?", re.IGNORECASE)

_RE_CANDIDATE_TOKEN = re.compile(r"[A-Za-z][A-Za-z0-9_]{2,}")
_MAX_CANDIDATE_TOKENS = 200


def extract_candidate_tokens(text: str, sql_rows: list[dict]) -> list[str]:
    """Pull likely signal-name tokens out of a Genie response for a targeted DB lookup.

    Deliberately cheap and over-inclusive -- plain English words match the token
    shape too. The caller does an exact, case-insensitive lookup against the real
    catalog, so a false-positive token just matches nothing; it doesn't cost a
    full-catalog scan the way matching client-side against an unbounded signal
    list would.
    """
    tokens: list[str] = [str(row["signal_name"]) for row in (sql_rows or []) if row.get("signal_name")]
    tokens.extend(_RE_CANDIDATE_TOKEN.findall(text or ""))
    return list(dict.fromkeys(tokens))[:_MAX_CANDIDATE_TOKENS]


def build_context_prefix(
    filenames: list[str] | None,
    sources: list[str] | None,
    channels: list[str] | None,
) -> str:
    """Summarize the sidebar's current file/source/channel filters as a Genie context prefix.

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

        w = WorkspaceClient(config=cfg)
        if conversation_id:
            msg = w.genie.create_message_and_wait(space_id, conversation_id, content=content)
        else:
            result = w.genie.start_conversation_and_wait(space_id, content=content)
            msg = result
            conversation_id = str(msg.conversation_id)

        text = ""
        sql_rows: list[dict] = []
        if hasattr(msg, "attachments") and msg.attachments:
            for att in msg.attachments:
                if hasattr(att, "text") and att.text:
                    text += att.text.content or ""
                if hasattr(att, "query") and att.query:
                    try:
                        result_set = w.genie.get_message_query_result_by_attachment(
                            space_id, str(msg.conversation_id), str(msg.id), str(att.id)
                        )
                        if result_set and hasattr(result_set, "statement_response"):
                            sr = result_set.statement_response
                            if sr and sr.result and sr.manifest:
                                cols = [c.name for c in sr.manifest.schema.columns]
                                sql_rows = [dict(zip(cols, row)) for row in (sr.result.data_array or [])]
                    except Exception:
                        pass
        print(f"[_genie_query] done status=done conversation_id={msg.conversation_id}", flush=True)
        return {"status": "done", "text": text, "sql_rows": sql_rows, "conversation_id": str(msg.conversation_id)}
    except Exception as exc:
        print(f"[_genie_query] error: {exc}", flush=True)
        return {"status": "error", "text": str(exc), "sql_rows": [], "conversation_id": conversation_id or ""}


def _extract_signals(text: str, sql_rows: list[dict], all_signals: list[dict]) -> list[str]:
    """Extract signal keys matching known signals from Genie response."""
    all_keys = {f"{r['signal_source']}{r['channel']}::{r['signal_name']}": r for r in all_signals}
    all_names = {
        r["signal_name"].lower(): f"{r['signal_source']}{r['channel']}::{r['signal_name']}" for r in all_signals
    }

    matched: list[str] = []

    # Priority 1: SQL result rows with signal_name column
    if sql_rows:
        for row in sql_rows:
            sn = row.get("signal_name", "")
            src = str(row.get("signal_source", ""))
            ch = str(row.get("channel", ""))
            full_key = f"{src}{ch}::{sn}"
            if full_key in all_keys:
                matched.append(full_key)
            elif sn.lower() in all_names:
                matched.append(all_names[sn.lower()])
        if matched:
            return list(dict.fromkeys(matched))

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

    def to_dict(self) -> dict:
        return {
            "signals": self.signals,
            "anomalous_signals": self.anomalous_signals,
            "t_lo": self.t_lo,
            "t_hi": self.t_hi,
            "explanation": self.explanation,
            "anomalies": self.anomalies,
        }


def interpret_genie_response(
    result: dict,
    all_signals: list[dict],
    time_store: dict | None,
) -> "GeniePreview | None":
    """Map a Genie API result to a set of app actions.

    Extracts signals, time range, and anomaly markers from the response, applies
    automatic time-windowing around anomalies when no explicit range is given, and
    returns a GeniePreview describing the intended state change.  Returns None when
    the response contains nothing actionable.
    """
    if result["status"] != "done":
        return None

    text = result["text"] or ""
    sql_rows = result["sql_rows"]

    signals = _extract_signals(text, sql_rows, all_signals)
    time_range = _extract_time_range(text, sql_rows, time_store)
    anomalies = _extract_anomalies(sql_rows)
    anomalous_signals = [a["signal_key"] for a in anomalies if a.get("signal_key")]

    if time_range is None and anomalies and time_store:
        t_min = float(time_store.get("min", 0))
        t_max = float(time_store.get("max", 0))
        anom_ts = [a["timestamp_s"] for a in anomalies]
        lo = max(t_min, min(anom_ts) - _ANOMALY_WINDOW_SEC)
        hi = min(t_max, max(anom_ts) + _ANOMALY_WINDOW_SEC)
        if lo < hi:
            time_range = (lo, hi)

    if not signals and time_range is None and not anomalies:
        return None

    return GeniePreview(
        signals=signals,
        anomalous_signals=anomalous_signals,
        t_lo=time_range[0] if time_range else None,
        t_hi=time_range[1] if time_range else None,
        explanation=text,
        anomalies=anomalies,
    )
