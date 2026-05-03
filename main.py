"""FastAPI bot server for the magicpin Vera challenge.

Run:  python bot.py
or:   uvicorn bot:app --host 0.0.0.0 --port 8080
"""

from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from composer import (
    compose,
    deterministic_id,
    parse_dt,
    simple_reply_from_context,
    trigger_priority_score,
    owner_name,
)

ALLOWED_SCOPES = {"category", "merchant", "customer", "trigger"}

AUTO_REPLY_PATTERNS = (
    "thank you for contacting",
    "will respond shortly",
    "our team will contact",
    "away right now",
    "auto reply",
    "auto-reply",
    "business account",
)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(dt: Optional[datetime] = None) -> str:
    return (dt or now_utc()).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def is_auto_reply(message: str) -> bool:
    low = (message or "").strip().lower()
    return any(p in low for p in AUTO_REPLY_PATTERNS)


def customer_has_consent(customer: Dict[str, Any], trigger_kind: str) -> bool:
    consent = customer.get("consent") or {}
    scopes = {str(x).lower() for x in (consent.get("scope") or [])}
    if not scopes:
        return False
    if (customer.get("preferences") or {}).get("reminder_opt_in") is False:
        return False
    kind_scope_map: Dict[str, set] = {
        "recall_due":               {"recall_reminders", "appointment_reminders", "promotional_offers"},
        "appointment_tomorrow":     {"appointment_reminders"},
        "customer_lapsed_soft":     {"promotional_offers", "winback_offers", "renewal_reminders"},
        "customer_lapsed_hard":     {"promotional_offers", "winback_offers", "renewal_reminders"},
        "trial_followup":           {"appointment_reminders", "kids_program_updates", "bridal_package_followup"},
        "wedding_package_followup": {"bridal_package_followup", "promotional_offers"},
        "chronic_refill_due":       {"refill_reminders", "delivery_notifications", "recall_alerts"},
    }
    allowed = kind_scope_map.get(trigger_kind, {"promotional_offers", "appointment_reminders"})
    return bool(scopes & allowed)


# ---------------------------------------------------------------------------
# In-memory state store
# ---------------------------------------------------------------------------

class BotState:
    def __init__(self) -> None:
        self.start_ts = time.time()
        self._lock = threading.RLock()
        self.contexts: Dict[str, Dict[str, Dict[str, Any]]] = {
            s: {} for s in ALLOWED_SCOPES
        }
        self.sent_suppressions: Dict[str, datetime] = {}   # key → expires_at
        self.conversations: Dict[str, Dict[str, Any]] = {}
        self.merchant_snooze_until: Dict[str, datetime] = {}
        self.merchant_auto_reply_count: Dict[str, int] = {}

    # -- context store --

    def context_counts(self) -> Dict[str, int]:
        with self._lock:
            return {s: len(v) for s, v in self.contexts.items()}

    def upsert_context(self, scope: str, context_id: str, version: int, payload: Dict[str, Any]):
        if scope not in ALLOWED_SCOPES:
            return 400, {"accepted": False, "reason": "invalid_scope", "details": f"scope={scope!r} not in {sorted(ALLOWED_SCOPES)}"}
        if not isinstance(version, int) or version < 0:
            return 400, {"accepted": False, "reason": "invalid_version", "details": "version must be a non-negative integer"}
        with self._lock:
            current = self.contexts[scope].get(context_id)
            if current and current["version"] >= version:
                return 409, {"accepted": False, "reason": "stale_version", "current_version": current["version"]}
            self.contexts[scope][context_id] = {"version": version, "payload": payload}
            return 200, {"accepted": True, "ack_id": f"ack_{context_id}_v{version}", "stored_at": iso_utc()}

    def get_payload(self, scope: str, context_id: str) -> Optional[Dict[str, Any]]:
        entry = self.contexts.get(scope, {}).get(str(context_id))
        return entry["payload"] if entry else None

    # -- merchant snooze --

    def is_snoozed(self, merchant_id: Optional[str], ref: Optional[datetime] = None) -> bool:
        if not merchant_id:
            return False
        with self._lock:
            until = self.merchant_snooze_until.get(str(merchant_id))
            return bool(until and (ref or now_utc()) < until)

    def snooze(self, merchant_id: Optional[str], days: int = 30) -> None:
        if not merchant_id:
            return
        with self._lock:
            self.merchant_snooze_until[str(merchant_id)] = now_utc() + timedelta(days=days)

    # -- suppression --

    def is_suppressed(self, key: str, ref: Optional[datetime] = None) -> bool:
        if not key:
            return False
        check = ref or now_utc()
        with self._lock:
            until = self.sent_suppressions.get(key)
            if not until:
                return False
            if check >= until:
                self.sent_suppressions.pop(key, None)
                return False
            return True

    def mark_sent(self, action: Dict[str, Any], trigger: Dict[str, Any]) -> None:
        with self._lock:
            key = str(action.get("suppression_key") or "")
            if key:
                expires = parse_dt(trigger.get("expires_at"))
                default = now_utc() + timedelta(hours=48)
                until = min(expires, default) if (expires and expires > now_utc()) else default
                self.sent_suppressions[key] = until

            conv_id = str(action.get("conversation_id"))
            self.conversations[conv_id] = {
                "conversation_id": conv_id,
                "merchant_id": action.get("merchant_id"),
                "customer_id": action.get("customer_id"),
                "trigger_id": action.get("trigger_id"),
                "trigger_kind": trigger.get("kind"),
                "trigger_context": action.get("trigger_context", ""),
                "scope": trigger.get("scope", "merchant"),
                "suppression_key": key,
                "auto_reply_count": 0,
                "ended": False,
                "last_bot_body": action.get("body", ""),
                "updated_at": iso_utc(),
                "history": [
                    {"from": "bot", "type": "tick_send", "body": action.get("body", ""), "ts": iso_utc()},
                ],
            }

    # -- conversations --

    def get_or_create_conv(self, conv_id: str, merchant_id: Optional[str], customer_id: Optional[str]) -> Dict[str, Any]:
        with self._lock:
            if conv_id not in self.conversations:
                self.conversations[conv_id] = {
                    "conversation_id": conv_id,
                    "merchant_id": merchant_id,
                    "customer_id": customer_id,
                    "trigger_id": None,
                    "trigger_kind": "unknown",
                    "scope": "merchant",
                    "suppression_key": "",
                    "auto_reply_count": 0,
                    "ended": False,
                    "last_bot_body": "",
                    "updated_at": iso_utc(),
                    "history": [],
                }
            return self.conversations[conv_id]

    def append_history(self, conv_id: str, entry: Dict[str, Any]) -> None:
        with self._lock:
            conv = self.conversations.get(conv_id)
            if conv:
                conv.setdefault("history", []).append(entry)
                conv["updated_at"] = iso_utc()

    def bump_auto_reply(self, merchant_id: Optional[str]) -> int:
        if not merchant_id:
            return 1
        with self._lock:
            n = self.merchant_auto_reply_count.get(str(merchant_id), 0) + 1
            self.merchant_auto_reply_count[str(merchant_id)] = n
            return n

    def reset_auto_reply(self, merchant_id: Optional[str]) -> None:
        if not merchant_id:
            return
        with self._lock:
            self.merchant_auto_reply_count.pop(str(merchant_id), None)

    def trigger_expired(self, trigger: Dict[str, Any], now_dt: datetime) -> bool:
        exp = parse_dt(trigger.get("expires_at"))
        return bool(exp and exp < now_dt)


STATE = BotState()

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class HealthzResponse(BaseModel):
    status: str
    uptime_seconds: int
    contexts_loaded: Dict[str, int]


class MetadataResponse(BaseModel):
    team_name: str
    team_members: List[str]
    model: str
    approach: str
    contact_email: str
    version: str
    submitted_at: str


class ContextRequest(BaseModel):
    scope: str
    context_id: str
    version: int = Field(ge=0)
    payload: Dict[str, Any]
    delivered_at: Optional[str] = None


class ContextAccepted(BaseModel):
    accepted: bool
    ack_id: str
    stored_at: str


class ContextRejected(BaseModel):
    accepted: bool
    reason: str
    current_version: Optional[int] = None
    details: Optional[str] = None


class TickRequest(BaseModel):
    now: str
    available_triggers: List[str] = Field(default_factory=list)


class TickAction(BaseModel):
    conversation_id: str
    merchant_id: str
    customer_id: Optional[str] = None
    send_as: str
    trigger_id: str
    template_name: str
    template_params: List[str]
    body: str
    cta: str
    suppression_key: str
    rationale: str


class TickResponse(BaseModel):
    actions: List[TickAction] = Field(default_factory=list)


class ReplyRequest(BaseModel):
    conversation_id: str
    merchant_id: Optional[str] = None
    customer_id: Optional[str] = None
    from_role: str
    message: str
    received_at: Optional[str] = None
    turn_number: int


class ReplySend(BaseModel):
    action: str
    body: str
    cta: str
    rationale: str


class ReplyWait(BaseModel):
    action: str
    wait_seconds: int
    rationale: str


class ReplyEnd(BaseModel):
    action: str
    rationale: str


ReplyResponse = Union[ReplySend, ReplyWait, ReplyEnd]

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="Vera Deterministic Bot", version="2.0.0")


@app.get("/v1/healthz", response_model=HealthzResponse)
def healthz() -> HealthzResponse:
    return HealthzResponse(
        status="ok",
        uptime_seconds=int(time.time() - STATE.start_ts),
        contexts_loaded=STATE.context_counts(),
    )


@app.get("/v1/metadata", response_model=MetadataResponse)
def metadata() -> MetadataResponse:
    raw_members = os.getenv("TEAM_MEMBERS", "KIIT")
    members = [m.strip() for m in raw_members.split(",") if m.strip()] or ["KIIT"]
    return MetadataResponse(
        team_name=os.getenv("TEAM_NAME", "Vera Deterministic Bot"),
        team_members=members,
        model="deterministic-rules-v2",
        approach=(
            "deterministic router: trigger.kind × merchant/category/customer signals → "
            "multi-strategy scored decision engine → 2-3 sentence grounded message with "
            "action-forward CTA; stateful suppression + history-aware reply escalation"
        ),
        contact_email=os.getenv("CONTACT_EMAIL", "team@example.com"),
        version=os.getenv("BOT_VERSION", "2.0.0"),
        submitted_at=os.getenv("SUBMITTED_AT", "2026-05-03T00:00:00Z"),
    )


@app.post("/v1/context", response_model=Union[ContextAccepted, ContextRejected])
def push_context(body: ContextRequest):
    status, resp = STATE.upsert_context(body.scope, body.context_id, body.version, body.payload)
    if status != 200:
        return JSONResponse(status_code=status, content=resp)
    return resp


@app.post("/v1/tick", response_model=TickResponse)
def tick(body: TickRequest) -> TickResponse:
    now_dt = parse_dt(body.now) or now_utc()
    actions: List[Dict[str, Any]] = []

    # --- Candidate selection: ranked list per (scope, merchant, customer) bucket ---
    # Stores top-3 per bucket so suppressed best can fall back to second-best (Scenario C fix)
    best: Dict[str, List[Dict[str, Any]]] = {}

    for tid in body.available_triggers:
        trigger = STATE.get_payload("trigger", str(tid))
        if not trigger:
            continue
        if STATE.trigger_expired(trigger, now_dt):
            continue

        merchant_id = trigger.get("merchant_id") or (trigger.get("payload") or {}).get("merchant_id")
        if not merchant_id:
            continue
        if STATE.is_snoozed(merchant_id, now_dt):
            continue

        merchant = STATE.get_payload("merchant", str(merchant_id))
        if not merchant:
            continue

        scope = str(trigger.get("scope") or "merchant")

        # FIX: missing category_slug → use empty dict, never silently drop (Scenario B)
        category_slug = merchant.get("category_slug")
        category = STATE.get_payload("category", str(category_slug)) if category_slug else {}
        if category is None:
            category = {}

        customer = None
        customer_id = trigger.get("customer_id") or (trigger.get("payload") or {}).get("customer_id")
        if scope == "customer":
            if not customer_id:
                continue
            customer = STATE.get_payload("customer", str(customer_id))
            if not customer:
                continue
            if not customer_has_consent(customer, str(trigger.get("kind") or "")):
                continue

        priority = trigger_priority_score(trigger)
        bucket = f"{scope}:{merchant_id}:{customer_id or 'na'}"
        entry = {
            "priority": priority,
            "trigger_id": str(tid),
            "trigger": trigger,
            "merchant_id": str(merchant_id),
            "customer_id": str(customer_id) if customer_id else None,
            "merchant": merchant,
            "category": category,
            "customer": customer,
        }
        bucket_list = best.setdefault(bucket, [])
        bucket_list.append(entry)
        bucket_list.sort(key=lambda c: (-c["priority"], c["trigger_id"]))
        if len(bucket_list) > 3:
            bucket_list.pop()  # keep top 3 per bucket

    # --- Compose and emit: try each bucket's candidates in order, fall back when suppressed ---
    seen_buckets: set = set()
    all_sorted = sorted(
        (c for clist in best.values() for c in clist),
        key=lambda c: (-c["priority"], c["trigger_id"]),
    )

    for candidate in all_sorted:
        if len(actions) >= 20:
            break

        trigger = candidate["trigger"]
        tid = candidate["trigger_id"]
        merchant_id = candidate["merchant_id"]
        customer_id = candidate.get("customer_id")
        bucket = f"{trigger.get('scope', 'merchant')}:{merchant_id}:{customer_id or 'na'}"

        if bucket in seen_buckets:
            continue  # already emitted for this bucket

        merchant = candidate["merchant"]
        category = candidate["category"]
        customer = candidate.get("customer")

        composed = compose(category, merchant, trigger, customer)
        supp_key = str(composed.get("suppression_key") or "")

        if supp_key and STATE.is_suppressed(supp_key, now_dt):
            continue  # try next candidate for this bucket (bucket NOT marked as seen yet)

        seen_buckets.add(bucket)  # mark only after a successful emit
        conv_id = deterministic_id("conv", merchant_id, tid, supp_key or "ns")
        action = {
            "conversation_id": conv_id,
            "merchant_id": merchant_id,
            "customer_id": customer_id,
            "send_as": composed["send_as"],
            "trigger_id": tid,
            "template_name": composed["template_name"],
            "template_params": composed["template_params"],
            "body": composed["body"],
            "cta": composed["cta"],
            "suppression_key": supp_key,
            "rationale": composed["rationale"],
        }
        actions.append(action)
        STATE.mark_sent(action, trigger)

    return TickResponse(actions=[TickAction(**a) for a in actions])


@app.post("/v1/reply", response_model=ReplyResponse)
def reply(body: ReplyRequest):
    conv = STATE.get_or_create_conv(body.conversation_id, body.merchant_id, body.customer_id)

    if conv.get("ended"):
        return ReplyEnd(action="end", rationale="Conversation already closed.")

    message = body.message or ""
    merchant_id = conv.get("merchant_id") or body.merchant_id

    # Track auto-reply streak
    if is_auto_reply(message):
        conv["auto_reply_count"] = STATE.bump_auto_reply(merchant_id)
    else:
        STATE.reset_auto_reply(merchant_id)
        conv["auto_reply_count"] = 0

    # Load context for reply logic
    merchant = STATE.get_payload("merchant", str(merchant_id)) if merchant_id else None
    category = None
    if merchant:
        slug = merchant.get("category_slug")
        if slug:
            category = STATE.get_payload("category", str(slug))

    # Log inbound turn
    STATE.append_history(
        body.conversation_id,
        {"from": body.from_role, "type": "reply_in", "body": message, "ts": iso_utc()},
    )

    resp = simple_reply_from_context(conv, message, merchant=merchant, category=category)
    action = resp.get("action", "send")

    # Opt-out snooze — always, regardless of entry path (item 30)
    if action == "end" and resp.get("snooze_merchant"):
        STATE.snooze(merchant_id, days=30)
    # Also snooze on hostile keywords in message even if reply handler didn't set flag
    low = message.lower()
    if any(w in low for w in ["stop", "unsubscribe", "not interested", "spam", "useless", "leave me alone"]):
        STATE.snooze(merchant_id, days=30)

    if action == "send":
        body_text = str(resp.get("body") or "").strip()
        if not body_text:
            body_text = "Understood — reply YES to proceed or STOP to close this thread."

        # Deduplicate: don't send exact same body twice
        if body_text == conv.get("last_bot_body"):
            body_text += " Reply YES to continue."

        conv["last_bot_body"] = body_text
        conv["updated_at"] = iso_utc()
        STATE.append_history(
            body.conversation_id,
            {"from": "bot", "type": "reply_out", "action": "send", "body": body_text, "ts": iso_utc()},
        )
        return ReplySend(
            action="send",
            body=body_text,
            cta=str(resp.get("cta", "binary_yes_no")),
            rationale=str(resp.get("rationale", "Continuing conversation.")),
        )

    if action == "wait":
        wait_secs = int(resp.get("wait_seconds") or 1800)
        conv["updated_at"] = iso_utc()
        STATE.append_history(
            body.conversation_id,
            {"from": "bot", "type": "reply_out", "action": "wait", "wait_seconds": wait_secs, "ts": iso_utc()},
        )
        return ReplyWait(
            action="wait",
            wait_seconds=wait_secs,
            rationale=str(resp.get("rationale", "Backing off before retry.")),
        )

    # end
    conv["ended"] = True
    conv["updated_at"] = iso_utc()
    STATE.append_history(
        body.conversation_id,
        {"from": "bot", "type": "reply_out", "action": "end", "ts": iso_utc()},
    )
    return ReplyEnd(
        action="end",
        rationale=str(resp.get("rationale", "Conversation closed.")),
    )


def main() -> None:
    import uvicorn
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run("bot:app", host=host, port=port, reload=False, access_log=False)


if __name__ == "__main__":
    main()