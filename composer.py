"""Deterministic composition engine for the magicpin Vera challenge.

Architecture:
- Each compose_* function produces the COMPLETE final message (2-3 sentences max).
- No assembly pipeline. No "Recommended action:" label. No fixed 4-part structure.
- Every message = hook (impact/data/urgency) + consequence/action + CTA (action-forward).
- CTAs always frame WHAT WILL HAPPEN, never "Should I?" or "Ready to?".
- Fallback is always actionable — internal data state is never exposed to merchant.
- Strategy selection is scored across all options, not just first-in-list.
- Demand signals and merchant context drive decisions, not just wording.
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Core utilities
# ---------------------------------------------------------------------------

def parse_dt(value):
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def safe_first(seq, default=None):
    if isinstance(seq, list) and seq:
        return seq[0]
    return default


def as_pct(value, sign=False):
    if value is None:
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    prefix = "+" if sign and num > 0 else ""
    return f"{prefix}{num * 100:.0f}%"


def compact_num(value):
    try:
        n = int(value)
    except (TypeError, ValueError):
        return "?"
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n:,}"
    return str(n)


def pick_active_offer(merchant, category):
    for offer in (merchant.get("offers") or []):
        if offer.get("status") == "active" and offer.get("title"):
            return str(offer["title"])
    for offer in (category.get("offer_catalog") or []):
        if offer.get("title"):
            return str(offer["title"])
    return None


_HONORIFICS = {"dr", "mr", "mrs", "ms", "prof", "sir", "smt", "shri"}

_KIND_SUFFIX = {
    "competitor_opened":    "— a competitor just entered your area",
    "festival_upcoming":    "— a demand spike is building right now",
    "perf_dip":             "— your metrics are slipping below peer average",
    "seasonal_perf_dip":    "— seasonal demand is shifting in your category",
    "dormant_with_vera":    "— your audience is warm but getting cold",
    "winback_eligible":     "— lapsed customers are still in reach",
    "gbp_unverified":       "— unverified listings lose clicks to verified ones",
    "supply_alert":         "— an inventory gap is live on your shelf right now",
    "ipl_match_day":        "— match-day demand is peaking in the next few hours",
    "renewal_due":          "— your subscription window is closing",
    "review_theme_emerged": "— a review pattern is affecting your visibility",
}

_SLUG_METRICS = {
    "dentists":        {"calls": "consult calls",       "views": "profile views"},
    "restaurants":     {"calls": "order calls",         "views": "menu views"},
    "salons":          {"calls": "booking calls",       "views": "profile views"},
    "gyms":            {"calls": "trial calls",         "views": "listing views"},
    "pharmacies":      {"calls": "refill calls",        "views": "search impressions"},
    "spas":            {"calls": "appointment calls",   "views": "profile views"},
    "opticians":       {"calls": "eye-test bookings",   "views": "listing views"},
    "diagnostic_labs": {"calls": "test bookings",       "views": "search impressions"},
    "bakeries":        {"calls": "order calls",         "views": "menu views"},
    "jewellers":       {"calls": "visit inquiries",     "views": "catalogue views"},
    "clinics":         {"calls": "appointment calls",   "views": "listing views"},
    "hospitals":       {"calls": "appointment calls",   "views": "listing views"},
    "coaching":        {"calls": "inquiry calls",       "views": "listing views"},
    "beauty_parlours": {"calls": "booking calls",       "views": "profile views"},
    "grocery":         {"calls": "delivery calls",      "views": "store views"},
    "hardware_stores": {"calls": "order calls",         "views": "store views"},
}

_CUSTOMER_NOUN = {
    "dentists":        "patients",
    "salons":          "clients",
    "gyms":            "members",
    "restaurants":     "regulars",
    "pharmacies":      "patients",
    "spas":            "clients",
    "clinics":         "patients",
    "coaching":        "students",
    "beauty_parlours": "clients",
    "diagnostic_labs": "patients",
}

_CTR_LABEL = {
    "dentists":    "appointment conversion rate",
    "pharmacies":  "search-to-call rate",
    "restaurants": "menu click rate",
    "salons":      "booking conversion rate",
    "gyms":        "inquiry conversion rate",
    "spas":        "booking conversion rate",
    "clinics":     "appointment conversion rate",
}

_SOCIAL_PROOF = {
    "dentists": {
        "winback":           "In {locality}, dental practices that run targeted recall campaigns in the first 30-day window see 25-38% reactivation.",
        "review_theme":      "{n} dental practices in {locality} resolved similar review patterns this month with a response template + one GBP correction post.",
        "regulation_change": "{n} practices in {locality} are already reviewing this — acting first keeps you compliant before DCI enforcement windows open.",
        "milestone":         "Your review count now puts you in the top {pct}% of dental practices in {locality}.",
        "perf_spike":        "This spike puts your CTR {gap_txt} above the {locality} peer average — few practices hit this consistently.",
        "dormant_with_vera": "{n} dentists in {locality} who reactivated outreach last month saw a 20-30% uptick in consultation calls within 2 weeks.",
    },
    "restaurants": {
        "winback":           "Restaurants in {locality} that sent comeback campaigns in the first 45 days averaged 22% reactivation from lapsed diners.",
        "review_theme":      "{n} restaurants in {locality} contained similar review patterns in under 48h using a templated reply + corrective post — their rating held.",
        "regulation_change": "{n} outlets in {locality} have already updated compliance docs this week.",
        "milestone":         "Your review count now ranks you in the top {pct}% of restaurants in {locality}.",
        "perf_spike":        "This spike is {gap_txt} above the {locality} dining average — amplifying now before it decays is the highest-leverage action.",
        "dormant_with_vera": "{n} restaurants in {locality} that restarted outreach last month saw order volume recover within 10 days.",
    },
    "salons": {
        "winback":           "Salons in {locality} that sent lapsed-client offers in the first 30 days saw 30-40% return in the first pass.",
        "review_theme":      "{n} salons in {locality} responded to similar patterns this month — early responses consistently outperformed silent ones in star rating.",
        "regulation_change": "{n} salons in {locality} have already reviewed the updated guidelines.",
        "milestone":         "Your review count now puts you in the top {pct}% of salons in {locality}.",
        "dormant_with_vera": "{n} salons in {locality} that reactivated outreach last month recovered 35% of their dormant booking slots.",
    },
    "gyms": {
        "winback":           "Gyms in {locality} that ran trial-to-membership re-engagement in the first 30 days saw 25-35% reactivation.",
        "review_theme":      "{n} gyms in {locality} resolved similar review patterns this month with templated responses.",
        "milestone":         "Your review count now ranks you in the top {pct}% of gyms in {locality}.",
        "dormant_with_vera": "{n} gyms in {locality} that restarted outreach recovered trial-call volume within 2 weeks.",
    },
    "pharmacies": {
        "winback":           "Pharmacies in {locality} that sent refill reminders in the first 30-day lapse window saw 30%+ reactivation of repeat customers.",
        "review_theme":      "{n} pharmacies in {locality} addressed similar feedback this month — response time under 24h improved aggregate ratings.",
        "milestone":         "Your review count now puts you in the top {pct}% of pharmacies in {locality}.",
        "dormant_with_vera": "{n} pharmacies in {locality} that restarted outreach recovered refill call volume within 10 days.",
    },
}

def _get_social_proof(slug, action, locality, n=3, pct=30, gap_txt=""):
    """Returns a social-proof sentence for the given (slug, action), or empty string."""
    slug_map = _SOCIAL_PROOF.get(slug) or _SOCIAL_PROOF.get("restaurants")
    template = (slug_map or {}).get(action)
    if not template:
        return ""
    return template.format(locality=locality, n=n, pct=pct, gap_txt=gap_txt)


def owner_name(merchant):
    ident = merchant.get("identity") or {}
    owner = ident.get("owner_first_name")
    if owner:
        return str(owner)
    name = str(ident.get("name") or "")
    parts = name.split()
    for part in parts:
        clean = part.lower().rstrip(".")
        if clean not in _HONORIFICS and part.replace(".", "").isalpha():
            return part
    return parts[0] if parts else "there"


def merchant_name(merchant):
    return str((merchant.get("identity") or {}).get("name") or "your business")


def merchant_locality(merchant):
    return str((merchant.get("identity") or {}).get("locality") or "your area")


def customer_language_pref(customer):
    if not customer:
        return "en"
    pref = str((customer.get("identity") or {}).get("language_pref") or "en").lower()
    return "hi-en" if "hi" in pref else "en"


def month_day_label(iso_text):
    dt = parse_dt(iso_text)
    return dt.strftime("%d %b") if dt else iso_text


def find_metric_snapshot(merchant):
    perf = merchant.get("performance") or {}
    try:
        views = int(perf.get("views"))
    except (TypeError, ValueError):
        views = None
    try:
        calls = int(perf.get("calls"))
    except (TypeError, ValueError):
        calls = None
    try:
        ctr = float(perf.get("ctr"))
    except (TypeError, ValueError):
        ctr = None
    return views, calls, ctr


def find_peer_ctr(category):
    try:
        return float((category.get("peer_stats") or {}).get("avg_ctr"))
    except (TypeError, ValueError):
        return None


def resolve_digest_item(category, trigger):
    digest = category.get("digest") or []
    payload = trigger.get("payload") or {}
    for key in ("top_item_id", "digest_item_id", "alert_id"):
        item_id = payload.get(key)
        if item_id:
            for item in digest:
                if item.get("id") == item_id:
                    return item
    return safe_first(digest)


def deterministic_id(prefix, *parts):
    key = "|".join(parts)
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}_{digest}"


def _join(*parts):
    return " ".join(p.strip() for p in parts if p and p.strip())


def _slug_metric(slug, metric):
    return _SLUG_METRICS.get(slug, {}).get(metric, str(metric).replace("_", " "))


def _ctr_label(slug):
    return _CTR_LABEL.get(slug, "click-through rate")


def _customer_noun(slug):
    return _CUSTOMER_NOUN.get(slug, "customers")


def _metric_narrative(ms, slug):
    """One-sentence metric story for message bodies: 'X views, Y calls (Z% CTR) — area peers avg P% (Npp gap).'"""
    views = ms.get("views")
    calls = ms.get("calls")
    ctr = ms.get("ctr")
    peer_ctr = ms.get("peer_ctr")
    ctr_gap = ms.get("ctr_gap")
    locality = ms.get("locality", "your area")
    if views is None and calls is None:
        return None
    parts = []
    if views is not None:
        parts.append(f"{compact_num(views)} {_slug_metric(slug, 'views')}")
    if calls is not None:
        parts.append(f"{compact_num(calls)} {_slug_metric(slug, 'calls')}")
    base = ", ".join(parts)
    if ctr is not None:
        base += f" ({ctr*100:.1f}% CTR)"
    if ctr_gap is not None and peer_ctr is not None:
        try:
            gap_pp = float(ctr_gap) * 100
            direction = "below" if gap_pp < 0 else "above"
            base += f" — {locality} peers avg {peer_ctr*100:.1f}%, {abs(gap_pp):.1f}pp {direction}."
        except (TypeError, ValueError):
            base += "."
    else:
        base += "."
    return base


def _estimate_uplift_calls(views, ctr, peer_ctr):
    """Estimated extra calls/month if CTR reaches peer benchmark. Returns int >= 3, or None."""
    if views is None or ctr is None or peer_ctr is None:
        return None
    try:
        gap = float(peer_ctr) - float(ctr)
        if gap <= 0:
            return None
        uplift = round(gap * int(views))
        return uplift if uplift >= 3 else None
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def _stage_callback(stage, kind_label, no_reply_streak=0):
    """Returns a short callback line for stage >= 2 re-engagements. Empty string at stage 1."""
    if stage < 2:
        return ""
    days_est = max(1, round((no_reply_streak or (stage - 1)) * 1.5))
    if stage == 2:
        return f"You haven't acted on the {kind_label} I flagged {days_est} day(s) ago — it's still live."
    return f"Third touch on this {kind_label} — the window is narrowing, not widening."


def _pick_cta(pool, merchant_id, kind, no_reply_streak=0):
    key = f"{merchant_id}:{kind}:{no_reply_streak % max(len(pool), 1)}"
    idx = int(hashlib.md5(key.encode()).hexdigest()[:4], 16) % len(pool)
    return pool[idx]


def _cta(scenario, offer=None, locality="", urgency="medium", stage=1, merchant_id="", kind="",
         no_reply_streak=0, uplift=None, context=""):
    """Action-forward CTA — frames WHAT WILL HAPPEN, never 'Should I?'."""
    _o = f'"{offer}"' if offer else "a post"
    _l = f" to {locality} searchers" if locality else ""

    # Scenario-specific pools checked FIRST for cases where consequence > generic urgency
    if scenario == "compliance":
        pool = [
            "Reply CONFIRM to lock it in — lapse recovery takes 5-10 days.",
            "CONFIRM to process it now before the cutoff.",
            "On CONFIRM, renewal goes through in 30 seconds.",
        ]
    elif scenario == "defend" and kind == "review_theme_emerged":
        pool = [
            "Reply YES — I send the response script + GBP post draft now.",
            "YES to the response pack — stops the pattern today.",
            "On YES, response draft goes out before the next review posts.",
        ]
    elif urgency == "high" or stage >= 3:
        pool = [
            "Reply YES — this needs to go out today.",
            f"On YES, I send it now{_l}.",
            "YES to act before the window closes.",
        ]
    elif stage == 2:
        pool = [
            "Reply YES for the ready-to-send draft.",
            "YES and I queue this now.",
            "On YES, done in 60 seconds.",
        ]
    elif scenario == "recovery":
        pool = [
            f"Reply YES — I send {_o} for the recovery push.",
            f"YES to a 7-day recovery push{_l}.",
            "On YES, I send the recovery draft now.",
        ]
    elif scenario == "festival":
        pool = [
            "YES to a festive campaign draft now.",
            "Reply YES — campaign copy ready in 2 min.",
            "On YES, draft goes out today.",
        ]
    elif scenario == "defend":
        pool = [
            "Reply YES and I send a counter-offer this week.",
            "YES to a local defence offer now.",
            "On YES, I draft the counter-positioning post.",
        ]
    elif scenario == "winback":
        cohort = context or "your lapsed cohort"
        pool = [
            f"Reply YES — comeback offer goes to {cohort} this week.",
            "YES to targeted reactivation — 30-day window is the highest-convert rate.",
            "On YES, I send the outreach now before the 45-day cold window closes.",
        ]
    elif scenario == "scale":
        pool = [
            "Reply YES — I send the amplification post while this signal is live.",
            "YES to capture this momentum window.",
            "On YES, celebration post + review template go out now.",
        ]
    elif scenario == "verify":
        pool = [
            "Reply YES for the verification checklist now.",
            "YES and I send the step-by-step guide.",
            "On YES, shortest path to verified.",
        ]
    elif scenario == "digest":
        pool = [
            "Reply YES — I turn this into a ready-to-post draft.",
            "YES to a merchant-ready summary + post.",
            "On YES, draft goes out now.",
        ]
    else:
        pool = [
            "Reply YES to proceed.",
            "YES to send the draft now.",
            "On YES, I execute this immediately.",
        ]
    # ROI uplift override: replace first pool variant with a concrete payoff framing
    if uplift and len(pool) > 0 and urgency in ("high", "medium"):
        pool = list(pool)
        pool[0] = f"This can add ~{uplift} more calls this month. Reply YES to send it."
    return _pick_cta(pool, merchant_id, kind, no_reply_streak)


# ---------------------------------------------------------------------------
# Action taxonomy
# ---------------------------------------------------------------------------

ACTION_RECOVERY      = "recovery"
ACTION_SCALE         = "scale"
ACTION_WINBACK       = "winback"
ACTION_SEASONAL_PUSH = "seasonal_push"
ACTION_COMPLIANCE    = "compliance"
ACTION_RETENTION     = "retention"
ACTION_EXECUTION     = "execution"
ACTION_DEFENSE       = "defense"
ACTION_EDUCATION     = "education"
ACTION_LISTING_FIX   = "listing_fix"
ACTION_NURTURE       = "nurture"
ACTION_FALLBACK      = "fallback"

VALID_CTAS = {"binary_yes_no", "binary_confirm_cancel", "multi_choice_slot"}
URGENCY_RANK = {"low": 1, "medium": 2, "high": 3}

TRIGGER_ACTION_MAP = {
    "research_digest":          ACTION_EDUCATION,
    "cde_opportunity":          ACTION_EDUCATION,
    "regulation_change":        ACTION_COMPLIANCE,
    "supply_alert":             ACTION_COMPLIANCE,
    "renewal_due":              ACTION_COMPLIANCE,
    "recall_due":               ACTION_RETENTION,
    "appointment_tomorrow":     ACTION_RETENTION,
    "trial_followup":           ACTION_RETENTION,
    "chronic_refill_due":       ACTION_RETENTION,
    "wedding_package_followup": ACTION_RETENTION,
    "perf_dip":                 ACTION_RECOVERY,
    "seasonal_perf_dip":        ACTION_RECOVERY,
    "review_theme_emerged":     ACTION_RECOVERY,
    "perf_spike":               ACTION_SCALE,
    "milestone_reached":        ACTION_SCALE,
    "winback_eligible":         ACTION_WINBACK,
    "dormant_with_vera":        ACTION_WINBACK,
    "customer_lapsed_soft":     ACTION_WINBACK,
    "customer_lapsed_hard":     ACTION_WINBACK,
    "festival_upcoming":        ACTION_SEASONAL_PUSH,
    "ipl_match_today":          ACTION_SEASONAL_PUSH,
    "category_seasonal":        ACTION_SEASONAL_PUSH,
    "active_planning_intent":   ACTION_EXECUTION,
    "competitor_opened":        ACTION_DEFENSE,
    "gbp_unverified":           ACTION_LISTING_FIX,
    "curious_ask_due":          ACTION_NURTURE,
}

ACTION_STRATEGIES = {
    ACTION_RECOVERY:      ["offer_refresh", "metric_recovery", "reputation_repair", "gentle_nudge"],
    ACTION_SCALE:         ["milestone_amplify", "momentum_scale", "viral_capture"],
    ACTION_WINBACK:       ["lapse_reactivation", "comeback_offer", "dormant_restart"],
    ACTION_SEASONAL_PUSH: ["event_push", "seasonal_offer", "festive_defence"],
    ACTION_COMPLIANCE:    ["safety_response", "deadline_checklist", "renewal_completion"],
    ACTION_RETENTION:     ["slot_confirmation", "refill_reminder", "followup_conversion"],
    ACTION_EXECUTION:     ["plan_dispatch", "launch_ready"],
    ACTION_DEFENSE:       ["counter_positioning", "locality_lock"],
    ACTION_EDUCATION:     ["digest_application", "knowledge_to_campaign"],
    ACTION_LISTING_FIX:   ["verification_fix", "listing_optimise"],
    ACTION_NURTURE:       ["engagement_probe"],
    ACTION_FALLBACK:      ["category_playbook"],
}

ACTION_CTA_TYPE = {
    ACTION_RECOVERY: "binary_yes_no", ACTION_SCALE: "binary_yes_no",
    ACTION_WINBACK: "binary_yes_no", ACTION_SEASONAL_PUSH: "binary_yes_no",
    ACTION_COMPLIANCE: "binary_confirm_cancel", ACTION_RETENTION: "multi_choice_slot",
    ACTION_EXECUTION: "binary_confirm_cancel", ACTION_DEFENSE: "binary_yes_no",
    ACTION_EDUCATION: "binary_yes_no", ACTION_LISTING_FIX: "binary_yes_no",
    ACTION_NURTURE: "binary_yes_no", ACTION_FALLBACK: "binary_yes_no",
}

PROBLEM_KINDS = {
    "regulation_change", "recall_due", "perf_dip", "seasonal_perf_dip",
    "renewal_due", "review_theme_emerged", "supply_alert", "chronic_refill_due",
    "gbp_unverified", "competitor_opened", "dormant_with_vera", "winback_eligible",
    "customer_lapsed_soft", "customer_lapsed_hard", "appointment_tomorrow",
}

TRIGGER_PRIORITY_BASE = {
    "supply_alert": 100, "regulation_change": 95, "appointment_tomorrow": 92,
    "chronic_refill_due": 90, "recall_due": 88, "active_planning_intent": 86,
    "customer_lapsed_hard": 84, "renewal_due": 82, "review_theme_emerged": 80,
    "perf_dip": 78, "winback_eligible": 74, "competitor_opened": 72,
    "gbp_unverified": 70, "trial_followup": 68, "wedding_package_followup": 66,
    "customer_lapsed_soft": 64, "seasonal_perf_dip": 62, "ipl_match_today": 60,
    "festival_upcoming": 58, "category_seasonal": 56, "perf_spike": 54,
    "milestone_reached": 52, "research_digest": 50, "cde_opportunity": 48,
    "dormant_with_vera": 46, "curious_ask_due": 44,
}


def urgency_label(raw):
    try:
        n = int(raw)
    except (TypeError, ValueError):
        n = 0
    return "high" if n >= 4 else "medium" if n >= 2 else "low"


def urgency_max(a, b):
    return a if URGENCY_RANK.get(a, 1) >= URGENCY_RANK.get(b, 1) else b


def trigger_priority_score(trigger):
    """Extended priority: base + business_impact + demand_intensity + merchant_health."""
    kind = str(trigger.get("kind") or "unknown")
    base = TRIGGER_PRIORITY_BASE.get(kind, 20)
    try:
        urg = min(int(trigger.get("urgency") or 0), 5) * 3
    except (TypeError, ValueError):
        urg = 0

    payload = trigger.get("payload") or {}
    impact = 0
    if payload.get("renewal_amount"):
        impact += 5
    try:
        impact += min(int(payload.get("lapsed_customers_added_since_expiry") or 0) // 5, 10)
    except (TypeError, ValueError):
        pass
    if payload.get("estimated_uplift_pct"):
        impact += 4
    if payload.get("affected_batches"):
        impact += 8
    try:
        impact += min(int(payload.get("occurrences_30d") or 0) * 2, 8)
    except (TypeError, ValueError):
        pass

    # Fix W5: log scale so 10/30/75/200 searches all get meaningful boosts (no cliff at 50)
    import math
    demand = 0
    for key in ("search_count", "searches_nearby", "missed_searches", "demand_count"):
        try:
            c = int(payload.get(key) or 0)
            if c > 0:
                demand = min(int(math.log2(c + 1)), 10)
                break
        except (TypeError, ValueError):
            pass

    health = 0
    try:
        d = float(payload.get("delta_pct") or 0)
        if d < -0.2:
            health += 6
        elif d < -0.1:
            health += 3
    except (TypeError, ValueError):
        pass

    scope_bonus = 2 if str(trigger.get("scope") or "merchant") == "customer" else 0
    return base * 10 + urg + impact + demand + health + scope_bonus


# ---------------------------------------------------------------------------
# Signal normalization
# ---------------------------------------------------------------------------

def _count_no_reply_streak(conv_history):
    streak = 0
    for entry in reversed(conv_history or []):
        frm = str(entry.get("from") or "").lower()
        if frm == "bot":
            streak += 1
        elif frm in {"merchant", "customer"}:
            break
    return streak


def normalize_signals(category, merchant, trigger, customer=None, conversation=None):
    payload = trigger.get("payload") or {}
    perf = merchant.get("performance") or {}
    delta_7d = perf.get("delta_7d") or {}
    views, calls, ctr = find_metric_snapshot(merchant)
    peer_ctr = find_peer_ctr(category)
    ctr_gap = (ctr - peer_ctr) if (ctr is not None and peer_ctr is not None) else None

    active_offer = pick_active_offer(merchant, category)
    trend_signals = category.get("trend_signals") or []
    top_trend = safe_first(trend_signals) or {}
    trend_text = None
    if top_trend.get("query") and top_trend.get("delta_yoy") is not None:
        try:
            trend_text = f"{top_trend['query']} searches {float(top_trend['delta_yoy'])*100:.0f}% YoY"
        except (TypeError, ValueError):
            trend_text = str(top_trend.get("query"))

    kind = str(trigger.get("kind") or "unknown")
    urgency = urgency_label(trigger.get("urgency"))

    parts = []
    if views is not None:
        parts.append(f"views={compact_num(views)}")
    if calls is not None:
        parts.append(f"calls={compact_num(calls)}")
    if ctr is not None:
        parts.append(f"ctr={ctr*100:.1f}%")
    if ctr_gap is not None:
        parts.append(f"peer_gap={ctr_gap*100:.1f}pp")
    merchant_metric_summary = ", ".join(parts)

    timing_signal = (
        payload.get("deadline_iso") or payload.get("due_date")
        or payload.get("stock_runs_out_iso") or payload.get("trial_date")
        or payload.get("date") or payload.get("match_time_iso")
        or trigger.get("expires_at")
    )
    timing_summary = month_day_label(str(timing_signal)) if timing_signal else ""

    reason_bits = [f"kind={kind}"]
    for key in ("metric", "delta_pct", "days_remaining", "days_since_expiry", "occurrences_30d"):
        if payload.get(key) is not None:
            reason_bits.append(f"{key}={payload[key]}")
    trigger_reason = ", ".join(reason_bits)

    # Fix W4: prefer live conversation history over stale merchant payload field
    if conversation is not None:
        conv_history = list(conversation.get("history") or [])
    else:
        conv_history = merchant.get("conversation_history") or []
    if not isinstance(conv_history, list):
        conv_history = []
    no_reply_streak = _count_no_reply_streak(conv_history)
    escalation_stage = min(no_reply_streak + 1, 3)

    last_conv = safe_first(conv_history[-1:], default={}) if conv_history else {}
    last_engagement = ""
    for field in ("engagement", "engagement_status", "status", "outcome"):
        val = last_conv.get(field)
        if val:
            last_engagement = str(val)
            break

    raw_search_count = (
        payload.get("search_count") or payload.get("searches_nearby")
        or payload.get("missed_searches") or payload.get("demand_count")
    )

    customer_identity = (customer.get("identity") or {}) if customer else {}
    relationship = (customer.get("relationship") or {}) if customer else {}
    preferences = (customer.get("preferences") or {}) if customer else {}

    return {
        "trigger_id": str(trigger.get("id") or ""),
        "trigger_kind": kind,
        "scope": str(trigger.get("scope") or ("customer" if customer else "merchant")),
        "trigger_intent": "problem" if kind in PROBLEM_KINDS else "opportunity",
        "urgency": urgency,
        "owner_name": owner_name(merchant),
        "merchant_state": {
            "merchant_id": str(merchant.get("merchant_id") or ""),
            "category_slug": str(merchant.get("category_slug") or category.get("slug") or ""),
            "merchant_name": merchant_name(merchant),
            "locality": merchant_locality(merchant),
            "city": str((merchant.get("identity") or {}).get("city") or ""),
            "views": views, "calls": calls, "ctr": ctr, "peer_ctr": peer_ctr,
            "ctr_gap": ctr_gap,
            "delta_views_pct_7d": delta_7d.get("views_pct"),
            "delta_calls_pct_7d": delta_7d.get("calls_pct"),
            "signals": merchant.get("signals") or [],
            "last_engagement": last_engagement,
            "active_offer": active_offer,
            "no_active_offer": active_offer is None,
            "escalation_stage": escalation_stage,
            "no_reply_streak": no_reply_streak,
        },
        "demand_signals": {
            "trend_summary": trend_text,
            "seasonal_trends": payload.get("trends") or [],
            "event": payload.get("festival") or payload.get("match"),
            "locality": merchant_locality(merchant),
            "raw_search_count": raw_search_count,
        },
        "timing_summary": timing_summary,
        "trigger_reason": trigger_reason,
        "merchant_metric_summary": merchant_metric_summary,
        "customer_state": {
            "name": str(customer_identity.get("name") or ""),
            "language_pref": str(customer_identity.get("language_pref") or ""),
            "last_visit": relationship.get("last_visit"),
            "visits_total": relationship.get("visits_total"),
            "preferred_slots": preferences.get("preferred_slots"),
        },
        "payload": payload,
    }


# ---------------------------------------------------------------------------
# Multi-strategy scoring (items 1, 5)
# ---------------------------------------------------------------------------

def score_strategies(action_type, signals):
    strategies = ACTION_STRATEGIES.get(action_type, ["category_playbook"])
    ms = signals.get("merchant_state") or {}
    demand = signals.get("demand_signals") or {}
    payload = signals.get("payload") or {}
    kind = signals.get("trigger_kind", "")
    active_offer = ms.get("active_offer")
    ctr_gap = ms.get("ctr_gap")
    calls_delta = ms.get("delta_calls_pct_7d")
    has_demand = bool(demand.get("raw_search_count") or demand.get("trend_summary") or demand.get("event"))
    urgency = signals.get("urgency", "low")

    scores = {s: 50.0 for s in strategies}
    for strategy in strategies:
        s = scores[strategy]
        if strategy == "offer_refresh":
            if active_offer: s += 35
            if has_demand: s += 20
            try:
                if ctr_gap is not None and float(ctr_gap) < -0.02: s += 15
            except (TypeError, ValueError): pass
        elif strategy == "metric_recovery":
            try:
                d = float(calls_delta or 0)
                if d < -0.2: s += 30
                elif d < -0.1: s += 15
            except (TypeError, ValueError): pass
            if not active_offer: s += 10
            try:
                if ctr_gap is not None and float(ctr_gap) < -0.03: s += 10
            except (TypeError, ValueError): pass
        elif strategy == "reputation_repair":
            if kind == "review_theme_emerged": s += 40
            try: s += min(int(payload.get("occurrences_30d") or 0) * 5, 30)
            except (TypeError, ValueError): pass
            if str(payload.get("trend") or "").lower() in ("increasing", "worsening"): s += 20
        elif strategy == "event_push":
            if has_demand: s += 40
            if demand.get("event"): s += 20
            if active_offer: s += 10
        elif strategy == "seasonal_offer":
            if not active_offer: s += 20
            if payload.get("days_until") is not None: s += 15
        elif strategy == "lapse_reactivation":
            try: s += min(int(payload.get("lapsed_customers_added_since_expiry") or 0) * 3, 30)
            except (TypeError, ValueError): pass
        elif strategy == "comeback_offer":
            if active_offer: s += 30
        elif strategy == "dormant_restart":
            if kind == "dormant_with_vera": s += 30
            try:
                if int(ms.get("views") or 0) > 200: s += 10
            except (TypeError, ValueError): pass
        elif strategy == "safety_response":
            if urgency == "high": s += 40
            if kind == "supply_alert": s += 30
        elif strategy == "deadline_checklist":
            try:
                d = int(payload.get("days_remaining") or 999)
                if d <= 2: s += 30
                elif d <= 7: s += 15
            except (TypeError, ValueError): pass
        elif strategy == "renewal_completion":
            if kind == "renewal_due": s += 35
        elif strategy == "counter_positioning":
            if payload.get("competitor_name"): s += 30
            if payload.get("their_offer"): s += 15
            if not active_offer: s += 10
        elif strategy == "verification_fix":
            if kind == "gbp_unverified": s += 50
        elif strategy == "milestone_amplify":
            if kind == "milestone_reached": s += 50
        elif strategy == "momentum_scale":
            if has_demand: s += 25
            if active_offer: s += 15
        scores[strategy] = s
    return scores


def choose_strategy(action_type, signals):
    scores = score_strategies(action_type, signals)
    return max(scores, key=lambda k: scores[k]) if scores else "category_playbook"


# ---------------------------------------------------------------------------
# Decision engine (items 1-6)
# ---------------------------------------------------------------------------

def _compute_risk(signals):
    factors = []
    ms = signals.get("merchant_state") or {}
    if signals.get("trigger_intent") == "problem": factors.append("problem_trigger")
    if signals.get("urgency") == "high": factors.append("high_urgency")
    if ms.get("no_active_offer"): factors.append("no_active_offer")
    try:
        if ms.get("delta_calls_pct_7d") is not None and float(ms["delta_calls_pct_7d"]) < -0.2:
            factors.append("calls_sharply_down")
    except (TypeError, ValueError): pass
    try:
        if ms.get("ctr_gap") is not None and float(ms["ctr_gap"]) < -0.03:
            factors.append("ctr_below_peer")
    except (TypeError, ValueError): pass
    if any("dormant" in str(s).lower() for s in (ms.get("signals") or [])):
        factors.append("dormant_signal")
    if "high_urgency" in factors or ("problem_trigger" in factors and len(factors) >= 2):
        level = "high"
    elif factors:
        level = "medium"
    else:
        level = "low"
    return {"risk_level": level, "risk_factors": factors}


def _compute_opportunity(signals):
    factors = []
    ms = signals.get("merchant_state") or {}
    demand = signals.get("demand_signals") or {}
    if ms.get("active_offer"): factors.append("active_offer")
    if demand.get("trend_summary"): factors.append("trend_signal")
    if demand.get("event"): factors.append("event_signal")
    if demand.get("raw_search_count"): factors.append("demand_volume")
    if signals.get("trigger_intent") == "opportunity": factors.append("opportunity_trigger")
    try:
        if ms.get("ctr_gap") is not None and float(ms["ctr_gap"]) > 0.02:
            factors.append("above_peer_ctr")
    except (TypeError, ValueError): pass
    level = "high" if len(factors) >= 3 else "medium" if factors else "low"
    return {"opportunity_level": level, "opportunity_factors": factors}


def decision_engine(signals):
    kind = str(signals.get("trigger_kind") or "unknown")
    action_type = TRIGGER_ACTION_MAP.get(kind, ACTION_FALLBACK)
    risk = _compute_risk(signals)
    opp = _compute_opportunity(signals)

    base_urg = {
        ACTION_COMPLIANCE: "high", ACTION_RECOVERY: "high", ACTION_EXECUTION: "high",
        ACTION_DEFENSE: "medium", ACTION_SEASONAL_PUSH: "medium", ACTION_WINBACK: "medium",
        ACTION_SCALE: "medium", ACTION_RETENTION: "medium", ACTION_LISTING_FIX: "medium",
        ACTION_EDUCATION: "low", ACTION_NURTURE: "low", ACTION_FALLBACK: "low",
    }.get(action_type, "low")

    urgency = urgency_max(base_urg, signals.get("urgency", "low"))
    if risk["risk_level"] == "high":
        urgency = "high"

    # Demand drives urgency (item 3)
    try:
        count = int((signals.get("demand_signals") or {}).get("raw_search_count") or 0)
        if count >= 100:
            urgency = "high"
        elif count >= 30:
            urgency = urgency_max(urgency, "medium")
    except (TypeError, ValueError): pass

    # Fix: flat/positive delta must not fire high-urgency recovery (contradictory hook+CTA)
    try:
        payload = signals.get("payload") or {}
        d = float(payload.get("delta_pct") if payload.get("delta_pct") is not None else -1)
        if d >= 0 and action_type == ACTION_RECOVERY and urgency == "high":
            urgency = "medium"
    except (TypeError, ValueError):
        pass

    strategy = choose_strategy(action_type, signals)

    # Merchant context forces strategy (item 4)
    ms = signals.get("merchant_state") or {}
    if action_type == ACTION_RECOVERY:
        if ms.get("active_offer") and opp["opportunity_level"] in {"high", "medium"}:
            strategy = "offer_refresh"
        elif not ms.get("active_offer") and risk["risk_level"] == "high":
            strategy = "metric_recovery"
    if action_type == ACTION_SEASONAL_PUSH and not ms.get("active_offer"):
        strategy = "seasonal_offer"
    if action_type == ACTION_DEFENSE and not ms.get("active_offer"):
        strategy = "locality_lock"

    cta = ACTION_CTA_TYPE.get(action_type, "binary_yes_no")
    scope = signals.get("scope", "merchant")
    if scope == "customer" and kind in {"recall_due", "appointment_tomorrow", "trial_followup"}:
        cta = "multi_choice_slot"
    if cta not in VALID_CTAS:
        cta = "binary_yes_no"

    return {"action_type": action_type, "urgency": urgency, "strategy": strategy,
            "cta": cta, "risk": risk, "opportunity": opp}


# ---------------------------------------------------------------------------
# Confidence tier (item 6)
# ---------------------------------------------------------------------------

_NUMERIC_PAYLOAD_FIELDS = (
    "delta_pct", "days_remaining", "search_count", "searches_nearby",
    "missed_searches", "demand_count", "occurrences_30d", "distance_km",
    "estimated_uplift_pct", "lapsed_customers_added_since_expiry",
    "renewal_amount", "trial_n", "days_since_expiry", "days_since_last_visit",
)

def confidence_tier(signals, decision):
    ms = signals.get("merchant_state") or {}
    payload = signals.get("payload") or {}
    has_metrics = ms.get("views") is not None or ms.get("calls") is not None
    has_ctr = ms.get("ctr") is not None
    has_peer = ms.get("peer_ctr") is not None
    has_name = bool(ms.get("merchant_name"))
    numeric_payload = any(payload.get(k) is not None for k in _NUMERIC_PAYLOAD_FIELDS)
    if has_metrics and has_ctr and has_peer and has_name and numeric_payload:
        return 1
    if has_metrics and has_name:
        return 2
    return 3


def _readiness_ok(signals):
    return bool((signals.get("merchant_state") or {}).get("merchant_name"))


# ---------------------------------------------------------------------------
# Why-now hook — always specific, never generic (items 8, 9, 10, 11)
# ---------------------------------------------------------------------------

def why_now_hook(signals, tier):
    payload = signals.get("payload") or {}
    ms = signals.get("merchant_state") or {}
    demand = signals.get("demand_signals") or {}
    kind = signals.get("trigger_kind", "")
    slug = ms.get("category_slug", "")
    locality = ms.get("locality", "your area")
    owner = signals.get("owner_name", "")
    prefix = f"{owner}, " if owner else ""
    calls = ms.get("calls")
    views = ms.get("views")
    ctr = ms.get("ctr")
    peer_ctr = ms.get("peer_ctr")
    ctr_gap = ms.get("ctr_gap")
    calls_delta = ms.get("delta_calls_pct_7d")

    # 1. Real search volume
    try:
        count = int(demand.get("raw_search_count") or 0)
        if count > 0:
            service = payload.get("service") or payload.get("query") or _slug_metric(slug, "calls")
            return f"{prefix}{compact_num(count)} people searched for {service} in {locality} recently and you're missing most of it."
    except (TypeError, ValueError): pass

    # 2. Metric + delta — handle edge cases (item 10)
    metric = payload.get("metric")
    delta_val = payload.get("delta_pct")
    if metric is not None and delta_val is not None:
        try:
            d = float(delta_val)
            metric_label = _slug_metric(slug, metric)
            # Positive delta in a perf_dip trigger
            if kind in {"perf_dip", "seasonal_perf_dip"} and d >= 0:
                if ctr_gap is not None and float(ctr_gap) < -0.02:
                    gap_pp = abs(float(ctr_gap)) * 100
                    return f"{prefix}{metric_label.capitalize()} held but CTR is {gap_pp:.1f}pp below {locality} peers — impressions aren't converting."
                return f"{prefix}Metrics steady but the {locality} category is shifting — staying ahead requires one active move."
            direction = "dropped" if d < 0 else "spiked"
            pct = as_pct(abs(d))
            return f"{prefix}Your {metric_label} {direction} {pct} this week in {locality}."
        except (TypeError, ValueError): pass

    # 3. CTR gap vs peer (locality in body — item 11)
    if ctr is not None and peer_ctr is not None and ctr_gap is not None:
        try:
            gap_pp = float(ctr_gap) * 100
            if abs(gap_pp) >= 0.5:
                label = _ctr_label(slug)
                direction = "below" if gap_pp < 0 else "above"
                return (
                    f"{prefix}Your {label} sits at {ctr*100:.1f}% in {locality} — "
                    f"{abs(gap_pp):.1f}pp {direction} local peers at {peer_ctr*100:.1f}%."
                )
        except (TypeError, ValueError): pass

    # 4. Calls + views together
    if calls is not None and views is not None and tier <= 2:
        metric_label = _slug_metric(slug, "calls")
        try:
            d = float(calls_delta or 0) * 100
            if d < -10:
                return f"{prefix}{compact_num(calls)} {metric_label} this month, down {abs(d):.0f}% week-on-week."
        except (TypeError, ValueError): pass
        return f"{prefix}{compact_num(views)} people saw your listing this month and {compact_num(calls)} called — that gap is closeable."

    # 5. Days remaining
    days_remaining = payload.get("days_remaining")
    if days_remaining is not None:
        try:
            d = int(days_remaining)
            if d <= 0: return f"{prefix}This cycle already expired — acting now prevents a service gap."
            if d == 1: return f"{prefix}One day left on this window — last chance before it closes."
            return f"{prefix}Only {d} day(s) left on this cycle."
        except (TypeError, ValueError): pass

    # 6. Review pattern
    occ = payload.get("occurrences_30d")
    if occ is not None:
        theme = payload.get("theme") or "a recurring theme"
        try:
            return f"{prefix}\"{theme}\" appeared {int(occ)} time(s) in recent reviews — it's becoming a pattern in {locality}."
        except (TypeError, ValueError): pass

    # 7. Competitor
    competitor = payload.get("competitor_name")
    if competitor:
        dist = payload.get("distance_km")
        dist_txt = f" {dist}km away" if dist else " nearby"
        return f"{prefix}{competitor} just opened{dist_txt} and is now indexing in the same {locality} search pool."

    # 8. Days since — consequence inferred
    days_since = payload.get("days_since_expiry") or payload.get("days_since_last_merchant_message")
    if days_since is not None:
        return f"{prefix}It's been {days_since} day(s) — momentum in {locality} doesn't hold itself."

    # 9. Event/festival
    event = demand.get("event") or payload.get("festival")
    if event:
        days_until = payload.get("days_until")
        if days_until is not None:
            return f"{prefix}{event} is {days_until} day(s) out — the {locality} search window is already open."
        return f"{prefix}{event} is approaching and the {locality} search window is live."

    # 10. Trend
    trend = demand.get("trend_summary")
    if trend:
        return f"{prefix}{trend.capitalize()} — that demand is moving through {locality} right now."

    # 11. Category consequence (tier 3 — never expose missing data, items 35, 38)
    slug_consequence = {
        "dentists":        f"{prefix}Dental recall windows in {locality} are competitive — first-mover listings capture most bookings.",
        "restaurants":     f"{prefix}Order intent in {locality} peaks in 90-minute windows — being visible at the right moment is everything.",
        "salons":          f"{prefix}Weekend booking slots in {locality} fill fast — late listings get leftover traffic.",
        "gyms":            f"{prefix}Trial-to-membership conversions in {locality} peak early in the week.",
        "pharmacies":      f"{prefix}Refill windows are narrow — missed timing means lost repeat customers.",
        "spas":            f"{prefix}Appointment slots in {locality} get booked in advance — listing without availability loses the comparison.",
        "opticians":       f"{prefix}Eye-test bookings in {locality} cluster around weekends — early visibility captures that intent.",
        "diagnostic_labs": f"{prefix}Test booking decisions in {locality} are made fast — the listing with an offer gets the call.",
        "bakeries":        f"{prefix}Order intent in {locality} spikes around events — early listing presence captures same-day demand.",
        "jewellers":       f"{prefix}Visit intent in {locality} is high around occasions — a visible offer converts browsers into inquiries.",
        "clinics":         f"{prefix}Appointment demand in {locality} fills fast on short notice — first-response listings win.",
        "coaching":        f"{prefix}Inquiry intent in {locality} peaks at enrollment windows — being visible early captures the batch.",
        "beauty_parlours": f"{prefix}Booking slots in {locality} fill for weekends — late listings get the cancellation slots.",
    }
    kind_suffix = _KIND_SUFFIX.get(kind, "")
    if slug in slug_consequence:
        base = slug_consequence[slug].rstrip(".")
        return f"{base}{(' ' + kind_suffix) if kind_suffix else '.'}"
    metric_label = _slug_metric(slug, "calls")
    fallback = f"{prefix}The {locality} {metric_label} window is open"
    return f"{fallback}{(' ' + kind_suffix) if kind_suffix else ' — your next action determines whether you lead or follow.'}"


# ---------------------------------------------------------------------------
# Helpers: suppression, rationale, template
# ---------------------------------------------------------------------------

def build_suppression_key(trigger, decision, signals):
    merchant_id = str((signals.get("merchant_state") or {}).get("merchant_id", "m"))
    kind = str(signals.get("trigger_kind") or trigger.get("kind") or "unknown")
    trigger_id = str(trigger.get("id") or "")
    if trigger_id:
        return f"supp|m={merchant_id}|t={trigger_id}|k={kind}"
    return deterministic_id("supp", merchant_id, kind)


def build_rationale(decision, signals, tier, strategy_scores=None):
    """Narrative rationale explaining WHY this action, WHY now, and WHY this strategy."""
    risk = decision.get("risk") or {}
    opp = decision.get("opportunity") or {}
    ms = signals.get("merchant_state") or {}
    strat = decision.get("strategy", "")
    kind = signals.get("trigger_kind", "unknown")
    urgency = decision.get("urgency", "medium")
    action = decision.get("action_type", "")
    merchant_nm = ms.get("merchant_name", "merchant")
    locality = ms.get("locality", "the area")
    parts = []

    # Why this trigger warrants action now
    risk_level = risk.get("risk_level", "low")
    opp_level = opp.get("opportunity_level", "low")
    risk_factors = risk.get("risk_factors") or []
    if urgency == "high" and risk_level == "high":
        parts.append(
            f"Acting now on {kind.replace('_', ' ')} for {merchant_nm}: "
            f"{', '.join(risk_factors) or 'escalated state'} make delay costly."
        )
    elif urgency == "high":
        parts.append(
            f"High-urgency {kind.replace('_', ' ')} for {merchant_nm} in {locality} — "
            f"the window is time-limited."
        )
    else:
        parts.append(f"{kind.replace('_', ' ').capitalize()} signal for {merchant_nm} in {locality}.")

    # Why this action type
    action_why = {
        "recovery": "Metrics show a decline that is still within the reversible window this week.",
        "defense": f"Competitor entry threatens {locality} search-share — a counter-move in 48h limits first-week indexing advantage.",
        "winback": "Lapsed cohort is inside the 45-day high-conversion window; waiting past it cuts response rate by half.",
        "scale": f"Current momentum in {locality} is exploitable — spikes decay if not amplified within 48h.",
        "seasonal_push": f"Event demand is live in {locality} and the first-mover listings capture disproportionate share.",
        "compliance": "Non-compliance risks listing suspension — preventable if acted on before the deadline.",
        "retention": "Scheduled touchpoint: missing it erodes trust and increases churn probability.",
        "listing_fix": "Unverified listing loses clicks to verified competitors in the same search pool.",
        "education": "Actionable category signal — converting it to a post now beats the 48h digest decay window.",
    }
    if action in action_why:
        parts.append(action_why[action])

    # Why this strategy over alternatives
    if strat and strategy_scores:
        top = sorted(strategy_scores.items(), key=lambda x: -x[1])[:2]
        runner_up = top[1][0] if len(top) > 1 else None
        score_gap = (top[0][1] - top[1][1]) if len(top) > 1 else 0
        if runner_up and score_gap < 15:
            parts.append(
                f"Strategy '{strat}' chosen (score {top[0][1]:.0f}) over '{runner_up}' ({top[1][1]:.0f}) — "
                f"close call; both are viable."
            )
        else:
            parts.append(f"Strategy '{strat}' is the dominant path given current merchant state.")

    # Confidence and data quality
    metrics_txt = signals.get("merchant_metric_summary") or "limited metrics available"
    parts.append(f"Tier {tier} confidence — {metrics_txt}.")

    return " ".join(parts)


def build_template_name(action_type, scope):
    at = re.sub(r"[^a-z0-9_]+", "_", action_type.lower()).strip("_")
    side = "customer" if scope == "customer" else "merchant"
    return f"vera_{side}_{at}_v2"


# ---------------------------------------------------------------------------
# Merchant-scope compose functions
# Each produces a COMPLETE 2-3 sentence message with action-forward CTA.
# No "Recommended action:" label. No fixed structure. (items 16-24)
# ---------------------------------------------------------------------------

def compose_perf_dip(category, merchant, trigger, signals, tier, stage):
    ms = signals.get("merchant_state") or {}
    slug = ms.get("category_slug", "")
    payload = signals.get("payload") or {}
    offer = ms.get("active_offer")
    locality = ms.get("locality", "your area")
    mid = ms.get("merchant_id", "")
    ctr_gap = ms.get("ctr_gap")
    delta_val = payload.get("delta_pct")
    strategy = signals.get("_strategy", "metric_recovery")  # injected by compose()
    views = ms.get("views")
    calls = ms.get("calls")

    # Edge: positive delta in a perf_dip trigger
    if delta_val is not None:
        try:
            if float(delta_val) >= 0:
                hook = why_now_hook(signals, tier)
                # Fix hidden failure: check is not None, not truthiness (ctr_gap=0.0 is valid)
                if ctr_gap is not None and float(ctr_gap) < -0.02:
                    action = "Impressions exist but aren't converting — a tighter offer angle fixes the click gap."
                    return _join(hook, action, _cta("recovery", offer, locality, urgency="medium", stage=stage, merchant_id=mid, kind="perf_dip"))
                return _join(hook, _cta("defend", offer, locality, stage=stage, merchant_id=mid, kind="perf_dip"))
        except (TypeError, ValueError):
            pass

    # Gap B: stage-aware escalation prefix for repeat non-responders
    streak = ms.get("no_reply_streak", 0)
    callback = _stage_callback(stage, "performance dip", no_reply_streak=streak)

    hook = why_now_hook(signals, tier)

    # Gap C: demand-gap synthesis — high demand + no offer = explicit visibility gap (early return)
    demand = signals.get("demand_signals") or {}
    try:
        search_count = int(demand.get("raw_search_count") or 0)
    except (TypeError, ValueError):
        search_count = 0
    if search_count >= 100 and not offer:
        service = payload.get("service") or _slug_metric(slug, "calls")
        gap_action = (
            f"{compact_num(search_count)} people searched for {service} in {locality} recently "
            f"— you have no offer live, so they cannot find you in results. "
            f"One offer post today makes you visible to all of them."
        )
        uplift = _estimate_uplift_calls(views, ms.get("ctr"), ms.get("peer_ctr"))
        cta_line = _cta("recovery", None, locality, urgency="high", stage=stage,
                        merchant_id=mid, kind="perf_dip", no_reply_streak=streak, uplift=uplift)
        return _join(callback, hook, gap_action, cta_line)

    # Gap A: metric narrative injection for tier-1 data when hook isn't already metric-specific
    if tier == 1:
        narrative = _metric_narrative(ms, slug)
        if narrative and "%" not in hook:
            hook = f"{hook} {narrative}"

    # Compute uplift for ROI framing (Gap D)
    uplift = _estimate_uplift_calls(views, ms.get("ctr"), ms.get("peer_ctr"))

    # Strategy drives the action sentence (W1 + Fix 5 for zero-metric coherence)
    has_metric_context = (views is not None or calls is not None or delta_val is not None)
    if strategy == "offer_refresh" and offer:
        if not has_metric_context:
            # Fix 5: no metric baseline — frame as proactive capture, not recovery
            if slug == "dentists":
                action = f'Launching "{offer}" now captures new dental searchers in {locality} — without a post, they find whoever is visible first.'
            elif slug == "restaurants":
                action = f'A peak-hour push on "{offer}" this week captures order intent from new {locality} searchers actively comparing options.'
            else:
                action = f'Pushing "{offer}" now captures {locality} searchers currently looking — without a post this week, that demand goes elsewhere.'
        else:
            if slug == "dentists":
                action = f'"{offer}" is already set up — a 7-day GBP post + recall message pushes it in front of the {locality} searches you\'re missing.'
            elif slug == "restaurants":
                action = f'"{offer}" is live — a peak-hour push around it for 7 days is the fastest path back to full order volume.'
            elif slug == "salons":
                action = f'"{offer}" can fill empty slots this weekend — one post now, track bookings per day.'
            else:
                action = f'"{offer}" is already live — one locality-targeted post now converts that into the calls you\'ve dropped.'

    elif strategy == "metric_recovery":
        metric_label = _slug_metric(slug, payload.get("metric", "calls"))
        if views and calls:
            try:
                actual_ctr = calls / views
                gap_txt = f" ({actual_ctr*100:.1f}% CTR on {compact_num(views)} views)"
            except ZeroDivisionError:
                gap_txt = ""
        else:
            gap_txt = ""
        uplift_txt = f" Closing the gap to {locality} peer average adds ~{uplift} {_slug_metric(slug, 'calls')} this month." if uplift else ""
        if slug == "dentists":
            action = f"The drop in {metric_label}{gap_txt} means fewer consultations booked this week — one price-anchored recall post today starts the recovery.{uplift_txt}"
        elif slug == "restaurants":
            action = f"The drop in {metric_label}{gap_txt} means order intent isn't landing — one meal-combo price post at lunch+dinner hours captures it.{uplift_txt}"
        elif slug == "pharmacies":
            action = f"The drop in {metric_label}{gap_txt} means refill customers are going elsewhere — one delivery-convenience post recovers them.{uplift_txt}"
        else:
            action = f"The drop in {metric_label}{gap_txt} is recoverable — one tightly targeted post this week reverses the trend before it compounds.{uplift_txt}"

    elif strategy == "reputation_repair":
        action = "The CTR gap suggests review signals may be suppressing clicks — a response script + one 'improvement' GBP post addresses both the ranking and the trust deficit."

    else:
        if slug == "dentists":
            action = "One price-anchored consultation post today pulls consult calls back — no offer live means this is the fastest fix."
        elif slug == "restaurants":
            action = "One meal-combo price point in a GBP post captures the lunch+dinner windows you're currently missing."
        elif slug == "salons":
            action = "One service+price combo post this weekend fills the empty slots before they're gone."
        elif slug == "gyms":
            action = "A free-trial hook post recovers trial calls — without one there's no entry point for new members."
        elif slug == "pharmacies":
            action = "One refill+delivery combo post restores call volume this week."
        else:
            action = "One price-anchored post this week closes the gap before it compounds."

    # Gap E: cross-signal synthesis — connect competitor signal to call drop
    m_signals = ms.get("signals") or []
    competitor_signal = next(
        (s for s in m_signals if "competitor" in str(s).lower() or "new_listing" in str(s).lower()),
        None,
    )
    if competitor_signal and strategy in ("metric_recovery", "offer_refresh"):
        sig_label = str(competitor_signal).replace("_", " ")
        action += f" This drop coincides with {sig_label} — it's a positioning gap, not a product problem."

    # Pass uplift to CTA only when action sentence doesn't already embed it (metric_recovery embeds it)
    cta_uplift = None if strategy == "metric_recovery" else uplift
    cta_line = _cta("recovery", offer, locality, urgency="high", stage=stage,
                    merchant_id=mid, kind="perf_dip", no_reply_streak=streak, uplift=cta_uplift)
    return _join(callback, hook, action, cta_line)


def compose_perf_spike(category, merchant, trigger, signals, tier, stage):
    ms = signals.get("merchant_state") or {}
    payload = signals.get("payload") or {}
    offer = ms.get("active_offer")
    locality = ms.get("locality", "your area")
    mid = ms.get("merchant_id", "")
    slug = ms.get("category_slug", "")
    metric = _slug_metric(slug, payload.get("metric", "calls"))
    delta_txt = as_pct(payload.get("delta_pct"), sign=True) or "up"
    likely = payload.get("likely_driver")
    owner = signals.get("owner_name", "")

    hook = f"{owner}, {metric} {delta_txt} this week in {locality}.".lstrip(", ")
    driver = f"Likely driver: {likely}." if likely else ""

    # Peer comparison — addresses Decision Quality gap (why amplify *now* vs baseline)
    ctr_gap = ms.get("ctr_gap")
    peer_ctr = ms.get("peer_ctr")
    if ctr_gap is not None and peer_ctr is not None:
        try:
            gap_pp = float(ctr_gap) * 100
            if gap_pp > 0:
                gap_txt = f"{abs(gap_pp):.1f}pp above {locality} peer average ({peer_ctr*100:.1f}%)"
                peer_note = f"This spike puts your CTR {gap_txt} — amplifying now locks in rank before competitors catch up."
            else:
                peer_note = ""
        except (TypeError, ValueError):
            peer_note = ""
    else:
        peer_note = _get_social_proof(slug, "perf_spike", locality, gap_txt=delta_txt)

    action = (f'Scaling now with a 5-day variant of "{offer}" locks in momentum before it decays.'
              if offer else "One locality-specific post before the weekend doubles down on this window — spikes don't wait.")
    cta_line = _cta("scale", offer, locality, stage=stage, merchant_id=mid, kind="perf_spike")
    if peer_note:
        return _join(hook, driver, peer_note, action, cta_line)
    return _join(hook, driver, action, cta_line)


def compose_research_digest(category, merchant, trigger, signals, tier, stage):
    ms = signals.get("merchant_state") or {}
    offer = ms.get("active_offer")
    locality = ms.get("locality", "your area")
    mid = ms.get("merchant_id", "")
    owner = signals.get("owner_name", "")
    cat_display = category.get("display_name") or ms.get("category_slug", "category")
    item = resolve_digest_item(category, trigger) or {}
    title = str(item.get("title") or "")
    trial_n = item.get("trial_n")
    segment = item.get("patient_segment") or item.get("segment")
    source = str(item.get("source") or "category digest")

    if trial_n and title:
        hook = f"{owner}, {compact_num(trial_n)}-sample {cat_display} signal{' for ' + str(segment).replace('_',' ') if segment else ''} in {locality}: {title.lower()}.".lstrip(", ")
    elif title:
        hook = f"{owner}, new {cat_display} signal in {locality}: {title} ({source}).".lstrip(", ")
    else:
        trend = (signals.get("demand_signals") or {}).get("trend_summary")
        if trend:
            hook = f"{owner}, category signal: {trend} — that's actionable now in {locality}.".lstrip(", ")
        else:
            hook = f"{owner}, latest {cat_display} digest has an actionable update for {locality}.".lstrip(", ")

    action = f'I can turn this into a patient-ready post for "{offer}" and one GBP update.' if offer else "I can turn this into one patient-facing post and one GBP update — 2 minutes."

    # "Asking the merchant" lever — brief's top missed compulsion (#7)
    ask = "Which of your patient groups would this be most relevant for — I'll tailor the post to them."

    cta_line = _cta("digest", offer, locality, stage=stage, merchant_id=mid, kind="research_digest")
    # Lead with action → ask → CTA to keep it under 3 sentences
    return _join(hook, action, ask, cta_line)


def compose_regulation_change(category, merchant, trigger, signals, tier, stage):
    ms = signals.get("merchant_state") or {}
    mid = ms.get("merchant_id", "")
    slug = ms.get("category_slug", "")
    locality = ms.get("locality", "your area")
    owner = signals.get("owner_name", "")
    item = resolve_digest_item(category, trigger) or {}
    title = str(item.get("title") or "a regulation update")
    source = str(item.get("source") or "regulatory update")
    payload = signals.get("payload") or {}
    deadline = payload.get("deadline_iso") or trigger.get("expires_at")
    deadline_txt = f"by {month_day_label(str(deadline))}" if deadline else "now"

    hook = f"{owner}, compliance heads-up: {title} ({source}).".lstrip(", ")
    consequence = f"Acting {deadline_txt} keeps your listing compliant — lapses can suspend rank for 5-10 days."

    # Social proof — "other practices in your area are already acting" (compulsion lever #3)
    social = _get_social_proof(slug, "regulation_change", locality, n=3)

    # Reciprocity framing — "I flagged this specifically for you" (compulsion lever #6)
    reciprocity = f"Flagging this specifically for {merchant_name(merchant)} — your category and locality make this directly applicable."

    cta_line = _cta("compliance", stage=stage, merchant_id=mid, kind="regulation_change")
    if social:
        return _join(hook, consequence, social, cta_line)
    return _join(hook, consequence, reciprocity, cta_line)


def compose_renewal_due(category, merchant, trigger, signals, tier, stage):
    ms = signals.get("merchant_state") or {}
    mid = ms.get("merchant_id", "")
    owner = signals.get("owner_name", "")
    payload = signals.get("payload") or {}
    plan = payload.get("plan") or (merchant.get("subscription") or {}).get("plan") or "current"
    amt = payload.get("renewal_amount")
    amount_txt = f" at ₹{compact_num(amt)}" if amt is not None else ""
    days = payload.get("days_remaining")
    try:
        d = int(days) if days is not None else None
    except (TypeError, ValueError):
        d = None

    if d is None:   timing = f"Your {plan} renewal window is open."
    elif d < 0:     timing = f"Your {plan} plan expired {abs(d)} day(s) ago."
    elif d == 0:    timing = f"Your {plan} plan expires today."
    elif d == 1:    timing = f"Your {plan} plan expires tomorrow."
    else:           timing = f"Your {plan} plan is due in {d} day(s)."

    hook = f"{owner}, {timing}".lstrip(", ")
    consequence = f"Renewing{amount_txt} keeps verified rank live — a lapse takes 5-10 days to recover."
    cta_line = _cta("compliance", urgency="high", stage=stage, merchant_id=mid, kind="renewal_due")
    return _join(hook, consequence, cta_line)


def compose_competitor_opened(category, merchant, trigger, signals, tier, stage):
    ms = signals.get("merchant_state") or {}
    slug = ms.get("category_slug", "")
    offer = ms.get("active_offer")
    locality = ms.get("locality", "your area")
    mid = ms.get("merchant_id", "")
    payload = signals.get("payload") or {}
    competitor = payload.get("competitor_name") or "a new competitor"
    distance = payload.get("distance_km")
    their_offer = payload.get("their_offer")

    # Demand-gap synthesis: high search volume + no active offer = explicit invisibility framing
    try:
        search_count = int((signals.get("demand_signals") or {}).get("raw_search_count") or 0)
    except (TypeError, ValueError):
        search_count = 0
    if search_count >= 100 and not offer:
        service = _slug_metric(slug, "calls")
        hook_gap = (
            f"{compact_num(search_count)} people are searching for {service} in {locality} — "
            f"without an offer live, those comparing {competitor} and your listing default to the one with a visible deal."
        )
        cta_line = _cta("defend", None, locality, urgency="high", stage=stage, merchant_id=mid, kind="competitor_opened")
        return _join(hook_gap, cta_line)

    dist_txt = f" {distance}km away" if distance is not None else " nearby"
    comp_context = f"{competitor} opened{dist_txt} and is indexing in {locality} search."
    if their_offer:
        comp_context += f' They\'re advertising "{their_offer}".'
    # why_now_hook cascades: demand count → metric delta → CTR gap → competitor (step 7)
    # If demand signals exist, they surface first; competitor context is appended as secondary sentence
    demand_hook = why_now_hook(signals, tier)
    hook = comp_context if competitor in demand_hook else _join(demand_hook, comp_context)

    if slug == "dentists":
        action = (f'A counter-offer on "{offer}" with one local GBP post this week protects your recall share.' if offer
                  else "One price-anchored consultation post this week appears in the same searches before their listing gains reviews.")
    elif slug == "restaurants":
        action = (f'A locality-targeted push on "{offer}" in 48h captures searchers comparing both listings.' if offer
                  else "One combo-price GBP post in 48h shows up first while their listing has zero reviews.")
    else:
        action = (f'A counter-positioning offer on "{offer}" this week limits their first-week indexing advantage.' if offer
                  else "Without a counter-offer live this week, searchers comparing options default to the newer listing.")

    cta_line = _cta("defend", offer, locality, urgency="high", stage=stage, merchant_id=mid, kind="competitor_opened")
    return _join(hook, action, cta_line)


def compose_festival(category, merchant, trigger, signals, tier, stage):
    ms = signals.get("merchant_state") or {}
    slug = ms.get("category_slug", "")
    offer = ms.get("active_offer")
    locality = ms.get("locality", "your area")
    mid = ms.get("merchant_id", "")
    payload = signals.get("payload") or {}
    festival = payload.get("festival") or "the upcoming festival"
    days = payload.get("days_until")
    days_txt = f"{days} day(s) out" if days is not None else "approaching"

    urgency = signals.get("_urgency", "medium")
    # why_now_hook cascades: demand count → metrics → CTR → event/festival (step 9)
    # All slugs route through here so demand signals are always surfaced
    hook = why_now_hook(signals, tier)

    if slug == "pharmacies":
        # Pharmacy framing: wellness/OTC demand — no early return (Fix 4: was bypassing urgency)
        if offer:
            action = f'"{offer}" as a delivery festive reminder before the window opens captures OTC and refill demand.'
        else:
            action = "One wellness-combo + delivery post before the window opens captures the festive OTC demand spike."
        cta_line = _cta("recovery" if urgency == "high" else "digest", offer, locality,
                        urgency=urgency, stage=stage, merchant_id=mid, kind="festival_upcoming")
    elif offer:
        action = f'Relaunching "{offer}" with festive framing now captures first-mover share before the peak.'
        cta_line = _cta("festival", offer, locality, urgency=urgency, stage=stage, merchant_id=mid, kind="festival_upcoming")
    else:
        action = "Listings that launch a service+price offer 48h before the window typically capture 2x the festive traffic."
        cta_line = _cta("festival", offer, locality, urgency=urgency, stage=stage, merchant_id=mid, kind="festival_upcoming")
    return _join(hook, action, cta_line)


def compose_ipl(category, merchant, trigger, signals, tier, stage):
    ms = signals.get("merchant_state") or {}
    offer = ms.get("active_offer")
    locality = ms.get("locality", "your area")
    mid = ms.get("merchant_id", "")
    payload = signals.get("payload") or {}
    match = payload.get("match") or "today's match"
    match_time = payload.get("match_time_iso")
    label = month_day_label(str(match_time)) if match_time else "today"
    is_weeknight = payload.get("is_weeknight")

    # why_now_hook surfaces demand/CTR signals first; falls through to event (the match) if none
    demand_hook = why_now_hook(signals, tier)
    match_line = f"IPL: {match} ({label}) shifts {locality} order search in the 90-min pre-match window."
    hook = match_line if (match and match in demand_hook) else _join(demand_hook, match_line)
    channel = ("Weekend matches drive home-watch group orders — delivery positioning outperforms dine-in today." if is_weeknight is False
               else "Weeknight matches lift last-minute group orders in the 90-min pre-match window." if is_weeknight is True else "")
    action = (f'A match-window push on "{offer}" timed to the pre-match hour captures this spike.' if offer
              else "One match-timed combo post (service+price) in the pre-match hour is the highest-leverage action right now.")
    cta_line = _cta("festival", offer, locality, urgency="high", stage=stage, merchant_id=mid, kind="ipl_match_today")
    return _join(hook, channel, action, cta_line)


def compose_review_theme(category, merchant, trigger, signals, tier, stage):
    ms = signals.get("merchant_state") or {}
    slug = ms.get("category_slug", "")
    mid = ms.get("merchant_id", "")
    locality = ms.get("locality", "your area")
    owner = signals.get("owner_name", "")
    payload = signals.get("payload") or {}
    theme = str(payload.get("theme") or "a recurring complaint")
    occ = payload.get("occurrences_30d")
    trend = payload.get("trend")
    is_worsening = str(trend or "").lower() in ("increasing", "worsening")

    occ_txt = f"{occ} time(s)" if occ else "repeatedly"
    trend_prefix = "Trend is worsening — " if is_worsening else ""

    # Category-specific consequence with concrete numbers and rank/rating impact
    if slug == "dentists":
        hook = f"{owner}, \"{theme}\" appears {occ_txt} in your {locality} reviews.".lstrip(", ")
        if occ and int(occ) >= 3:
            consequence = (
                f"{trend_prefix}Three or more occurrences is the threshold where Google's algorithm "
                f"starts suppressing star-score display — a response script + one corrective GBP post "
                f"stops the aggregation before your rating drops visibly."
            )
        else:
            consequence = (
                f"{trend_prefix}Unaddressed review themes pull down aggregate ratings for dental practices "
                f"— a response script + one corrective GBP post contains it before it crosses the suppression threshold."
            )
    elif slug == "restaurants":
        # Delivery-specific framing when theme is delivery-related
        is_delivery = any(w in theme.lower() for w in ("delivery", "late", "cold", "wait", "slow"))
        if is_delivery:
            hook = f"{owner}, \"{theme}\" appears {occ_txt} in your {locality} delivery reviews.".lstrip(", ")
            consequence = (
                f"{trend_prefix}Each unanswered delivery complaint reduces your aggregate rating by ~0.02 stars "
                f"— at 4+ occurrences Google Maps deprioritises your listing in local food searches. "
                f"A templated reply to each review + one corrective GBP post stops the rating slide before it crosses that threshold."
            )
        else:
            hook = f"{owner}, \"{theme}\" appears {occ_txt} in your {locality} reviews.".lstrip(", ")
            consequence = (
                f"{trend_prefix}Recurring review themes drop discovery rank for restaurants by "
                f"suppressing the star-rating display — a response script + one corrective post "
                f"stops the pattern before it shows in your monthly rank report."
            )
    elif slug == "salons":
        hook = f"{owner}, \"{theme}\" appears {occ_txt} in your {locality} reviews.".lstrip(", ")
        consequence = (
            f"{trend_prefix}In salons, unaddressed review themes reduce booking confidence for new clients — "
            f"a response template + one GBP update shifts the visible narrative within 48h."
        )
    elif slug == "pharmacies":
        hook = f"{owner}, \"{theme}\" appears {occ_txt} in your {locality} reviews.".lstrip(", ")
        consequence = (
            f"{trend_prefix}In pharmacies, recurring service complaints suppress trust signals and reduce "
            f"refill call rate — a response script + corrective post contains it now."
        )
    else:
        hook = f"{owner}, \"{theme}\" appears {occ_txt} in your {locality} reviews.".lstrip(", ")
        consequence = (
            f"{trend_prefix}At this frequency it surfaces in aggregate review signals and starts to "
            f"suppress discovery rank — a response script + corrective GBP post stops it now."
        )

    # Social proof — directly addresses the "what should I do?" decision quality gap
    social = _get_social_proof(slug, "review_theme", locality, n=3)
    cta_line = _cta("defend", urgency="high", stage=stage, merchant_id=mid, kind="review_theme_emerged")

    # Assemble: hook → consequence → [social proof] → CTA (no interrogative ask — declare, don't ask)
    if social:
        return _join(hook, consequence, social, cta_line)
    return _join(hook, consequence, cta_line)


def compose_milestone(category, merchant, trigger, signals, tier, stage):
    ms = signals.get("merchant_state") or {}
    slug = ms.get("category_slug", "")
    offer = ms.get("active_offer")
    locality = ms.get("locality", "your area")
    mid = ms.get("merchant_id", "")
    owner = signals.get("owner_name", "")
    payload = signals.get("payload") or {}
    metric = payload.get("metric") or "milestone"
    value_now = payload.get("value_now")
    milestone_value = payload.get("milestone_value")

    if metric == "review_count" and value_now and milestone_value:
        noun = _customer_noun(slug)
        hook = (
            f"{owner}, {compact_num(int(value_now))} {noun[:-1] if noun.endswith('s') else noun} reviews in {locality} "
            f"put you above most {slug.replace('_',' ')} listings — "
            f"{compact_num(int(milestone_value))}+ reviews unlocks Priority Listing status."
        ).lstrip(", ")
    elif value_now and milestone_value:
        label = metric.replace("_", " ")
        try:
            hook = (
                f"{owner}, you've hit {compact_num(int(value_now))} {label} in {locality} — "
                f"{compact_num(int(milestone_value))} is the threshold for featured placement."
            ).lstrip(", ")
        except (TypeError, ValueError):
            hook = f"{owner}, you've crossed a visibility milestone in {locality} on {label}.".lstrip(", ")
    else:
        label = metric.replace("_", " ")
        hook = f"{owner}, you've crossed a {label} milestone in {locality} — your listing is now in the top visibility tier.".lstrip(", ")

    # Peer percentile framing — addresses "Category Fit" and "Engagement" gaps
    peer_stats = category.get("peer_stats") or {}
    avg_reviews = peer_stats.get("avg_reviews")
    if value_now and avg_reviews:
        try:
            ratio = float(value_now) / float(avg_reviews)
            if ratio >= 2.0:
                pct = 10
            elif ratio >= 1.5:
                pct = 20
            elif ratio >= 1.0:
                pct = 35
            else:
                pct = 50
            peer_line = _get_social_proof(slug, "milestone", locality, pct=pct)
        except (TypeError, ValueError):
            peer_line = ""
    else:
        peer_line = _get_social_proof(slug, "milestone", locality, pct=30)

    action = "A celebration post + review-reply template now converts this momentum into visible social proof before the signal fades."

    cta_line = _cta("scale", offer, locality, stage=stage, merchant_id=mid, kind="milestone_reached")
    if peer_line:
        return _join(hook, peer_line, action, cta_line)
    return _join(hook, action, cta_line)


def compose_winback_eligible(category, merchant, trigger, signals, tier, stage):
    ms = signals.get("merchant_state") or {}
    slug = ms.get("category_slug", "")
    offer = ms.get("active_offer")
    locality = ms.get("locality", "your area")
    mid = ms.get("merchant_id", "")
    owner = signals.get("owner_name", "")
    payload = signals.get("payload") or {}
    expiry_days = payload.get("days_since_expiry")
    lapse = payload.get("lapsed_customers_added_since_expiry")
    strategy = signals.get("_strategy", "lapse_reactivation")

    streak = ms.get("no_reply_streak", 0)
    callback = _stage_callback(stage, "winback opportunity", no_reply_streak=streak)
    noun = _customer_noun(slug)
    lapse_txt = f"{lapse} lapsed {noun}" if lapse else f"a lapsed {noun} cohort"
    expiry_txt = f" ({expiry_days} days since your last offer)" if expiry_days else ""
    hook = f"{owner}, {lapse_txt} have accumulated{expiry_txt} — this is the easiest recoverable revenue you have.".lstrip(", ")

    # Strategy drives the action sentence
    if strategy == "comeback_offer" and offer:
        if slug == "dentists":
            action = f'"{offer}" as a lapsed-patient recall incentive is the fastest reactivation path — 20-35% convert in the first 30-day window.'
        elif slug == "salons":
            action = f'"{offer}" with one reserved slot option converts 25-40% of lapsed {noun} back in the first pass.'
        else:
            action = f'"{offer}" sent directly to this cohort this week reaches them before the 45-day cold window closes.'
    elif strategy == "lapse_reactivation":
        if slug == "dentists":
            action = f"Targeted recall outreach with a one-time checkup incentive converts 20-35% of lapsed {noun} in the first 30-day window — after that the rate drops sharply."
        elif slug == "restaurants":
            action = f"A direct comeback message to this cohort this week reaches your {noun} while the relationship is still warm — waiting past 45 days cuts response rate by half."
        elif slug == "salons":
            action = f"A lapsed-{noun} reactivation offer with one slot option converts 25-40% back — the first 30 days post-lapse are the highest-leverage window."
        else:
            action = f"A targeted reactivation message this week reaches your {noun} before the 45-day cold window closes — after that re-acquisition costs 4x more."
    else:  # dormant_restart
        action = (
            ('A 2-message reactivation plan built around "' + offer + '" can restart momentum this week.' if offer
             else "A 2-message reactivation plan with one concrete service+price anchor can restart momentum this week.")
        )

    social = _get_social_proof(slug, "winback", locality)
    cta_context = f"all {lapse} {noun}" if lapse else noun
    cta_line = _cta("winback", offer, locality, stage=stage, merchant_id=mid, kind="winback_eligible",
                    context=cta_context)

    if social:
        return _join(callback, hook, action, social, cta_line)
    return _join(callback, hook, action, cta_line)


def compose_gbp_unverified(category, merchant, trigger, signals, tier, stage):
    ms = signals.get("merchant_state") or {}
    mid = ms.get("merchant_id", "")
    locality = ms.get("locality", "your area")
    owner = signals.get("owner_name", "")
    payload = signals.get("payload") or {}
    uplift = as_pct(payload.get("estimated_uplift_pct")) or "15-25%"

    hook = f"{owner}, your listing shows up in {locality} searches but you're losing the click to verified competitors.".lstrip(", ")
    consequence = f"Verification (~20 min) delivers ~{uplift} visibility uplift — every day unverified, those clicks go to whoever verified first."
    cta_line = _cta("verify", stage=stage, merchant_id=mid, kind="gbp_unverified")
    return _join(hook, consequence, cta_line)


def compose_dormant(category, merchant, trigger, signals, tier, stage):
    ms = signals.get("merchant_state") or {}
    slug = ms.get("category_slug", "")
    offer = ms.get("active_offer")
    locality = ms.get("locality", "your area")
    mid = ms.get("merchant_id", "")
    owner = signals.get("owner_name", "")
    payload = signals.get("payload") or {}
    days = payload.get("days_since_last_merchant_message")
    views = ms.get("views")
    calls = ms.get("calls")
    strategy = signals.get("_strategy", "dormant_restart")

    streak = ms.get("no_reply_streak", 0)
    callback = _stage_callback(stage, "dormant reactivation", no_reply_streak=streak)

    if views and calls:
        hook = f"{owner}, {compact_num(views)} views and {compact_num(calls)} calls in 30 days with no outreach — that audience is already warm.".lstrip(", ")
    elif views:
        hook = f"{owner}, {compact_num(views)} people saw your listing this month and got nothing from you.".lstrip(", ")
    else:
        hook = f"{owner}, {days if days else 'a while'} day(s) of silence in {locality} — the search window stays open regardless.".lstrip(", ")

    # Fix W1: strategy drives the action sentence, not just offer presence
    if strategy == "comeback_offer" and offer:
        if slug == "dentists":
            action = f'"{offer}" sent to this warm audience this week converts 20-35% of views into booked consultations.'
        elif slug == "salons":
            action = f'"{offer}" with a slot option turns passive viewers into confirmed bookings — one message, done.'
        else:
            action = f'"{offer}" targeted at this warm audience converts their interest before they find an alternative.'
    elif strategy == "dormant_restart":
        if slug == "dentists":
            action = "Two short recall messages this week converts some of that visibility into booked consultations."
        elif slug == "restaurants":
            action = "Two targeted posts this week activates that warm audience before they book elsewhere."
        else:
            action = ('Two short outreach messages converts that warm attention into calls using "' + offer + '".' if offer
                      else "Two short outreach messages converts that warm attention into calls.")
    else:  # lapse_reactivation or fallback
        action = ('A 2-message reactivation sequence built around "' + offer + '" restarts momentum this week.' if offer
                  else "A 2-message sequence with one concrete service+price anchor restarts momentum this week.")

    cta_line = _cta("winback", offer, locality, stage=stage, merchant_id=mid, kind="dormant_with_vera")
    # Social proof — addresses category fit gap for dormant messages
    social = _get_social_proof(slug, "dormant_with_vera", locality, n=3)
    if social:
        return _join(callback, hook, action, social, cta_line)
    return _join(callback, hook, action, cta_line)


def compose_active_planning(category, merchant, trigger, signals, tier, stage):
    ms = signals.get("merchant_state") or {}
    mid = ms.get("merchant_id", "")
    owner = signals.get("owner_name", "")
    payload = signals.get("payload") or {}
    topic = str(payload.get("intent_topic") or "the plan you asked for").replace("_", " ")
    city = str((merchant.get("identity") or {}).get("city") or "your city")
    hook = f"{owner}, action mode for {topic} in {city}:".lstrip(", ")
    action = "Pricing tiers, one launch message, and a 7-day rollout checklist ready on CONFIRM."
    return _join(hook, action, "Reply CONFIRM — I send the full plan immediately.")


def compose_supply_alert(category, merchant, trigger, signals, tier, stage):
    ms = signals.get("merchant_state") or {}
    mid = ms.get("merchant_id", "")
    owner = signals.get("owner_name", "")
    payload = signals.get("payload") or {}
    molecule = payload.get("molecule") or "listed molecule"
    batches = payload.get("affected_batches") or []
    if batches:
        shown = batches[:3]
        extra = len(batches) - 3
        batch_txt = ", ".join(str(x) for x in shown)
        if extra > 0:
            batch_txt += f" and {extra} more"
    else:
        batch_txt = "your affected inventory"
    hook = f"{owner}, urgent: {molecule} recall — batches {batch_txt} require immediate shelf check.".lstrip(", ")
    consequence = "Customer notification for impacted recent purchases must go out before end of day."
    return _join(hook, consequence, "CONFIRM and I draft the customer message + pickup-replacement workflow now.")


def compose_category_seasonal(category, merchant, trigger, signals, tier, stage):
    ms = signals.get("merchant_state") or {}
    slug = ms.get("category_slug", "")
    offer = ms.get("active_offer")
    locality = ms.get("locality", "your area")
    mid = ms.get("merchant_id", "")
    owner = signals.get("owner_name", "")
    payload = signals.get("payload") or {}
    season = payload.get("season") or "this season"
    trends = payload.get("trends") or []
    trend_short = ", ".join(str(x) for x in trends[:2]) if trends else "demand mix has shifted"

    hook = f"{owner}, {season} shift in {locality}: {trend_short}.".lstrip(", ")
    if slug == "restaurants":
        action = "A 14-day menu re-prioritisation + one updated GBP post captures this before competitors adjust."
    elif slug == "pharmacies":
        action = "A 14-day shelf re-prioritisation for the seasonal demand shift maximises the refill window."
    else:
        action = "A 14-day offer re-prioritisation captures this window before the demand peak passes."

    cta_line = _cta("digest", offer, locality, stage=stage, merchant_id=mid, kind="category_seasonal")
    return _join(hook, action, cta_line)


def compose_curious_ask(category, merchant, trigger, signals, tier, stage):
    ms = signals.get("merchant_state") or {}
    locality = ms.get("locality", "your area")
    owner = signals.get("owner_name", "")
    hook = f"{owner}, quick 30-second signal from {locality}:".lstrip(", ")
    action = "What service demand felt strongest this week? One line from you = a GBP post + customer reply snippet from me."
    return _join(hook, action)


def compose_fallback_inferred(signals, tier, stage):
    """Always actionable fallback — never exposes missing data. (items 36-38)"""
    ms = signals.get("merchant_state") or {}
    offer = ms.get("active_offer")
    locality = ms.get("locality", "your area")
    mid = ms.get("merchant_id", "")
    kind = signals.get("trigger_kind", "action")
    hook = why_now_hook(signals, tier)
    action = (f'I can run a targeted recovery push using "{offer}" now.' if offer
              else f"I can run a targeted outreach for {locality} searchers in the next 2 minutes.")
    cta_line = _cta("", offer, locality, stage=stage, merchant_id=mid, kind=kind)
    return _join(hook, action, cta_line)


# ---------------------------------------------------------------------------
# Customer-scope compose functions
# ---------------------------------------------------------------------------

def compose_customer_recall(category, merchant, trigger, customer):
    merchant_nm = merchant_name(merchant)
    cname = str((customer.get("identity") or {}).get("name") or "there")
    payload = trigger.get("payload") or {}
    due = payload.get("due_date") or payload.get("stock_runs_out_iso")
    due_txt = month_day_label(str(due)) if due else "soon"
    offer = pick_active_offer(merchant, category)
    slots = payload.get("available_slots") or []
    slot_text = " or ".join(str(s.get("label") or s.get("iso")) for s in slots[:2]) if slots else ""

    # W7 fix: surface peer context and trend to personalise the urgency signal
    peer_ctr = find_peer_ctr(category)
    perf = merchant.get("performance") or {}
    ctr_val = None
    try:
        ctr_val = float(perf.get("ctr"))
    except (TypeError, ValueError):
        pass
    above_peer = (ctr_val is not None and peer_ctr is not None and ctr_val > peer_ctr)
    trend_signals = category.get("trend_signals") or []
    top_trend = safe_first(trend_signals) or {}
    trend_note = ""
    if top_trend.get("query") and top_trend.get("delta_yoy") is not None:
        try:
            delta = float(top_trend["delta_yoy"]) * 100
            if delta > 10:
                trend_note = f" Demand for {top_trend['query']} is up {delta:.0f}% this year."
        except (TypeError, ValueError):
            pass

    if customer_language_pref(customer) == "hi-en":
        lines = [f"Hi {cname}, {merchant_nm} yahan."]
        lines.append(f"Aapka recall/refill due {due_txt} ke around hai.")
        if above_peer: lines.append("Hum area mein top-rated providers mein se hain.")
        if trend_note: lines.append(trend_note.strip())
        if slot_text: lines.append(f"Slot options: {slot_text}.")
        if offer: lines.append(f"Current offer: {offer}.")
        cta_close = "Reply 1/2 for slot, ya preferred time bhej dijiye." if slot_text else "Reply YES and I'll confirm a slot that works for you."
        lines.append(cta_close)
    else:
        lines = [f"Hi {cname}, this is {merchant_nm}."]
        lines.append(f"Your recall/refill window is due around {due_txt}.")
        if above_peer: lines.append("We're one of the top-rated providers in your area.")
        if trend_note: lines.append(trend_note.strip())
        if slot_text: lines.append(f"Available slots: {slot_text}.")
        if offer: lines.append(f"Current offer: {offer}.")
        cta_close = "Reply 1/2 for a slot, or share a better time." if slot_text else "Reply YES and I'll confirm a slot that works for you."
        lines.append(cta_close)
    return _join(*lines)


def compose_customer_lapse(category, merchant, trigger, customer):
    cname = str((customer.get("identity") or {}).get("name") or "there")
    owner = owner_name(merchant)
    kind = str(trigger.get("kind") or "customer_lapsed_soft")
    payload = trigger.get("payload") or {}
    days = payload.get("days_since_last_visit")
    focus = payload.get("previous_focus")
    offer = pick_active_offer(merchant, category)

    # W7 fix: add peer CTR comparison and seasonal trend to personalise the message
    peer_ctr = find_peer_ctr(category)
    perf = merchant.get("performance") or {}
    ctr_val = None
    try:
        ctr_val = float(perf.get("ctr"))
    except (TypeError, ValueError):
        pass
    above_peer = (ctr_val is not None and peer_ctr is not None and ctr_val > peer_ctr)
    trend_signals = category.get("trend_signals") or []
    top_trend = safe_first(trend_signals) or {}
    season_note = ""
    if top_trend.get("query") and top_trend.get("delta_yoy") is not None:
        try:
            delta = float(top_trend["delta_yoy"]) * 100
            if delta > 10:
                season_note = f"Demand for {top_trend['query']} is high right now — good time to restart."
        except (TypeError, ValueError):
            pass

    lines = [f"Hi {cname}, {owner} here from {merchant_name(merchant)}."]
    if days is not None: lines.append(f"It's been about {days} days — easy to pick back up.")
    if focus: lines.append(f"Last time your focus was {str(focus).replace('_', ' ')}.")
    if above_peer: lines.append("We're consistently rated among the top in your area.")
    if season_note: lines.append(season_note)
    if offer: lines.append(f"We can restart with {offer} this week.")
    else: lines.append("One easy first step is all it takes this week.")
    lines.append("Reply YES — I'll hold a no-pressure slot for you." if kind == "customer_lapsed_hard"
                 else "Reply YES for two quick slot options.")
    return _join(*lines)


def compose_customer_trial_followup(category, merchant, trigger, customer):
    cname = str((customer.get("identity") or {}).get("name") or "there")
    payload = trigger.get("payload") or {}
    trial_date = payload.get("trial_date")
    options = payload.get("next_session_options") or []
    option_txt = ", ".join(str(opt.get("label") or opt.get("iso")) for opt in options[:2])
    lines = [f"Hi {cname}, thanks for trying {merchant_name(merchant)}."]
    if trial_date: lines.append(f"Your trial was on {month_day_label(str(trial_date))}.")
    if option_txt: lines.append(f"Next session options: {option_txt}.")
    lines.append("Reply 1 to confirm, or share another preferred slot.")
    return _join(*lines)


def compose_customer_chronic_refill(category, merchant, trigger, customer):
    cname = str((customer.get("identity") or {}).get("name") or "there")
    payload = trigger.get("payload") or {}
    mols = payload.get("molecule_list") or []
    molecules = ", ".join(str(m) for m in mols[:3]) if mols else "your regular medicines"
    run_out = payload.get("stock_runs_out_iso")
    run_out_txt = month_day_label(str(run_out)) if run_out else "soon"
    offer = pick_active_offer(merchant, category)
    if customer_language_pref(customer) == "hi-en":
        lines = [f"Namaste {cname}, {merchant_name(merchant)} se reminder."]
        lines.append(f"Aapki medicines ({molecules}) {run_out_txt} tak khatam ho sakti hain.")
        if offer: lines.append(f"Applicable offer: {offer}.")
        lines.append("Reply CONFIRM for refill + delivery, ya dosage change ho to bata dijiye.")
    else:
        lines = [f"Hi {cname}, refill reminder from {merchant_name(merchant)}."]
        lines.append(f"Your medicines ({molecules}) may run out around {run_out_txt}.")
        if offer: lines.append(f"Current offer: {offer}.")
        lines.append("Reply CONFIRM for refill + delivery, or let us know if dosage changed.")
    return _join(*lines)


def compose_customer_appointment(category, merchant, trigger, customer):
    cname = str((customer.get("identity") or {}).get("name") or "there")
    payload = trigger.get("payload") or {}
    slot = payload.get("slot") or payload.get("appointment_time") or payload.get("iso")
    slot_txt = month_day_label(str(slot)) if slot else "tomorrow"
    return _join(f"Hi {cname}, reminder from {merchant_name(merchant)}.",
                 f"Your appointment is confirmed for {slot_txt}.",
                 "Reply 1 to confirm, 2 to reschedule.")


def compose_customer_wedding_followup(category, merchant, trigger, customer):
    cname = str((customer.get("identity") or {}).get("name") or "there")
    payload = trigger.get("payload") or {}
    days = payload.get("days_to_wedding")
    step = payload.get("next_step_window_open")
    step_txt = str(step).replace("_", " ") if step else "next prep step"
    return _join(f"Hi {cname}, bridal follow-up from {merchant_name(merchant)}.",
                 f"{'Wedding in ' + str(days) + ' days — ' if days else ''}{step_txt} is the best move now.",
                 "Reply YES to block your preferred slot.")


# ---------------------------------------------------------------------------
# Main compose()
# ---------------------------------------------------------------------------

def _build_trigger_context(signals):
    """Compact fact clause for reply personalisation, e.g. 'calls down 22%, competitor QuickDent nearby'."""
    ms = signals.get("merchant_state") or {}
    payload = signals.get("payload") or {}
    parts = []
    delta = payload.get("delta_pct")
    if delta is not None:
        try:
            d = float(delta) * 100
            if abs(d) >= 1:
                parts.append(f"calls {'down' if d < 0 else 'up'} {abs(d):.0f}%")
        except (TypeError, ValueError):
            pass
    comp = payload.get("competitor_name")
    if comp:
        dist = payload.get("distance_km")
        parts.append(f"competitor {comp}{f' {dist}km away' if dist else ' nearby'}")
    offer = ms.get("active_offer")
    if offer:
        parts.append(f'offer "{offer}" live')
    try:
        search = int((signals.get("demand_signals") or {}).get("raw_search_count") or 0)
        if search >= 50:
            parts.append(f"{compact_num(search)} searches this week")
    except (TypeError, ValueError):
        pass
    return ", ".join(parts) if parts else ""


def compose(category, merchant, trigger, customer=None, conversation=None):
    """Deterministically compose the next outbound message payload."""
    signals = normalize_signals(category, merchant, trigger, customer, conversation=conversation)
    decision = decision_engine(signals)
    # Inject resolved strategy and urgency so compose_* functions can branch on them
    signals["_strategy"] = decision.get("strategy", "")
    signals["_urgency"] = decision.get("urgency", "medium")
    strategy_scores = score_strategies(decision["action_type"], signals)

    scope = str(signals.get("scope") or ("customer" if customer else "merchant"))
    kind = str(signals.get("trigger_kind") or "unknown")
    send_as = "merchant_on_behalf" if scope == "customer" else "vera"
    ms = signals.get("merchant_state") or {}
    offer = ms.get("active_offer")
    locality = ms.get("locality", "your area")
    mid = ms.get("merchant_id", "")
    stage = int(ms.get("escalation_stage", 1))
    tier = confidence_tier(signals, decision)

    # Readiness: merchant name required — fallback always actionable (items 36-38)
    if not _readiness_ok(signals):
        hook = why_now_hook(signals, 3)
        body = _join(hook, "I can run a default outreach push in 2 minutes.", "Reply YES to proceed or STOP to pause.")
        cta = "binary_yes_no"
        rationale = build_rationale(decision, signals, tier=3, strategy_scores=strategy_scores)
        return {"body": body, "cta": cta, "send_as": send_as,
                "suppression_key": build_suppression_key(trigger, decision, signals),
                "rationale": rationale,
                "template_name": build_template_name(decision["action_type"], scope),
                "template_params": [signals.get("owner_name", "there"), kind, locality]}

    # Customer scope (item 15: no scope leaks)
    if scope == "customer" and customer:
        cta_map = {
            "recall_due": "multi_choice_slot", "appointment_tomorrow": "multi_choice_slot",
            "trial_followup": "multi_choice_slot", "chronic_refill_due": "binary_confirm_cancel",
        }
        cta = cta_map.get(kind, "binary_yes_no")
        if cta not in VALID_CTAS:
            cta = "binary_yes_no"

        fn_map = {
            "recall_due": compose_customer_recall,
            "customer_lapsed_soft": compose_customer_lapse,
            "customer_lapsed_hard": compose_customer_lapse,
            "trial_followup": compose_customer_trial_followup,
            "chronic_refill_due": compose_customer_chronic_refill,
            "appointment_tomorrow": compose_customer_appointment,
            "wedding_package_followup": compose_customer_wedding_followup,
        }
        fn = fn_map.get(kind)
        if fn:
            body = fn(category, merchant, trigger, customer)
        else:
            cname = str((customer.get("identity") or {}).get("name") or "there")
            body = _join(f"Hi {cname}, this is {merchant_name(merchant)}.",
                         "We have an update for you — reply YES for details.")

    # Merchant scope
    else:
        cta = decision.get("cta", "binary_yes_no")
        if cta not in VALID_CTAS:
            cta = "binary_yes_no"

        kw = dict(category=category, merchant=merchant, trigger=trigger, signals=signals, tier=tier, stage=stage)
        router = {
            "perf_dip":               compose_perf_dip,
            "seasonal_perf_dip":      compose_perf_dip,
            "perf_spike":             compose_perf_spike,
            "research_digest":        compose_research_digest,
            "cde_opportunity":        compose_research_digest,
            "regulation_change":      compose_regulation_change,
            "renewal_due":            compose_renewal_due,
            "competitor_opened":      compose_competitor_opened,
            "festival_upcoming":      compose_festival,
            "ipl_match_today":        compose_ipl,
            "review_theme_emerged":   compose_review_theme,
            "milestone_reached":      compose_milestone,
            "winback_eligible":       compose_winback_eligible,
            "dormant_with_vera":      compose_dormant,
            "customer_lapsed_soft":   compose_winback_eligible,
            "customer_lapsed_hard":   compose_winback_eligible,
            "gbp_unverified":         compose_gbp_unverified,
            "category_seasonal":      compose_category_seasonal,
            "active_planning_intent": compose_active_planning,
            "supply_alert":           compose_supply_alert,
            "curious_ask_due":        compose_curious_ask,
        }
        fn = router.get(kind)
        body = fn(**kw) if fn else compose_fallback_inferred(signals, tier, stage)

    # Final guardrails
    if body and body[-1] not in (".", "!", "?"):
        body += "."

    rationale = build_rationale(decision, signals, tier=tier, strategy_scores=strategy_scores)
    first_param = str((customer or {}).get("identity", {}).get("name") or signals.get("owner_name") or "there")
    second_param = offer or str(decision.get("action_type") or kind or "update")
    third_param = locality

    return {
        "body": body, "cta": cta, "send_as": send_as,
        "suppression_key": build_suppression_key(trigger, decision, signals),
        "rationale": rationale,
        "template_name": build_template_name(decision["action_type"], scope),
        "template_params": [first_param, second_param, third_param],
        "trigger_context": _build_trigger_context(signals),
    }


# ---------------------------------------------------------------------------
# Trigger-aware, history-aware reply handler (items 28-31)
# ---------------------------------------------------------------------------

_EXECUTION_STEPS = {
    "perf_dip":             "Sending: (1) recovery campaign copy, (2) one GBP post, (3) 7-day call-tracking checklist.",
    "seasonal_perf_dip":    "Sending: (1) seasonal recovery message, (2) category offer hook, (3) 7-day plan.",
    "gbp_unverified":       "Sending: (1) step-by-step verification guide, (2) listing optimisation checklist.",
    "festival_upcoming":    "Sending: (1) festive campaign copy, (2) offer framing, (3) GBP post draft.",
    "ipl_match_today":      "Sending: (1) match-window offer copy, (2) timing guide, (3) GBP post.",
    "competitor_opened":    "Sending: (1) counter-positioning offer, (2) locality-targeted message copy.",
    "winback_eligible":     "Sending: (1) comeback offer draft, (2) outreach copy for lapsed customers.",
    "renewal_due":          "Sending: (1) renewal confirmation steps, (2) service-continuity checklist.",
    "review_theme_emerged": "Sending: (1) response script, (2) corrective GBP post draft.",
    "milestone_reached":    "Sending: (1) celebration post, (2) review-reply template.",
    "regulation_change":    "Sending: (1) 3-point compliance checklist, (2) deadline summary.",
    "research_digest":      "Sending: (1) 4-line merchant summary, (2) one patient-facing post draft.",
    "supply_alert":         "Sending: (1) customer notification message, (2) pickup-replacement workflow.",
    "category_seasonal":    "Sending: (1) 14-day assortment note, (2) shelf/menu suggestion.",
    "dormant_with_vera":    "Sending: (1) 2-message reactivation plan, (2) offer anchor from your latest metrics.",
    "active_planning_intent":"Sending: (1) pricing tiers, (2) launch message, (3) 7-day rollout checklist.",
    "perf_spike":           "Sending: (1) momentum-scaling campaign copy, (2) 5-day variant schedule.",
}


def simple_reply_from_context(conversation, message, merchant=None, category=None):
    """History-aware, trigger-specific reply policy (items 28-31)."""
    msg = (message or "").strip()
    low = msg.lower()
    history = conversation.get("history") or []
    kind = str(conversation.get("trigger_kind") or "action")
    ctx = str(conversation.get("trigger_context") or "")
    ctx_clause = f" ({ctx})" if ctx else ""
    merchant_turns = sum(1 for h in history if str(h.get("from") or "").lower() in {"merchant", "customer"})
    bot_turns = sum(1 for h in history if str(h.get("from") or "").lower() == "bot")
    turn_depth = merchant_turns + bot_turns
    no_reply_streak = _count_no_reply_streak(history)

    # Opt-out — always snooze (item 30)
    stop_words = ["stop", "unsubscribe", "dont message", "don't message", "not interested",
                  "spam", "useless", "go away", "never contact", "remove me",
                  "already told", "leave me alone", "do not contact"]
    if any(w in low for w in stop_words):
        return {"action": "end", "snooze_merchant": True,
                "rationale": "Merchant opted out — closing and applying 30-day snooze."}

    # Deferral — checked BEFORE commit so compound-intent ("yes, remind me tomorrow") defers correctly
    deferral_words = ["later", "busy", "after", "tomorrow", "call back", "remind", "next week"]
    if any(w in low for w in deferral_words):
        return {"action": "wait", "wait_seconds": 1800, "rationale": "Merchant requested time; backing off 30 min."}

    # Auto-reply
    auto_patterns = ["thank you for contacting", "will respond shortly", "our team will contact",
                     "away right now", "auto reply", "auto-reply", "business account"]
    if any(p in low for p in auto_patterns):
        count = int(conversation.get("auto_reply_count", 0)) + 1
        conversation["auto_reply_count"] = count
        if count == 1:
            return {"action": "send",
                    "body": "Looks like an auto-reply — when you're free, just reply YES and I'll continue where we left off.",
                    "cta": "binary_yes_no", "rationale": "Auto-reply detected; bridge message sent."}
        if count == 2:
            return {"action": "wait", "wait_seconds": 86400,
                    "rationale": "Repeated auto-reply; waiting 24h."}
        return {"action": "end", "rationale": "Auto-reply loop exhausted; closing cleanly."}

    # Commit — trigger-specific execution (item 29); deferral already cleared above
    commit_words = ["let's do it", "lets do it", "ok do it", "yes do", "proceed",
                    "go ahead", "confirm", "what's next", "whats next"]
    if any(w in low for w in commit_words) or low.strip() in {"yes", "y", "ok", "sure", "done"}:
        owner_n = owner_name(merchant or {}) if merchant else ""
        steps = _EXECUTION_STEPS.get(kind, f"Preparing the full plan for {kind.replace('_', ' ')}.")
        body = _join(f"On it{', ' + owner_n if owner_n else ''}.", steps, "Reply CONFIRM to dispatch, STOP to hold.")
        return {"action": "send", "body": body, "cta": "binary_confirm_cancel",
                "rationale": f"Commitment detected; execution mode for {kind}."}

    # Out-of-scope
    if any(t in low for t in ["gst", "income tax", "itr", "legal", "ca "]):
        return {"action": "send",
                "body": "Tax and legal questions are outside my lane — I'll leave those to your CA. On this thread, I can send the campaign rollout now. YES?",
                "cta": "binary_yes_no", "rationale": "Out-of-scope deflected; steered back to active task."}

    # Draft request
    if any(w in low for w in ["send", "draft", "template", "details", "how", "what", "share", "show"]):
        steps = _EXECUTION_STEPS.get(kind, f"Draft for {kind.replace('_', ' ')}.")
        body = _join(f"On it{ctx_clause}. {steps}", "Reply YES for a Hindi-English variant or CONFIRM to send as-is.")
        return {"action": "send", "body": body, "cta": "binary_yes_no",
                "rationale": "Draft requested; trigger-specific deliverable sent."}

    # Message evolution — escalate on no-reply streak (item 31)
    if no_reply_streak >= 3:
        return {"action": "end", "rationale": "No response after 3 bot messages; closing to avoid fatigue."}
    if no_reply_streak == 2:
        return {"action": "send", "body": "Last touch from me on this — YES to execute, STOP to close.",
                "cta": "binary_yes_no", "rationale": "Two prior messages unanswered; final short escalation."}
    if turn_depth >= 6 and no_reply_streak >= 1:
        return {"action": "wait", "wait_seconds": 43200,
                "rationale": "Deep conversation, merchant not responding; 12h cooldown."}
    if no_reply_streak == 1 or turn_depth >= 4:
        return {"action": "send", "body": f"One step left{ctx_clause} — YES to send the draft, STOP to pause this thread.",
                "cta": "binary_yes_no", "rationale": "Prior message unanswered; shortened and made direct."}

    return {"action": "send", "body": f"Understood — reply YES to send the ready-to-use draft now{ctx_clause}.",
            "cta": "binary_yes_no", "rationale": "Neutral continuation; single binary choice."}