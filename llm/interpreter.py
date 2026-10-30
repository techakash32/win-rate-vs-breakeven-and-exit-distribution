# llm/interpreter.py
import asyncio
import json
import logging
import os
import time

from config import settings

log = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a trade-rationale interpreter for a deterministic 3-bucket gate.
You are given the gate's final decision and the exact bucket states that produced it.
You MUST echo the gate's decision exactly. You are explaining, not deciding.
Return JSON matching the schema. Keep rationale under 280 characters.
Mention the specific bucket reasons that caused the decision.
Bucket C (dealer overlay) is informational unless it says it vetoed."""

# Groq strict mode: every property required, additionalProperties false.
OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "decision": {"type": "string", "enum": ["LONG", "SHORT", "STAND_ASIDE"]},
        "rationale": {"type": "string"},
        "key_factors": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["decision", "rationale", "key_factors"],
    "additionalProperties": False,
}


class TokenBudget:
    def __init__(self):
        self.minute_start = time.time()
        self.day_start = time.time()
        self.rpm_count = 0
        self.rpd_count = 0
        self.tpm_tokens = 0
        self.day_tokens = 0

    def _roll(self):
        now = time.time()
        if now - self.minute_start >= 60:
            self.minute_start = now
            self.rpm_count = 0
            self.tpm_tokens = 0
        if now - self.day_start >= 86400:
            self.day_start = now
            self.rpd_count = 0
            self.day_tokens = 0

    def can_call(self, est_tokens):
        self._roll()
        return (
            self.rpm_count < settings.GROQ_MAX_RPM
            and self.rpd_count < settings.GROQ_MAX_RPD
            and self.tpm_tokens + est_tokens <= settings.GROQ_MAX_TPM
            and self.day_tokens + est_tokens <= settings.GROQ_MAX_TOKENS_PER_DAY
        )

    def record(self, tokens):
        self._roll()
        self.rpm_count += 1
        self.rpd_count += 1
        self.tpm_tokens += tokens
        self.day_tokens += tokens


class GroqInterpreter:
    """
    Async, budgeted narrator. Runs AFTER the gate has decided and never
    influences it. Drops requests when the budget is exhausted.
    """

    def __init__(self):
        self._client = None
        self._budget = TokenBudget()
        self._queue = asyncio.Queue(maxsize=settings.GROQ_QUEUE_MAX)
        self._stats = {
            "queued": 0, "completed": 0, "budget_skipped": 0,
            "errors": 0, "decision_mismatch": 0,
        }

    def enabled(self) -> bool:
        return bool(os.environ.get("GROQ_API_KEY"))

    def _get_client(self):
        if self._client is None:
            from groq import AsyncGroq              # lazy: optional dependency
            self._client = AsyncGroq(api_key=os.environ["GROQ_API_KEY"])
        return self._client

    def submit(self, payload) -> bool:
        """Non-blocking. Drops the oldest item if the queue is full."""
        if not self.enabled():
            return False
        try:
            self._queue.put_nowait(payload)
        except asyncio.QueueFull:
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                self._queue.put_nowait(payload)
            except asyncio.QueueFull:
                return False
        self._stats["queued"] += 1
        return True

    async def run(self):
        while True:
            payload = await self._queue.get()
            try:
                await self._interpret(payload)
            except Exception as exc:
                self._stats["errors"] += 1
                log.warning("Interpreter error: %s: %s", type(exc).__name__, exc)

    async def _interpret(self, payload):
        est = settings.GROQ_EST_TOKENS_PER_CALL
        if not self._budget.can_call(est):
            self._stats["budget_skipped"] += 1
            log.info("LLM budget exhausted - skipping rationale for %s",
                     payload.get("decision"))
            return

        client = self._get_client()
        user_msg = json.dumps(payload, separators=(",", ":"))

        try:
            resp = await asyncio.wait_for(
                client.chat.completions.create(
                    model=settings.GROQ_MODEL,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": user_msg},
                    ],
                    response_format={
                        "type": "json_schema",
                        "json_schema": {
                            "name": "trade_rationale",
                            "strict": True,
                            "schema": OUTPUT_SCHEMA,
                        },
                    },
                    temperature=0.0,
                    # reasoning tokens count against this, so keep it generous
                    max_completion_tokens=settings.GROQ_MAX_COMPLETION_TOKENS,
                    extra_body={"reasoning_effort": settings.GROQ_REASONING_EFFORT},
                ),
                timeout=settings.GROQ_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            self._stats["errors"] += 1
            log.warning("Groq timeout after %ss", settings.GROQ_TIMEOUT_SECONDS)
            return

        usage = getattr(resp, "usage", None)
        used = getattr(usage, "total_tokens", None) or est
        self._budget.record(int(used))
        self._stats["completed"] += 1

        raw = resp.choices[0].message.content or ""
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            self._stats["errors"] += 1
            finish = getattr(resp.choices[0], "finish_reason", "?")
            log.warning("Groq returned non-JSON (finish_reason=%s): %r", finish, raw[:120])
            return

        gate_decision = payload.get("decision")
        llm_decision = parsed.get("decision")
        mismatch = gate_decision != llm_decision
        if mismatch:
            self._stats["decision_mismatch"] += 1
            log.warning("LLM echoed %s but gate decided %s - gate stands",
                        llm_decision, gate_decision)

        record = {
            "ts": time.time(),
            "gate_decision": gate_decision,
            "llm_decision": llm_decision,
            "mismatch": mismatch,
            "rationale": parsed.get("rationale", ""),
            "key_factors": (parsed.get("key_factors") or [])[:3],
            "tokens": int(used),
            "bucket_snapshot": payload,
        }
        folder = os.path.dirname(settings.LLM_LOG_PATH)
        if folder:
            os.makedirs(folder, exist_ok=True)
        with open(settings.LLM_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

        log.info("LLM [%s] %s - %s", "OK" if not mismatch else "MISMATCH",
                 llm_decision, parsed.get("rationale", "")[:160])

    def stats(self):
        return dict(self._stats)