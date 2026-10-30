# scripts/check_groq.py
"""
One-call dry run of the EXACT request the interpreter sends (strict schema,
reasoning_effort via extra_body, same token cap). Run this before the long test:

    python -m scripts.check_groq

PASS  = non-empty, schema-valid JSON.
FAIL  = empty / non-JSON output (usually reasoning tokens ate the budget, i.e.
        reasoning_effort was not honored) or an API error.
"""
import asyncio
import json
import os
import sys

from pathlib import Path

try:
    from dotenv import load_dotenv
    env_path = Path(__file__).resolve().parents[1] / ".env"
    load_dotenv(dotenv_path=env_path, override=True)
except ImportError:
    pass

from config import settings
from llm.interpreter import OUTPUT_SCHEMA, SYSTEM_PROMPT


async def main() -> int:
    key = os.environ.get("GROQ_API_KEY")
    if not key:
        print("FAIL: GROQ_API_KEY not set")
        return 1
    from groq import AsyncGroq

    client = AsyncGroq(api_key=key)
    payload = {
        "decision": "SHORT", "close": 81430.6,
        "structure": {"direction": "SHORT", "strength": 0.1, "reason": "close < VAL, below VWAP"},
        "aggression": {"direction": "SHORT", "strength": 0.61, "reason": "delta_sum -303.5, buy_ratio 0.35"},
        "dealer": {"direction": "NEUTRAL", "active": False, "reason": "INACTIVE: scale mismatch"},
    }
    try:
        resp = await asyncio.wait_for(client.chat.completions.create(
            model=settings.GROQ_MODEL,
            messages=[{"role": "system", "content": SYSTEM_PROMPT},
                      {"role": "user", "content": json.dumps(payload, separators=(",", ":"))}],
            response_format={"type": "json_schema", "json_schema": {
                "name": "trade_rationale", "strict": True, "schema": OUTPUT_SCHEMA}},
            temperature=0.0,
            max_completion_tokens=settings.GROQ_MAX_COMPLETION_TOKENS,
            extra_body={"reasoning_effort": settings.GROQ_REASONING_EFFORT},
        ), timeout=settings.GROQ_TIMEOUT_SECONDS)
    except Exception as exc:
        print(f"FAIL: API error {type(exc).__name__}: {exc}")
        return 1

    usage = getattr(resp, "usage", None)
    print("usage:", usage)
    details = getattr(usage, "completion_tokens_details", None)
    if details is not None:
        print("completion_tokens_details:", details)
    ch = resp.choices[0]
    content = ch.message.content or ""
    print("finish_reason:", ch.finish_reason)
    print("content:", content)

    try:
        parsed = json.loads(content)
        assert set(parsed) >= {"decision", "rationale", "key_factors"}
    except Exception:
        print("FAIL: empty or schema-invalid output. If completion_tokens is near "
              f"{settings.GROQ_MAX_COMPLETION_TOKENS} with finish_reason=length, reasoning "
              "consumed the budget: raise GROQ_MAX_COMPLETION_TOKENS or check reasoning_effort.")
        return 1

    used = getattr(usage, "total_tokens", None)
    print(f"PASS: valid JSON; decision={parsed['decision']}; total_tokens={used} "
          f"(pre-check budget GROQ_EST_TOKENS_PER_CALL={settings.GROQ_EST_TOKENS_PER_CALL})")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))