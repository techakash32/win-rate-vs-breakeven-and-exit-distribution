#!/bin/bash
# Runs the paper-live engine and the dashboard in one container.
# If either process dies, the container exits so the platform restarts it.

python -m scripts.paper_live &
ENGINE_PID=$!

python -m uvicorn scripts.dashboard:app --host 0.0.0.0 --port "${PORT:-8000}" &
WEB_PID=$!

trap 'kill $ENGINE_PID $WEB_PID 2>/dev/null' TERM INT

wait -n $ENGINE_PID $WEB_PID
STATUS=$?
kill $ENGINE_PID $WEB_PID 2>/dev/null
exit $STATUS