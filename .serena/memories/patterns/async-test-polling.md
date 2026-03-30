---
id: async-test-polling
trigger: when running long-running asynchronous tests like fuzz tests or cluster integration tests
confidence: 0.7
domain: testing
source: session-observation
scope: project
project_id: 0fdf8cbe7359
project_name: gookv
---

# Async Test Polling with Sleep Barriers

## Action
For long-running tests (>1000s timeout), execute the test as a background task, then sleep 300 seconds before reading results from the temporary task output file instead of waiting for direct stdout.

## Evidence
- Observed 10 times via sequential bash operations pattern
- Observed 4 times as explicit blocking sleep-wait pattern
- Pattern: `run test → sleep 300s → read results from .claude/projects/*/tool-results/*.txt`
- Last observed: 2026-03-29T10:34:32Z
- Example: TestFuzzCluster with 50 iterations, 2400s timeout, 1 count

## When to Apply
- Fuzz tests with timeouts exceeding 1000 seconds
- E2E cluster tests requiring multi-node initialization
- Tests with large output that would block agent context
- Integration tests with external dependencies (PD, TiKV stores)