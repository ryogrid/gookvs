#!/usr/bin/env python3
import json
from collections import defaultdict
import sys

file_path = "/home/ryo/.claude/homunculus/projects/0fdf8cbe7359/.observer-tmp/ecc-observer-analysis.hd1r3i.jsonl"

patterns = defaultdict(lambda: {
    'count': 0,
    'occurrences': [],
    'domains': set(),
    'last_session': None,
    'descriptions': []
})

try:
    with open(file_path, 'r') as f:
        for line in f:
            if not line.strip():
                continue
            try:
                entry = json.loads(line)

                # Collect user feedback/corrections
                if 'user_feedback' in entry and entry.get('user_feedback'):
                    feedback = entry['user_feedback']
                    key = feedback.get('pattern', 'unknown')
                    patterns[key]['count'] += 1
                    patterns[key]['occurrences'].append(entry.get('session_id'))
                    if entry.get('domain'):
                        patterns[key]['domains'].add(entry['domain'])
                    patterns[key]['last_session'] = entry.get('timestamp')
                    if feedback.get('description'):
                        patterns[key]['descriptions'].append(feedback['description'])

                # Collect error patterns
                if 'errors' in entry and entry.get('errors'):
                    for error in entry['errors']:
                        error_type = error.get('type', 'unknown')
                        key = f"error-{error_type}"
                        patterns[key]['count'] += 1
                        patterns[key]['occurrences'].append(entry.get('session_id'))
                        patterns[key]['domains'].add('debugging')
                        patterns[key]['last_session'] = entry.get('timestamp')
                        if error.get('description'):
                            patterns[key]['descriptions'].append(error['description'])

                # Collect tool usage patterns
                if 'tool_calls' in entry:
                    tools = entry.get('tool_calls', {})
                    for tool, count in tools.items():
                        key = f"tool-{tool}"
                        patterns[key]['count'] += 1
                        patterns[key]['occurrences'].append(entry.get('session_id'))
                        patterns[key]['domains'].add('workflow')
                        patterns[key]['last_session'] = entry.get('timestamp')

            except json.JSONDecodeError:
                continue

    # Filter for 3+ occurrences
    qualifying = {k: v for k, v in patterns.items() if v['count'] >= 3}

    print(f"Total patterns found: {len(patterns)}")
    print(f"Patterns with 3+ occurrences: {len(qualifying)}\n")

    for name, data in sorted(qualifying.items(), key=lambda x: -x[1]['count']):
        print(f"{name}: {data['count']} occurrences")
        print(f"  Domains: {', '.join(data['domains']) if data['domains'] else 'N/A'}")
        print(f"  Last session: {data['last_session']}")
        if data['descriptions']:
            print(f"  Description: {data['descriptions'][0]}")
        print()

except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    import traceback
    traceback.print_exc()
