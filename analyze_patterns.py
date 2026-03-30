#!/usr/bin/env python3
import json
from collections import defaultdict

file_path = "/home/ryo/.claude/homunculus/projects/0fdf8cbe7359/.observer-tmp/ecc-observer-analysis.YGSs0J.jsonl"

patterns = defaultdict(lambda: {
    'count': 0,
    'occurrences': [],
    'domains': set(),
    'last_session': None,
    'descriptions': []
})

with open(file_path, 'r') as f:
    for line in f:
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
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

qualifying = {k: v for k, v in patterns.items() if v['count'] >= 3}
output = {
    'total_patterns': len(patterns),
    'qualifying_patterns': len(qualifying),
    'patterns': {}
}

for name, data in sorted(qualifying.items(), key=lambda x: -x[1]['count']):
    output['patterns'][name] = {
        'occurrences': data['count'],
        'domains': list(data['domains']),
        'last_session': data['last_session'],
        'examples': data['descriptions'][:2]
    }

print(json.dumps(output, indent=2))
