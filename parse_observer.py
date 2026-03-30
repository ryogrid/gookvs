#!/usr/bin/env python3
import json
from collections import defaultdict
from datetime import datetime

file_path = "/home/ryo/.claude/homunculus/projects/0fdf8cbe7359/.observer-tmp/ecc-observer-analysis.YGSs0J.jsonl"

patterns = defaultdict(lambda: {
    'count': 0,
    'occurrences': [],
    'domains': set(),
    'last_observed': None,
    'examples': []
})

try:
    with open(file_path, 'r') as f:
        for line_num, line in enumerate(f, 1):
            if not line.strip():
                continue
            try:
                entry = json.loads(line)

                # Extract pattern information
                if 'pattern' in entry:
                    pattern_name = entry['pattern']
                    patterns[pattern_name]['count'] += 1
                    patterns[pattern_name]['occurrences'].append(entry.get('session_id', 'unknown'))
                    if entry.get('domain'):
                        patterns[pattern_name]['domains'].add(entry['domain'])
                    patterns[pattern_name]['last_observed'] = entry.get('timestamp')
                    if entry.get('description'):
                        patterns[pattern_name]['examples'].append(entry['description'])

            except json.JSONDecodeError as e:
                print(f"Error parsing line {line_num}: {e}")
                continue
except FileNotFoundError:
    print(f"File not found: {file_path}")
    exit(1)

# Filter patterns with 3+ occurrences
filtered = {k: v for k, v in patterns.items() if v['count'] >= 3}

# Create output
output = {
    'total_patterns_found': len(patterns),
    'qualifying_patterns': len(filtered),
    'patterns': {}
}

for pattern_name in sorted(filtered.keys(), key=lambda x: filtered[x]['count'], reverse=True):
    v = filtered[pattern_name]
    output['patterns'][pattern_name] = {
        'occurrences': v['count'],
        'domains': list(v['domains']),
        'last_observed': v['last_observed'],
        'unique_sessions': len(set(v['occurrences'])),
        'examples': v['examples'][:3] if v['examples'] else []
    }

print(json.dumps(output, indent=2))
