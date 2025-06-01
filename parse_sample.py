#!/usr/bin/env python3
import re
import sys

def parse_macos_sample(filename):
    with open(filename, 'r') as f:
        content = f.read()

    call_graph_start = content.find('Call graph:')
    if call_graph_start == -1:
        print("No call graph found", file=sys.stderr)
        return []
    
    lines = content[call_graph_start:].split('\n')
    stacks = []
    stack_frames = {}
    
    for line in lines[1:]:
        if not line.strip():
            continue

        match = re.match(r'^(\s*)(\d+)\s+(.+)', line)
        if match:
            indent = len(match.group(1))
            count = int(match.group(2))
            frame = match.group(3).strip()
            
            # Clean up frame name - extract function name
            if ' (in ' in frame:
                frame = frame.split(' (in ')[0].strip()
            if ' + ' in frame:
                frame = frame.split(' + ')[0].strip()

            if 'Thread_' in frame or 'DispatchQueue_' in frame:
                continue
                
            depth = indent // 2
            stack_frames[depth] = frame

            if depth > 0:
                stack_parts = []
                for d in range(1, depth + 1):
                    if d in stack_frames:
                        stack_parts.append(stack_frames[d])
                
                if stack_parts:
                    stack_str = ';'.join(stack_parts)
                    stacks.append(f'{stack_str} {count}')
    
    return stacks

if __name__ == "__main__":
    filename = sys.argv[1] if len(sys.argv) > 1 else 'profile_sample.txt'
    stacks = parse_macos_sample(filename)

    stack_counts = {}
    for stack_line in stacks:
        parts = stack_line.rsplit(' ', 1)
        if len(parts) == 2:
            stack = parts[0]
            count = int(parts[1])
            stack_counts[stack] = stack_counts.get(stack, 0) + count

    for stack, count in stack_counts.items():
        print(f'{stack} {count}') 