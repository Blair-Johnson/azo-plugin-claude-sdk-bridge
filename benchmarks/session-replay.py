"""Read-only session audit; metadata by default, sensitive request only with --request.

Request mode approximates saved history with placeholder historical tool schemas;
it does not reconstruct the live renderer/configuration or validate signatures.
Pipe it only into the offline fixture, never into a live provider or public log.
"""
import argparse
import json
from pathlib import Path
import sys

# The read-only journal loader is a host diagnostic, not part of this plugin.
# Resolve its checkout from the selected host interpreter, never sibling paths.
import agent_zoo
sys.path.insert(0, str(Path(agent_zoo.__file__).resolve().parents[1]))
from scripts.diagnostics.large_session_projection_probe import load_readonly

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('journal', type=Path)
parser.add_argument('--session-id', required=True)
parser.add_argument('--local-state-root', type=Path, required=True)
parser.add_argument('--commit')
parser.add_argument('--request', action='store_true', help='Emit native request to a private pipe instead of metadata.')
args = parser.parse_args()
session, loaded = load_readonly(args)
if args.request:
    from agents.llm import _messages_to_anthropic_messages
    messages = [message for entry in session.state.entries for message in entry.messages]
    native = _messages_to_anthropic_messages(messages)
    names = sorted({block['name'] for message in native for block in message['content']
                    if block['type'] == 'tool_use'})
    print(json.dumps({'model': 'claude-opus-5-5', 'max_tokens': 512,
                      'system': messages[0].get('content', ''), 'messages': native,
                      'tools': [{'name': name, 'input_schema': {'type': 'object'}} for name in names]}))
    raise SystemExit(0)
print(json.dumps({'kind': 'source', 'session_id': args.session_id,
                  'commit': loaded.state.commit_id, 'entries': len(session.state.entries)}))
totals = {'input_tokens': 0, 'cache_read_input_tokens': 0, 'cache_creation_input_tokens': 0}
for index, entry in enumerate(session.state.entries):
    for message in entry.messages:
        if message.get('role') != 'assistant':
            continue
        usage = message.get('_usage') or {}
        signatures = [len(block.get('signature', '')) for block in message.get('thinking_blocks', [])
                      if block.get('type') == 'thinking']
        counters = {key: usage.get(key, 0) for key in totals}
        for key, value in counters.items():
            totals[key] += value or 0
        print(json.dumps({'kind': 'assistant', 'entry': index,
                          'text_chars': len(message.get('content') or ''),
                          'tool_calls': len(message.get('tool_calls') or []),
                          'signature_lengths': signatures, 'usage': counters}))
denominator = sum(totals.values())
print(json.dumps({'kind': 'summary', 'usage': totals,
                  'cache_hit_fraction': totals['cache_read_input_tokens'] / denominator if denominator else None}))
