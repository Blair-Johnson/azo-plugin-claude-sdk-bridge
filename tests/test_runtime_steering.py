"""Offline-only integration checks for the opt-in native runtime benchmark."""
from __future__ import annotations

import json
import os
from pathlib import Path
import select
import signal
import shutil
import time
import socket
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

ROOT = Path(__file__).resolve().parents[3]
BENCHMARK = ROOT / "integrations/claude-sdk-bridge/benchmarks/runtime_steering.py"
STEERED = "AZO_RUNTIME_STEERED"
STALE = "AZO_RUNTIME_STALE"
KEY = "synthetic-runtime-steering-local-key-123456"


def _invoke(events_file, base_url, *extra):
    env = dict(os.environ, AZO_CLAUDE_BRIDGE_KEY=KEY)
    command = [sys.executable, str(BENCHMARK), "--offline-fixture", "--base-url", base_url,
               "--events-file", str(events_file), "--timeout", "25", *extra]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, env=env, start_new_session=True)
    try:
        stdout, stderr = process.communicate(timeout=40)
        return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
    finally:
        # Own the entire POSIX group, including native workers holding pipes.
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(process.pid, sig)
            except ProcessLookupError:
                break
            try:
                process.communicate(timeout=1)
            except subprocess.TimeoutExpired:
                pass
        process.stdout.close()
        process.stderr.close()
        process.wait(timeout=2)


@pytest.mark.parametrize("finish_first", [False, True])
def test_real_runtime_normal_user_text_cancels_native_worker(tmp_path, finish_first):
    events = tmp_path / "progress.jsonl"
    bodies = []
    first_disconnected = threading.Event()
    stop = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def do_POST(self):
            assert self.path == "/v1/messages"
            assert self.headers.get("x-api-key") == KEY
            assert self.headers.get("x-azo-session-id") == "runtime-steering"
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            bodies.append(body)
            index = len(bodies)
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True

            def emit(kind, **fields):
                data = json.dumps({"type": kind, **fields})
                self.wfile.write(f"event: {kind}\ndata: {data}\n\n".encode())
                self.wfile.flush()

            try:
                emit("message_start", message={
                    "id": f"msg_{index}", "type": "message", "role": "assistant",
                    "model": "sonnet", "content": [], "stop_reason": None,
                    "stop_sequence": None, "usage": {"input_tokens": 512, "output_tokens": 0},
                })
                emit("content_block_start", index=0, content_block={"type": "text", "text": ""})
                last_user = json.dumps(body["messages"][-1])
                reply = STEERED if STEERED in last_user else STALE
                emit("content_block_delta", index=0, delta={"type": "text_delta", "text": reply})
                if index == 1:
                    with events.open("a") as stream:
                        stream.write(json.dumps({"event": "provider_started",
                                                 "session_id": "runtime-steering",
                                                 "request_index": 1}) + "\n")
                    # A real socket EOF from the terminated native worker, not a
                    # mocked Future.cancel(), releases this deliberately stale stream.
                    while not finish_first and not stop.is_set():
                        if select.select([self.connection], [], [], 0.05)[0]:
                            if not self.connection.recv(1, socket.MSG_PEEK):
                                first_disconnected.set()
                                break
                emit("content_block_stop", index=0)
                emit("message_delta", delta={"stop_reason": "end_turn", "stop_sequence": None},
                     usage={"output_tokens": 4})
                emit("message_stop")
            except (BrokenPipeError, ConnectionResetError):
                if index == 1:
                    first_disconnected.set()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        process = _invoke(events, f"http://127.0.0.1:{server.server_port}",
                          "--steer-delay-ms", "500" if finish_first else "0")
        result = json.loads(process.stdout)
        if finish_first:
            assert process.returncode == 1, result
            assert result["ok"] is False, result
            assert result["checks"]["first_future_cancelled"] is False
            assert KEY not in process.stdout + process.stderr
            return
        assert result["config"]["max_tokens"] == 2048
        assert "steer_to_submit_started_ms" in result["metrics"]
        assert "steer_to_cancel_returned_ms" in result["metrics"]
        assert process.returncode == 0, result
        assert result["ok"], result
        assert process.stderr == "", process.stderr
        assert all(result["checks"].values()), result
        assert result["counts"] == {"submissions": 2, "applied_answers": 1, "cancellations": 1}
        assert first_disconnected.wait(2), "First native worker socket did not close"
        assert len(bodies) == 2
        assert STEERED not in json.dumps(bodies[0])
        assert STEERED in json.dumps(bodies[1]["messages"][-1])
        assert bodies[1]["messages"][-1]["role"] == "user"
        assert all(message["role"] != "assistant" for message in bodies[1]["messages"])
        assert KEY not in process.stdout
        assert "Synthetic input padding" not in process.stdout
        print(json.dumps(result, sort_keys=True))
    finally:
        stop.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.parametrize("url", ["https://example.com", "http://example.com", "http://localhost:8766",
                                  "http://127.0.0.1:8766/?key=secret"])
def test_runtime_benchmark_rejects_nonliteral_loopback_origins(tmp_path, url):
    process = _invoke(tmp_path / "unused.jsonl", url)
    assert process.returncode == 1
    result = json.loads(process.stdout)
    assert result["error_types"] == ["ValueError"]
    assert "secret" not in process.stdout


def test_runtime_benchmark_requires_explicit_mode(tmp_path):
    process = subprocess.run(
        [sys.executable, str(BENCHMARK), "--base-url", "http://127.0.0.1:8766",
         "--events-file", str(tmp_path / "unused.jsonl")], capture_output=True, text=True,
    )
    assert process.returncode == 2


def _node(source, *args):
    if not shutil.which("node"):
        pytest.skip("Node is required for launcher checks")
    return subprocess.run(["node", "--input-type=module", "-e", source, *map(str, args)],
                          capture_output=True, text=True, timeout=15)


def test_runtime_launcher_rejects_incomplete_sdk_cancellation_evidence():
    module = BENCHMARK.with_name("runtime-live.mjs").as_uri()
    process = _node(f"""
      import assert from 'node:assert/strict';
      import {{ validateRuntimeMarks }} from {json.dumps(module)};
      const mark = (event, at_ms, fields = {{}}) => ({{event, at_ms, ...fields}});
      const marks = [
        mark('runtime_request', 0, {{session:'test', request:1}}),
        mark('query_construct', 1, {{query:4}}), mark('spawn', 2, {{query:4}}),
        mark('first_text', 3, {{session:'test', request:1}}),
        mark('runtime_transport_aborted', 4, {{session:'test', request:1}}),
        mark('exit', 5, {{query:4}}),
        mark('runtime_request', 6, {{session:'test', request:2}}),
        mark('query_construct', 7, {{query:5}}), mark('spawn', 8, {{query:5}}),
        mark('first_text', 9, {{session:'test', request:2}}),
      ];
      assert.equal(validateRuntimeMarks(marks, 'test', 5000, 'rebuild').passed, true);
      const rejects = [
        marks.filter(m => m.event !== 'runtime_transport_aborted'),
        marks.filter(m => m.event !== 'exit'),
        marks.filter(m => !(m.event === 'spawn' && m.query === 5)),
        [...marks, mark('sdk_result', 3.5, {{query:4}})],
        marks.map(m => m.event === 'exit' ? {{...m, query:99}} : m),
        marks.map(m => m.event === 'runtime_transport_aborted' ? {{...m, request:2}} : m),
        marks.map(m => m.event === 'exit' ? {{...m, at_ms:3}} : m),
        marks.map(m => m.event === 'exit' ? {{...m, at_ms:6000}} : m),
      ];
      for (const candidate of rejects) assert.equal(validateRuntimeMarks(candidate, 'test', 5000, 'rebuild').passed, false);
      // An unrelated SDK result cannot be mistaken for completion of query 4.
      assert.equal(validateRuntimeMarks([...marks, mark('sdk_result', 3.5, {{query:99}})], 'test', 5000, 'rebuild').passed, true);
    """)
    assert process.returncode == 0, process.stderr


def test_runtime_launcher_reuse_requires_interrupt_boundary_and_same_live_child():
    module = BENCHMARK.with_name("runtime-live.mjs").as_uri()
    process = _node(f"""
      import assert from 'node:assert/strict';
      import {{ validateRuntimeMarks }} from {json.dumps(module)};
      const mark = (event, at_ms, fields = {{}}) => ({{event, at_ms, ...fields}});
      const marks = [
        mark('runtime_request', 0, {{session:'test', request:1}}),
        mark('query_construct', 1, {{query:4}}), mark('spawn', 2, {{query:4}}),
        mark('first_text', 3, {{session:'test', request:1}}),
        mark('runtime_transport_aborted', 4, {{session:'test', request:1}}),
        mark('interrupt_requested', 5, {{query:4}}),
        mark('runtime_request', 6, {{session:'test', request:2}}),
        mark('interrupt_ack', 7, {{query:4, queued_count:0}}),
        mark('sdk_result', 8, {{query:4}}),
        mark('first_text', 9, {{session:'test', request:2}}),
        mark('child_alive', 10, {{query:4}}),
      ];
      const valid = validateRuntimeMarks(marks, 'test');
      assert.equal(valid.passed, true);
      assert.equal(valid.interrupt_mode, 'reuse');
      assert.equal(valid.first_query, valid.replacement_query);
      assert.equal(valid.process_restarts, 0);
      assert.equal(valid.abort_to_sdk_exit_ms, null);
      assert.equal(valid.abort_to_replacement_first_text_ms, 5);
      const change = (event, fields) => marks.map(m => m.event === event ? {{...m, ...fields}} : m);
      for (const event of ['runtime_transport_aborted', 'interrupt_requested', 'interrupt_ack',
        'sdk_result', 'child_alive', 'first_text', 'spawn', 'runtime_request']) {{
        assert.equal(validateRuntimeMarks(marks.filter(m => m.event !== event), 'test').passed, false, event);
      }}
      const rejects = [
        [...marks, mark('spawn', 6, {{query:5}})],
        [...marks, mark('query_construct', 6, {{query:5}})],
        [...marks, mark('exit', 8.5, {{query:4}})],
        [...marks, mark('sdk_result', 3.5, {{query:4}})],
        [...marks, mark('interrupt_requested', 5.5, {{query:4}})],
        [...marks, mark('interrupt_ack', 7.5, {{query:4, queued_count:0}})],
        change('interrupt_requested', {{at_ms:3.5}}),
        change('interrupt_ack', {{at_ms:4.5}}),
        change('interrupt_ack', {{at_ms:10}}),
        change('interrupt_ack', {{queued_count:1}}),
        change('interrupt_ack', {{query:99}}),
        change('sdk_result', {{at_ms:3.5}}),
        change('sdk_result', {{at_ms:4.5}}),
        change('sdk_result', {{at_ms:10}}),
        change('sdk_result', {{query:99}}),
        change('child_alive', {{query:99}}),
        change('child_alive', {{at_ms:8}}),
        change('runtime_transport_aborted', {{request:2}}),
      ];
      for (const candidate of rejects) assert.equal(validateRuntimeMarks(candidate, 'test').passed, false);
      assert.equal(validateRuntimeMarks(marks, 'test', 5000, 'rebuild').passed, false);
      assert.equal(validateRuntimeMarks(marks, 'test', 5000, 'typo').passed, false);
      // The result and acknowledgment can arrive in either order, but both must
      // precede the replacement text. Unrelated query completion is not evidence.
      assert.equal(validateRuntimeMarks(change('sdk_result', {{at_ms:6.5}}), 'test').passed, true);
      assert.equal(validateRuntimeMarks([...marks, mark('sdk_result', 3.5, {{query:99}})], 'test').passed, true);
    """)
    assert process.returncode == 0, process.stderr



def test_measured_manager_forwards_interrupt_without_logging_queued_messages():
    module = BENCHMARK.with_name("measured-manager.mjs").as_uri()
    process = _node(f"""
      import assert from 'node:assert/strict';
      import {{ registerHooks }} from 'node:module';
      const receipt = {{still_queued:['secret-queued-uuid'], private_value:'credential-sentinel'}};
      let calls = 0, resolveInterrupt;
      const sdk = {{
        interrupt() {{
          assert.equal(this, sdk);
          calls++;
          return new Promise(resolve => {{ resolveInterrupt = resolve; }});
        }},
        close() {{ assert.equal(this, sdk); }},
        async *[Symbol.asyncIterator]() {{ yield {{type:'result', is_error:false, subtype:'success'}}; }},
      }};
      globalThis.measurementSdk = sdk;
      const hooks = registerHooks({{
        resolve(specifier, context, nextResolve) {{
          if (specifier === '@anthropic-ai/claude-agent-sdk') return {{shortCircuit:true,
            url:'data:text/javascript,export const query = () => globalThis.measurementSdk;'}};
          return nextResolve(specifier, context);
        }},
      }});
      const {{ measuredManager }} = await import({json.dumps(module)});
      const observation = measuredManager();
      try {{
        const wrapped = observation.manager.queryFactory({{options:{{}}}});
        const pending = wrapped.interrupt();
        assert.equal(calls, 1);
        assert.equal(observation.marks.at(-1).event, 'interrupt_requested');
        assert.equal(observation.marks.at(-1).query, 0);
        assert.equal(observation.marks.some(m => m.event === 'interrupt_ack'), false);
        resolveInterrupt(receipt);
        assert.equal(await pending, receipt);
        const ack = observation.marks.at(-1);
        assert.equal(ack.event, 'interrupt_ack');
        assert.equal(ack.query, 0);
        assert.equal(ack.queued_count, 1);
        assert.equal(JSON.stringify(observation.marks).includes('secret-queued-uuid'), false);
        assert.equal(JSON.stringify(observation.marks).includes('credential-sentinel'), false);
        for await (const message of wrapped) assert.equal(message.type, 'result');
        assert.equal(observation.marks.at(-1).event, 'sdk_result');
        wrapped.close();
        sdk.interrupt = async () => {{ throw new Error('credential-sentinel'); }};
        await assert.rejects(wrapped.interrupt());
        assert.equal(observation.marks.filter(m => m.event === 'interrupt_ack').length, 1);
        assert.equal(JSON.stringify(observation.marks).includes('credential-sentinel'), false);
      }} finally {{ await observation.manager.close(); hooks.deregister(); }}
    """)
    assert process.returncode == 0, process.stderr

def test_runtime_launcher_requires_one_successful_python_result():
    module = BENCHMARK.with_name("runtime-live.mjs").as_uri()
    process = _node(f"""
      import assert from 'node:assert/strict';
      import {{ validateRuntimeResult }} from {json.dumps(module)};
      const keys = ['provider_signal_received','normal_user_text_sent','exactly_two_native_submissions',
        'replacement_saw_user_instruction','first_future_cancelled','normal_user_preemption',
        'first_worker_exited','worker_replaced','only_steered_answer_applied','stale_answer_absent',
        'no_manual_interrupt','no_provider_error','watchdog_not_used','cancellation_bounded',
        'all_native_workers_exited','watchdog_thread_exited'];
      const result = {{kind:'runtime_steering', mode:'live', ok:true,
        checks:Object.fromEntries(keys.map(k => [k,true])), metrics:{{total_ms:1}},
        counts:{{submissions:2,applied_answers:1,cancellations:1}}, error_types:[]}};
      const valid = JSON.stringify(result);
      assert.equal(validateRuntimeResult(valid).ok, true);
      for (const candidate of ['', 'credential-sentinel', valid+'\\n'+valid,
        JSON.stringify({{...result,ok:false}}), JSON.stringify({{...result,checks:{{}}}}),
        JSON.stringify({{...result,counts:{{submissions:1,applied_answers:1,cancellations:1}}}})]) {{
        assert.throws(() => validateRuntimeResult(candidate));
      }}
    """)
    assert process.returncode == 0, process.stderr


@pytest.mark.skipif(os.name != "posix", reason="Owned process groups require POSIX")
def test_runtime_launcher_bounds_stubborn_child_and_handles_spawn_error(tmp_path):
    module = BENCHMARK.with_name("runtime-live.mjs").as_uri()
    started = time.monotonic()
    process = _node(f"""
      import assert from 'node:assert/strict';
      import {{ runOwnedPython }} from {json.dumps(module)};
      const stuck = await runOwnedPython(process.argv[1], ['-c',
        'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)'],
        {{timeoutMs:300, graceMs:100, killMs:1000}});
      assert.equal(stuck.timed_out, true);
      assert.equal(stuck.all_native_children_exited, true);
      assert.notEqual(stuck.exit_code, 0);
      const descendant = await runOwnedPython(process.argv[1], ['-c',
        `import subprocess,sys; subprocess.Popen([sys.executable, '-c',
          'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)'])`],
        {{timeoutMs:300, graceMs:100, killMs:1000}});
      assert.equal(descendant.timed_out, true); // Parent exited, but its child retained the pipes.
      assert.equal(descendant.all_native_children_exited, true);
      const missing = await runOwnedPython(process.argv[2], [], {{timeoutMs:300}});
      assert.equal(missing.spawn_error, true);
      assert.equal(missing.all_native_children_exited, true);
    """, sys.executable, tmp_path / "missing-python")
    assert process.returncode == 0, process.stderr
    assert time.monotonic() - started < 10


@pytest.mark.skipif(os.name != "posix", reason="Owned process groups require POSIX")
def test_runtime_launcher_malformed_child_output_fails_closed_without_leaking(tmp_path):
    fake_python = tmp_path / "fake-python"
    fake_python.write_text("#!/bin/sh\nprintf 'credential-sentinel-invalid-json\\n'\n")
    fake_python.chmod(0o700)
    module = BENCHMARK.with_name("runtime-live.mjs").as_uri()
    # The fake executable never runs the Python benchmark or calls a provider.
    process = _node(f"""
      import {{ main }} from {json.dumps(module)};
      process.exitCode = await main(['--live', '--replicates', '1', '--python', process.argv[1]]);
    """, fake_python)
    assert process.returncode == 1, process.stderr
    rows = [json.loads(line) for line in process.stdout.splitlines()]
    assert rows[-1]["kind"] == "final"
    assert rows[-1]["passed"] is False
    assert rows[-1]["all_children_exited"] is True
    assert rows[-1]["completed_replicates"] == 0
    assert "credential-sentinel" not in process.stdout + process.stderr
    assert rows[0]["max_tokens"] == 2048
    assert rows[0]["thinking"] == "sdk_default"
    assert rows[0]["system_prompt_mode"] == "append"
    assert rows[0]["interrupt_mode"] == "reuse"
