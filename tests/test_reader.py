"""Reader tests run independently of MiniTopol and its dependencies."""
import json
from pathlib import Path
import subprocess
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading

import pytest

from reader import Reader, ReaderError


def test_registry_retries_transient_access_failure(monkeypatch):
    import reader

    class Registry:
        attempts = 0

        def is_file(self):
            return True

        def read_text(self, **kwargs):
            self.attempts += 1
            if self.attempts < 3:
                raise PermissionError('transient replacement')
            return '{"schema":1}'

    registry = Registry()
    monkeypatch.setattr(reader, 'registry_path', lambda: registry)
    assert reader._registry() == {'schema': 1}
    assert registry.attempts == 3


def test_registry_permanent_access_failure_is_reported(monkeypatch):
    import reader

    class Registry:
        def is_file(self):
            return True

        def read_text(self, **kwargs):
            raise PermissionError('permanent denial')

    monkeypatch.setattr(reader, 'registry_path', lambda: Registry())
    with pytest.raises(ReaderError, match='remains inaccessible'):
        reader._registry()


class FakeHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        if self.headers.get("Authorization") != "Bearer test-token":
            self.send_response(401)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        result = ({"protocol": 1, "pid": 999, "budget_bytes": 1024, "cache_bytes": 0}
                  if self.path.endswith("status") else {"trajectories": []})
        self.send_json(result)

    def do_POST(self):
        value = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path.endswith("/get"):
            result = {"lease": "abc", "frames": value["frames"], "buffers": [],
                      "trajectory": value["trajectory"], "descriptors": value["descriptors"]}
        elif self.path.endswith("/release"):
            result = {"released": value["lease"] == "abc"}
        else:
            result = {"stopping": True}
            config = json.loads(self.server.registry_path.read_text())
            config.pop("session", None)
            self.server.registry_path.write_text(json.dumps(config))
        self.send_json(result)

    def send_json(self, obj):
        body = json.dumps(obj).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


@pytest.fixture
def server(tmp_path, monkeypatch):
    http = ThreadingHTTPServer(("127.0.0.1", 0), FakeHandler)
    thread = threading.Thread(target=http.serve_forever, daemon=True)
    thread.start()
    path = tmp_path / "registered.json"
    path.write_text(json.dumps({"schema": 1,
        "session": {"port": http.server_port, "token": "test-token", "pid": 999}}))
    http.registry_path = path
    monkeypatch.setenv("MINITOPOL_PROVIDER_REGISTRY", str(path))
    try:
        yield http
    finally:
        http.shutdown()
        http.server_close()
        thread.join()


def test_client_commands(server):
    client = Reader()
    assert client.status()["protocol"] == 1
    assert client.list() == {"trajectories": []}
    lease = client.request("test", data=["coords", "sasa"], frames=[3, 1])
    assert lease.metadata["frames"] == [3, 1]
    lease.close()
    assert lease.closed
    assert client.stop()["stopping"]


def test_all_descriptor_request(server):
    client = Reader()
    lease = client.request("test", data=["coords", "all"], frames=[0])
    assert lease.metadata["descriptors"] == "all"
    lease.close()


def test_cli_from_unrelated_directory(server, tmp_path):
    script = Path(__file__).resolve().parents[1] / "reader.py"
    p = subprocess.run([sys.executable, str(script), "get", "test",
                        "--data", "coords,all", "--frames", "4,2"],
                       cwd=tmp_path, capture_output=True, text=True, check=True)
    value = json.loads(p.stdout)
    assert value["frames"] == [4, 2]


def test_unknown_lease(server):
    client = Reader()
    lease = client.request("test", frames=[0])
    with pytest.raises(KeyError):
        lease.view("unknown")
    lease.close()


def test_no_registration(tmp_path, monkeypatch):
    monkeypatch.setenv("MINITOPOL_PROVIDER_REGISTRY", str(tmp_path / "missing.json"))
    with pytest.raises(ReaderError, match="not registered"):
        Reader().list()


def test_stop_waits_until_old_session_is_removed(monkeypatch):
    import reader
    session = {"pid": 999, "port": 1234, "token": "test"}
    remaining = iter([{"session": session}, {"session": session}, {}])
    waits = []
    monkeypatch.setattr(reader, "_session", lambda **kwargs: session)
    monkeypatch.setattr(reader, "_send", lambda *args: {"stopping": True})
    monkeypatch.setattr(reader, "_registry", lambda: next(remaining))
    monkeypatch.setattr(reader.time, "sleep", waits.append)
    assert reader.Reader().stop() == {"stopping": True}
    assert waits == [0.05, 0.05]


def test_next_frames_hint_is_optional(monkeypatch):
    import reader

    payloads = []
    monkeypatch.setattr(reader, '_session', lambda: {})
    def send(session, path, payload, timeout):
        payloads.append(payload)
        return {'lease': 'hint', 'buffers': []}
    monkeypatch.setattr(reader, '_send', send)
    client = Reader()
    client.request('trajectory', data=['ramachandran'], frames=[2, 0])
    client.request('trajectory', data=['ramachandran'], frames=[2, 0], next_frames=[1, 3])
    assert 'next_frames' not in payloads[0]
    assert payloads[1]['next_frames'] == [1, 3]


def test_multi_tier_api_and_ordered_batch_payload(monkeypatch):
    import reader

    calls = []
    monkeypatch.setattr(reader, "_session", lambda: {})
    def fake_send(session, path, payload=None, timeout=15):
        calls.append((path, payload))
        if path == "get":
            return {"lease": "test", "buffers": [], "frames": payload["frames"]}
        return {"accepted": True}
    monkeypatch.setattr(reader, "_send", fake_send)
    client = Reader()
    batch = client.batch_request("test", data=["coords", "sasa"], frames=[3, 0])
    assert batch == {"trajectory": "test", "coordinates": True,
                     "descriptors": ["sasa"], "frames": [3, 0]}
    client.configure_storage(enabled=True, external_root="/external",
                             cache_percent=10, write_policy="longevity", prefetch_batches=2)
    assert calls[-1][1]["prefetch_batches"] == 2
    client.plan_batches([batch])
    assert calls[-1] == ("batches/plan", {"batches": [batch]})
    client.request("test", data=["coords", "sasa"], frames=[3, 0],
                   plan_id="plan", plan_index=0)
    assert calls[-1][1] == {**batch, "plan_id": "plan", "plan_index": 0}
    client.storage_status()
    assert calls[-1][0] == "storage/status"
    client.offload("trajectories/test")
    assert calls[-1][1]["relative"] == "trajectories/test"
    client.ingest_sources("test")
    assert calls[-1] == ("storage/ingest", {"trajectory": "test"})
    client.prune_storage(all_cache=True)
    assert calls[-1][0] == "storage/prune"
    client.batch_status("plan")
    assert calls[-1] == ("batches/status", {"plan_id": "plan"})
    client.cancel_batches("plan")
    assert calls[-1] == ("batches/cancel", {"plan_id": "plan"})


def test_reader_capacity_budget_fields(monkeypatch):
    import reader
    sent = []
    monkeypatch.setattr(reader, "_session", lambda: {})
    monkeypatch.setattr(reader, "_send", lambda _session, path, payload=None, timeout=15:
                        sent.append((path, payload)) or {"ok": True})
    client = Reader()
    client.configure_storage(cache_limit_bytes=2 * 1024**3,
                             cache_slack_percent=25.0,
                             cache_slack_bytes=512 * 1024**2)
    assert sent[-1] == ("storage/configure", {
        "cache_limit_bytes": 2 * 1024**3,
        "cache_slack_percent": 25.0,
        "cache_slack_bytes": 512 * 1024**2,
    })
    client.configure_storage(cache_percent=40)
    assert sent[-1] == ("storage/configure", {"cache_percent": 40})


def test_reader_configures_atlas_raw_storage_destination(monkeypatch):
    import reader
    sent = []
    monkeypatch.setattr(reader, "_session", lambda: {})
    monkeypatch.setattr(reader, "_send", lambda _session, path, payload=None, timeout=15:
                        sent.append((path, payload)) or {"ok": True})
    Reader().configure_storage(atlas_source_tier="external")
    assert sent[-1] == ("storage/configure", {"atlas_source_tier": "external"})
