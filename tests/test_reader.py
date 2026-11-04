"""Reader tests run independently of MiniTopol and its dependencies."""
import json
from pathlib import Path
import subprocess
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading

import pytest

from reader import Reader, ReaderError


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
