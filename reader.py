"""MiniTopol Reader. Copy this single file into any Python 3.11+ project."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class ReaderError(RuntimeError):
    pass


def registry_path():
    override = os.environ.get("MINITOPOL_PROVIDER_REGISTRY")
    return Path(override).expanduser().resolve() if override else Path.home() / ".minitopol" / "provider.json"


def _registry():
    path = registry_path()
    if not path.is_file():
        raise ReaderError("MiniTopol Provider is not registered. Run: python -m minitopol.provider register")
    try:
        config = json.loads(path.read_text(encoding="utf-8"))
    except PermissionError:
        deadline = time.monotonic() + 2.0
        while True:
            try:
                config = json.loads(path.read_text(encoding="utf-8"))
                break
            except PermissionError as error:
                if time.monotonic() >= deadline:
                    raise ReaderError("Provider registry remains inaccessible") from error
                time.sleep(0.01)
    if config.get("schema") != 1:
        raise ReaderError("Incompatible Provider registry version")
    return config


def _send(session, path, payload=None, timeout=15):
    port, token = session["port"], session["token"]
    if type(port) is not int or not 0 < port < 65536:
        raise ReaderError("Invalid local Provider port")
    data = None if payload is None else json.dumps(payload, separators=(",", ":")).encode("utf-8")
    request = Request(f"http://127.0.0.1:{port}/v1/{path}", data=data,
                      headers={"Authorization": "Bearer " + token,
                               "Content-Type": "application/json"},
                      method="POST" if data is not None else "GET")
    try:
        with urlopen(request, timeout=timeout) as response:
            return json.load(response)
    except HTTPError as error:
        try:
            reason = json.loads(error.read().decode("utf-8")).get("error", str(error))
        except (ValueError, UnicodeDecodeError):
            reason = str(error)
        raise ReaderError(reason) from error
    except (OSError, URLError, ValueError, KeyError) as error:
        raise ReaderError(f"Provider connection failed: {error}") from error


def _live(config):
    session = config.get("session")
    if not isinstance(session, dict):
        return None
    try:
        response = _send(session, "status", timeout=0.5)
        if response.get("protocol") == 1 and response.get("pid") == session.get("pid"):
            return session
    except ReaderError:
        pass
    return None


def _session(autostart=True):
    config = _registry()
    session = _live(config)
    if session is not None:
        return session
    if not autostart:
        raise ReaderError("Provider is not running")
    launch = config.get("launch")
    if not isinstance(launch, list) or not launch or not all(isinstance(x, str) for x in launch):
        raise ReaderError("Provider launch command is missing")
    kwargs = {"stdin": subprocess.DEVNULL, "stdout": subprocess.DEVNULL,
              "stderr": subprocess.DEVNULL}
    if os.name == "nt":
        kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    else:
        kwargs["start_new_session"] = True
    subprocess.Popen(launch, **kwargs)
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        time.sleep(0.15)
        current = _registry()
        session = _live(current)
        if session is not None:
            return session
    raise ReaderError("Provider did not start; check its Python environment and registration")


class Lease:
    """A zero-copy view remains valid until explicit close/release."""

    def __init__(self, client, metadata):
        self.client = client
        self.metadata = metadata
        self._handles = {}
        self.closed = False

    def _part(self, name):
        if self.closed:
            raise ReaderError("Lease is closed")
        for item in self.metadata["buffers"]:
            if item["name"] == name:
                return item
        raise KeyError(name)

    def view(self, name):
        """Return a memoryview of a shared buffer (no copying)."""
        info = self._part(name)
        if name not in self._handles:
            from multiprocessing.shared_memory import SharedMemory
            if sys.version_info >= (3, 13):
                shm = SharedMemory(name=info["shm"], create=False, track=False)
            else:
                shm = SharedMemory(name=info["shm"], create=False)
                # Python <=3.12 may unlink another process' segment on exit.
                if os.name != "nt":
                    from multiprocessing import resource_tracker
                    resource_tracker.unregister(shm._name, "shared_memory")
            self._handles[name] = shm
        return self._handles[name].buf[:info["bytes"]]

    def numpy(self, name):
        """Optional numpy view for ndarray buffers; returned array is not copied."""
        info = self._part(name)
        if info["format"] != "ndarray":
            raise TypeError("Use arrow() for Arrow IPC descriptors")
        import numpy as np
        return np.ndarray(info["shape"], dtype=np.dtype(info["dtype"]), buffer=self.view(name))

    def arrow(self, descriptor):
        """Optional pyarrow logical table including sparse and nullable columns."""
        name = "descriptor:" + descriptor
        info = self._part(name)
        if info["format"] != "arrow-ipc":
            raise TypeError("Not an Arrow IPC descriptor")
        import pyarrow as pa
        return pa.ipc.open_stream(pa.BufferReader(self.view(name))).read_all()

    def close(self):
        if self.closed:
            return
        # Do not release Provider storage while this process exports memoryviews.
        for handle in self._handles.values():
            try:
                handle.close()
            except BufferError as error:
                raise ReaderError("Release local arrays/views before closing the lease") from error
        self.client.release(self.metadata["lease"])
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, kind, error, traceback):
        self.close()


class Reader:
    """Small connection-only client; MiniTopol format and codec are irrelevant."""

    def start(self):
        return _send(_session(), "status")

    def status(self):
        try:
            return _send(_session(autostart=False), "status")
        except ReaderError as error:
            if str(error) == "Provider is not running":
                return {"running": False}
            raise

    def list(self):
        return _send(_session(), "list")

    @staticmethod
    def batch_request(trajectory, *, data=("coords",), frames=None, start=None,
                      stop=None, atoms=None, next_frames=None):
        """Build one immutable batch specification for plan_batches/request."""
        if isinstance(data, str):
            data = [x.strip() for x in data.split(",") if x.strip()]
        if not isinstance(data, (tuple, list)):
            raise ValueError("data must be a list of coords, all, or descriptor names")
        coords = "coords" in data
        names = [name for name in data if name != "coords"]
        descriptors = "all" if names == ["all"] else names
        payload = {"trajectory": trajectory, "coordinates": coords, "descriptors": descriptors}
        if frames is not None:
            payload["frames"] = list(frames)
        if start is not None:
            payload["start"] = start
        if stop is not None:
            payload["stop"] = stop
        if atoms is not None:
            payload["atoms"] = list(atoms)
        if next_frames is not None:
            payload["next_frames"] = list(next_frames)
        return payload

    def request(self, trajectory, *, data=("coords",), frames=None, start=None,
                stop=None, atoms=None, next_frames=None, plan_id=None, plan_index=None):
        payload = self.batch_request(
            trajectory, data=data, frames=frames, start=start, stop=stop,
            atoms=atoms, next_frames=next_frames)
        if plan_id is not None or plan_index is not None:
            if not isinstance(plan_id, str) or type(plan_index) is not int:
                raise ValueError("Both plan_id and integer plan_index are required")
            payload.update(plan_id=plan_id, plan_index=plan_index)
        return Lease(self, _send(_session(), "get", payload, timeout=300))

    def storage_status(self):
        return _send(_session(), "storage/status")

    def configure_storage(self, *, enabled=None, external_root=None, cache_percent=None,
                          cache_limit_bytes=None, cache_slack_percent=None,
                          cache_slack_bytes=None, write_policy=None, prefetch_batches=None):
        options = {"enabled": enabled, "external_root": external_root,
                   "cache_percent": cache_percent, "cache_limit_bytes": cache_limit_bytes,
                   "cache_slack_percent": cache_slack_percent,
                   "cache_slack_bytes": cache_slack_bytes,
                   "write_policy": write_policy, "prefetch_batches": prefetch_batches}
        return _send(_session(), "storage/configure",
                     {k: v for k, v in options.items() if v is not None}, timeout=300)

    def offload(self, relative):
        return _send(_session(), "storage/offload", {"relative": relative}, timeout=7200)

    def ensure_storage(self, relative):
        return _send(_session(), "storage/ensure", {"relative": relative}, timeout=7200)

    def prune_storage(self, *, all_cache=False):
        return _send(_session(), "storage/prune", {"all_cache": all_cache}, timeout=300)

    def ingest_sources(self, trajectory):
        return _send(_session(), "storage/ingest", {"trajectory": trajectory}, timeout=7200)

    def plan_batches(self, batches):
        """Order is owned by the caller; only storage prefetch may run ahead."""
        return _send(_session(), "batches/plan", {"batches": batches})

    def batch_status(self, plan_id):
        return _send(_session(), "batches/status", {"plan_id": plan_id})

    def cancel_batches(self, plan_id):
        return _send(_session(), "batches/cancel", {"plan_id": plan_id})


    def release(self, lease):
        return _send(_session(), "release", {"lease": lease})

    def stop(self):
        session = _session(autostart=False)
        result = _send(session, "stop", {})
        # The HTTP response acknowledges shutdown, but the old server may
        # still answer a liveness probe while releasing its cache and lock.
        # Wait for its registration to be removed before allowing restart.
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if _registry().get("session") != session:
                return result
            time.sleep(0.05)
        raise ReaderError("Provider did not finish stopping")


def _indices(value):
    return [int(item) for item in value.split(",") if item]


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="reader.py", description="Request MiniTopol data from the local shared Provider.")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("start", "status", "list", "stop", "storage-status"):
        commands.add_parser(name)
    config = commands.add_parser("storage-configure")
    config.add_argument("--external")
    toggle = config.add_mutually_exclusive_group()
    toggle.add_argument("--enable", action="store_true")
    toggle.add_argument("--disable", action="store_true")
    budget = config.add_mutually_exclusive_group()
    budget.add_argument("--cache-percent", type=float)
    budget.add_argument("--cache-limit-gib", type=float)
    config.add_argument("--cache-slack-percent", type=float)
    config.add_argument("--cache-slack-gib", type=float)
    config.add_argument("--write-policy", choices=["longevity", "balanced", "throughput"])
    config.add_argument("--prefetch-batches", type=int)
    offload = commands.add_parser("storage-offload")
    offload.add_argument("relative")
    ensure = commands.add_parser("storage-ensure")
    ensure.add_argument("relative")
    prune = commands.add_parser("storage-prune")
    prune.add_argument("--all", action="store_true")
    ingest = commands.add_parser("storage-ingest")
    ingest.add_argument("trajectory")
    plan = commands.add_parser("batches-plan")
    plan.add_argument("json_file", help="JSON file containing the ordered batch list")
    plan_status = commands.add_parser("batches-status")
    plan_status.add_argument("plan_id")
    cancel = commands.add_parser("batches-cancel")
    cancel.add_argument("plan_id")
    get = commands.add_parser("get")
    get.add_argument("trajectory")
    get.add_argument("--data", default="coords", help="coords, all, or comma-separated descriptors")
    get.add_argument("--frames", type=_indices)
    get.add_argument("--start", type=int)
    get.add_argument("--stop", type=int)
    get.add_argument("--atoms", type=_indices)
    get.add_argument("--plan-id")
    get.add_argument("--plan-index", type=int)
    release = commands.add_parser("release")
    release.add_argument("lease")
    args = parser.parse_args(argv)
    client = Reader()
    if args.command == "get":
        result = client.request(args.trajectory, data=args.data, frames=args.frames,
                                start=args.start, stop=args.stop, atoms=args.atoms,
                                plan_id=args.plan_id, plan_index=args.plan_index).metadata
    elif args.command == "storage-status":
        result = client.storage_status()
    elif args.command == "storage-configure":
        import math
        for name, value in (("--cache-limit-gib", args.cache_limit_gib),
                            ("--cache-slack-gib", args.cache_slack_gib)):
            if value is not None and (not math.isfinite(value) or value < 0):
                parser.error(f"{name} must be a nonnegative finite number")
        result = client.configure_storage(
            external_root=args.external,
            enabled=True if args.enable else (False if args.disable else None),
            cache_percent=args.cache_percent,
            cache_limit_bytes=(round(args.cache_limit_gib * 1024**3)
                               if args.cache_limit_gib is not None else None),
            cache_slack_percent=args.cache_slack_percent,
            cache_slack_bytes=(round(args.cache_slack_gib * 1024**3)
                               if args.cache_slack_gib is not None else None),
            write_policy=args.write_policy, prefetch_batches=args.prefetch_batches)
    elif args.command == "storage-offload":
        result = client.offload(args.relative)
    elif args.command == "storage-ensure":
        result = client.ensure_storage(args.relative)
    elif args.command == "storage-prune":
        result = client.prune_storage(all_cache=args.all)
    elif args.command == "storage-ingest":
        result = client.ingest_sources(args.trajectory)
    elif args.command == "batches-plan":
        result = client.plan_batches(json.loads(Path(args.json_file).read_text(encoding="utf-8")))
    elif args.command == "batches-status":
        result = client.batch_status(args.plan_id)
    elif args.command == "batches-cancel":
        result = client.cancel_batches(args.plan_id)
    elif args.command == "release":
        result = client.release(args.lease)
    else:
        result = getattr(client, args.command)()
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ReaderError as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1)
