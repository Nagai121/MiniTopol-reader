"""Live cross-repository integration; run only in the CI e2e job."""
import gc
import warnings

import MDAnalysis as mda
import numpy as np
from minitopol import Workspace, convert
from minitopol.descriptors import analyze
from minitopol.provider.runtime import register

from reader import Reader


def test_reader_to_real_provider_from_unrelated_directory(tmp_path, monkeypatch):
    reg = tmp_path / "private" / "provider.json"
    monkeypatch.setenv("MINITOPOL_PROVIDER_REGISTRY", str(reg))
    universe = mda.Universe.empty(6, n_residues=2,
                                 atom_resindex=[0, 0, 0, 1, 1, 1], trajectory=True)
    universe.add_TopologyAttr("names", ["N", "CA", "H", "OW", "HW1", "HW2"])
    universe.add_TopologyAttr("resnames", ["ALA", "SOL"])
    universe.add_TopologyAttr("resids", [1, 2])
    universe.dimensions = [30, 31, 32, 90, 90, 80]
    pos = np.array([[1, 2, 3], [4, 5, 6], [1, 2, 4],
                    [8, 8, 8], [9, 8, 8], [8, 9, 8]], dtype=np.float32)
    universe.atoms.positions = pos
    gro, xtc = tmp_path / "input.gro", tmp_path / "input.xtc"
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        universe.atoms.write(str(gro))
    with mda.Writer(str(xtc), n_atoms=6, precision=4) as writer:
        for frame in range(9):
            universe.atoms.positions = pos + np.array([5*frame, 0, 0])
            universe.trajectory.ts.time = frame * 2.5
            writer.write(universe.atoms)

    ws = Workspace(tmp_path / "storage")
    tid = convert(ws, gro, xtc, solvent_resnames=["SOL"], chunk_frames=3)
    report = analyze(ws, tid, descriptors=["sasa", "ramachandran"],
                     coordinate_mode="nonperiodic")
    assert report["status"] == "complete", report
    register(ws.path)
    client = Reader()
    try:
        assert client.status() == {"running": False}
        with client.request(tid, data=["coords", "sasa", "ramachandran"],
                            frames=[8, 0], atoms=[1, 0]) as lease:
            assert lease.metadata["frames"] == [8, 0]
            xyz = lease.numpy("coordinates")
            np.testing.assert_allclose(xyz[0, 0], [44, 5, 6], atol=.002)
            np.testing.assert_allclose(xyz[1, 1], [1, 2, 3], atol=.002)
            sas = lease.arrow("sasa")
            ram = lease.arrow("ramachandran")
            assert set(sas["frame"].to_pylist()) == {0, 8}
            assert set(ram["frame"].to_pylist()) == {0, 8}
            assert sas.num_rows == ws.query_results(descriptor="sasa", trajectory=tid,
                                                     start=0, stop=1).num_rows * 2
            del xyz, sas, ram
            gc.collect()
        assert client.status()["active_leases"] == 0
        with client.request(tid, data=["coords", "sasa", "ramachandran"],
                            frames=[8, 0], atoms=[1, 0]) as repeated:
            assert repeated.metadata["buffers"]
        assert client.status()["cache_hits"] >= 1
    finally:
        client.stop()


def test_immediate_stop_restart_is_ready(tmp_path, monkeypatch):
    import json
    ws = Workspace(tmp_path / "restart-storage")
    registration = tmp_path / "restart-provider.json"
    monkeypatch.setenv("MINITOPOL_PROVIDER_REGISTRY", str(registration))
    register(ws.path)
    client = Reader()
    try:
        for _ in range(5):
            assert client.start()["protocol"] == 1
            assert client.stop()["stopping"]
            assert "session" not in json.loads(registration.read_text(encoding="utf-8"))
    finally:
        if client.status().get("running") is not False:
            client.stop()
