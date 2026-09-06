"""
Architecture enforcement test suite.

Verifies strict non-negotiable architectural boundaries across the codebase:
1. No module calls clock/time directly outside peerq/clock.py.
2. No module touches sockets/raw network outside peerq/transport.py.
3. No module uses global random; must use injected random.Random instance.
4. Single-threaded asyncio only; no threading or multiprocessing.
"""

import ast
from pathlib import Path


def get_peerq_sources() -> list[Path]:
    root = Path(__file__).resolve().parent.parent.parent / "peerq"
    return [p for p in root.glob("**/*.py") if p.is_file()]


def test_no_forbidden_time_calls() -> None:
    """Ensure time/datetime/asyncio.sleep are restricted strictly to peerq/clock.py."""
    forbidden_modules = {"time", "datetime"}
    sources = get_peerq_sources()

    for path in sources:
        if path.name == "clock.py":
            continue

        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert alias.name not in forbidden_modules, (
                        f"Forbidden import '{alias.name}' in {path.name}. "
                        "All time must flow through injected Clock protocol."
                    )
            elif isinstance(node, ast.ImportFrom):
                assert node.module not in forbidden_modules, (
                    f"Forbidden import from '{node.module}' in {path.name}. "
                    "All time must flow through injected Clock protocol."
                )
                if node.module == "asyncio":
                    for alias in node.names:
                        assert alias.name != "sleep", (
                            f"Forbidden import 'asyncio.sleep' in {path.name}. "
                            "Use injected clock.sleep() instead."
                        )
            elif (
                isinstance(node, ast.Attribute)
                and node.attr == "sleep"
                and isinstance(node.value, ast.Name)
            ):
                assert node.value.id != "asyncio", (
                    f"Forbidden direct call 'asyncio.sleep()' in {path.name}. "
                    "Use injected clock.sleep() instead."
                )


def test_no_forbidden_network_calls() -> None:
    """Ensure socket and raw connection calls are restricted strictly to peerq/transport.py."""
    sources = get_peerq_sources()

    for path in sources:
        if path.name == "transport.py":
            continue

        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert alias.name != "socket", (
                        f"Forbidden import 'socket' in {path.name}. "
                        "All network I/O must flow through injected Transport protocol."
                    )
            elif isinstance(node, ast.ImportFrom):
                assert node.module != "socket", (
                    f"Forbidden import from 'socket' in {path.name}. "
                    "All network I/O must flow through injected Transport protocol."
                )
                if node.module == "asyncio":
                    for alias in node.names:
                        assert alias.name not in {"open_connection", "start_server"}, (
                            f"Forbidden import 'asyncio.{alias.name}' in {path.name}. "
                            "Network opening restricted to peerq/transport.py."
                        )


def test_no_global_random_usage() -> None:
    """Ensure no module calls global random functions directly; must use injected Random."""
    global_random_funcs = {
        "random",
        "randint",
        "choice",
        "choices",
        "sample",
        "shuffle",
        "uniform",
        "gauss",
        "seed",
    }
    sources = get_peerq_sources()

    for path in sources:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "random":
                for alias in node.names:
                    assert alias.name not in global_random_funcs, (
                        f"Forbidden direct import 'from random import {alias.name}' in "
                        f"{path.name}. Inject a seeded random.Random instance instead."
                    )
            elif (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Name)
                and node.value.id == "random"
            ):
                assert node.attr not in global_random_funcs, (
                    f"Forbidden call 'random.{node.attr}' in {path.name}. "
                    "Use an injected random.Random instance instead."
                )


def test_no_multithreading_or_multiprocessing() -> None:
    """Ensure strict single-threaded asyncio: no threading or multiprocessing."""
    forbidden = {"threading", "multiprocessing", "concurrent.futures"}
    sources = get_peerq_sources()

    for path in sources:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert alias.name not in forbidden, (
                        f"Forbidden concurrency module '{alias.name}' in {path.name}. "
                        "peerq is strictly single-threaded asyncio."
                    )
            elif isinstance(node, ast.ImportFrom):
                assert node.module not in forbidden, (
                    f"Forbidden concurrency module '{node.module}' in {path.name}. "
                    "peerq is strictly single-threaded asyncio."
                )
