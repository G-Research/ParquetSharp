import asyncio
import contextlib
import logging
import os
import signal
import socket
import subprocess
import sys
import tempfile

import aiohttp
import aiofiles

LOCALHOST = "127.0.0.1"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


class ProxyStartError(RuntimeError):
    pass


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((LOCALHOST, 0))
        return s.getsockname()[1]


async def wait_until_healthy(proc: subprocess.Popen, port: int):
    url = f"http://{LOCALHOST}:{port}/health"
    async with aiohttp.ClientSession() as session:
        for x in range(20):
            if proc.poll() is not None:
                raise ProxyStartError(
                    f"Proxy process (PID {proc.pid}) exited unexpectedly with code {proc.returncode}"
                )

            try:
                async with session.get(url, timeout=0.5) as response:
                    if response.ok:
                        return
            except Exception:
                pass

            await asyncio.sleep(0.5)
    raise ProxyStartError("Proxy did not become healthy in time")


async def tail(log_path: str):
    async with aiofiles.open(log_path, mode="rb") as log_file:
        while True:
            buffer = await log_file.read(128)
            if buffer == b"":
                await asyncio.sleep(0.1)
            await aiofiles.stderr_bytes.write(buffer)
            await aiofiles.stderr.flush()


def isGitHubActions() -> bool:
    return os.getenv("GITHUB_ACTIONS") == "true"


def isPost() -> bool:
    return get_state("isPost") == "true"


def get_state(key: str) -> str | None:
    return os.getenv(f"STATE_{key}")


async def set_state(key: str, value: str) -> None:
    if isGitHubActions():
        async with aiofiles.open(os.getenv("GITHUB_STATE"), mode="a") as state_file:
            await state_file.write(f"{key}={value}\n")
    else:
        logger.info(f"STATE {key}={value}")


async def set_env(key: str, value: str) -> None:
    if isGitHubActions():
        async with aiofiles.open(os.getenv("GITHUB_ENV"), mode="a") as env_file:
            await env_file.write(f"{key}={value}\n")
    else:
        logger.info(f"ENV {key}={value}")


def proc_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


async def wait_for_proc(pid: int) -> None:
    while proc_exists(pid):
        await asyncio.sleep(0.1)


async def start() -> None:
    port = find_free_port()

    cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "--port",
        str(port),
        "--use-colors",
        "proxy:app",
    ]

    log_fd, log_path = tempfile.mkstemp()
    tail_task = asyncio.create_task(tail(log_path))
    proc: subprocess.Popen | None = None

    try:
        # asyncio.subprocess does not allow us to create detached processes on
        # Windows so we use subprocess.Popen here
        proc = subprocess.Popen(
            cmd,
            stdout=log_fd,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
        )

        await wait_until_healthy(proc, port)

        await set_state("isPost", "true")
        await set_state("pid", str(proc.pid))
        await set_state("log", log_path)
        await set_env(
            "VCPKG_BINARY_SOURCES",
            f"clear;http,http://{LOCALHOST}:{port}/"
            + "{name}/{triplet}/{version}/{sha},readwrite",
        )

    except Exception as e:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(5)
            if proc.poll() is None:
                proc.kill()
                proc.wait()

        os.unlink(log_path)

        if isinstance(e, ProxyStartError):
            logger.error(e)
            exit(1)

        raise e

    finally:
        if tail_task:
            await asyncio.sleep(0.5)  # Allow some time for final log output
            tail_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await tail_task


async def stop() -> None:
    pid = int(get_state("pid"))
    log_path = get_state("log")
    tail_task = asyncio.create_task(tail(log_path))

    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        pass
    else:
        async with asyncio.timeout(10):
            await wait_for_proc(pid)
        if proc_exists(pid):
            os.kill(pid, signal.SIGKILL)
            await wait_for_proc(pid)

    if tail_task:
        await asyncio.sleep(0.5)  # Allow some time for final log output
        tail_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await tail_task

    os.unlink(log_path)


async def main() -> None:
    if isPost():
        await stop()
    else:
        await start()


if __name__ == "__main__":
    asyncio.run(main())
