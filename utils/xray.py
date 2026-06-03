import asyncio
import glob as globmod
import json
import logging
import os
import subprocess
import sys
import urllib.request
import zipfile
from typing import Any, Dict, List

from colorama import Fore

log = logging.getLogger("xbalance.xray")

XRAY_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "xray"
)
XRAY_PATH = os.path.join(XRAY_DIR, "xray.exe")

XRAY_DOWNLOAD_URL = (
    "https://github.com/XTLS/Xray-core/releases/latest/download/Xray-windows-64.zip"
)


def kill_stale_xray():
    """Kills all orphan xray.exe processes that may have been left from previous runs."""
    try:
        result = subprocess.run(
            ["taskkill", "/F", "/IM", "xray.exe"],
            capture_output=True, creationflags=0x08000000 if sys.platform == "win32" else 0,
        )
        if result.returncode == 0:
            output = result.stdout.decode("utf-8", errors="replace").strip()
            count = output.count("xray.exe")
            if count > 0:
                log.info("Killed %d orphan xray.exe process(es)", count)
                print(f"{Fore.YELLOW}[*] Killed {count} orphan xray.exe process(es){Fore.RESET}")
        else:
            # taskkill returns 128 when no processes found — that's fine
            err = result.stderr.decode("utf-8", errors="replace").strip()
            if "not found" not in err.lower() and result.returncode != 128:
                log.warning("taskkill output: %s", err)
    except FileNotFoundError:
        # taskkill not available (non-Windows), fall back to ps-based kill
        try:
            subprocess.run(["pkill", "-f", "xray"], capture_output=True)
        except Exception:
            pass
    except Exception as e:
        log.warning("Failed to kill stale xray processes: %s", e)


def cleanup_stale_configs():
    """Removes any leftover temp_config_*.json files from previous runs."""
    pattern = os.path.join(XRAY_DIR, "temp_config_*.json")
    for f in globmod.glob(pattern):
        try:
            os.remove(f)
            log.debug("Removed stale config: %s", f)
        except Exception:
            pass


def ensure_xray_binary() -> str:
    """Downloads and extracts xray binary if not present."""
    cleanup_stale_configs()
    if os.path.exists(XRAY_PATH):
        return XRAY_PATH

    os.makedirs(XRAY_DIR, exist_ok=True)
    zip_path = os.path.join(XRAY_DIR, "xray.zip")

    print(f"{Fore.YELLOW}[*] Xray binary not found. Downloading...{Fore.RESET}")
    print(f"{Fore.YELLOW}[*] {XRAY_DOWNLOAD_URL}{Fore.RESET}")
    log.info("Downloading xray binary from %s", XRAY_DOWNLOAD_URL)

    try:
        urllib.request.urlretrieve(XRAY_DOWNLOAD_URL, zip_path)
        print(f"{Fore.YELLOW}[*] Extracting...{Fore.RESET}")

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(XRAY_DIR)

        if os.path.exists(zip_path):
            os.remove(zip_path)

        print(f"{Fore.GREEN}[+] Xray-core downloaded successfully!{Fore.RESET}")
        log.info("Xray binary extracted to %s", XRAY_PATH)
        return XRAY_PATH
    except Exception as e:
        print(f"{Fore.RED}[-] Error downloading Xray: {e}{Fore.RESET}")
        print(f"{Fore.RED}[-] Download xray.exe manually and place it in the 'xray/' folder.{Fore.RESET}")
        log.error("Failed to download xray: %s", e)
        sys.exit(1)


class XrayWorkerManager:
    """
    Manages multiple isolated background xray subprocesses.
    Each runs a local SOCKS5 inbound linked to one remote configuration.
    """

    def __init__(self, binary_path: str):
        self.binary_path = binary_path
        self.processes: List[subprocess.Popen] = []
        self.temp_configs: List[str] = []

    async def start_workers(
        self, outbounds: List[Dict[str, Any]], start_port: int = 20001
    ) -> List[int]:
        """Starts one Xray process per outbound config on sequential local ports."""
        active_ports: List[int] = []

        for index, outbound in enumerate(outbounds):
            port = start_port + index

            config_data = {
                "log": {"loglevel": "warning"},
                "inbounds": [
                    {
                        "port": port,
                        "listen": "127.0.0.1",
                        "protocol": "socks",
                        "settings": {"auth": "noauth", "udp": True},
                        "sniffing": {
                            "enabled": True,
                            "destOverride": ["http", "tls"],
                        },
                    }
                ],
                "outbounds": [outbound],
            }

            config_path = os.path.join(XRAY_DIR, f"temp_config_{port}.json")
            try:
                with open(config_path, "w", encoding="utf-8") as f:
                    json.dump(config_data, f)
                self.temp_configs.append(config_path)

                creation_flags = 0x08000000 if sys.platform == "win32" else 0
                process = subprocess.Popen(
                    [self.binary_path, "-config", config_path],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    creationflags=creation_flags,
                )

                await asyncio.sleep(0.1)

                if process.poll() is not None:
                    stderr_data = process.stderr.read() if process.stderr else b""
                    err_msg = stderr_data.decode("utf-8", errors="replace").strip()
                    print(f"{Fore.RED}[-] Worker on port {port} failed to start: {err_msg}{Fore.RESET}")
                    log.error("Worker port %d failed: %s", port, err_msg)
                    continue

                self.processes.append(process)
                active_ports.append(port)
                log.info("Worker started: port %d (pid=%d)", port, process.pid)

            except Exception as e:
                print(f"{Fore.RED}[-] Failed to start worker on port {port}: {e}{Fore.RESET}")
                log.error("Failed to start worker port %d: %s", port, e)

        return active_ports

    def stop_all(self):
        """Kills all workers and cleans up temp config files."""
        print(f"{Fore.YELLOW}[*] Terminating all background workers...{Fore.RESET}")
        log.info("Stopping %d worker process(es)", len(self.processes))

        for process in self.processes:
            try:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=2)
                log.debug("Stopped worker pid=%d", process.pid)
            except Exception as e:
                log.warning("Failed to stop worker pid=%d: %s", process.pid, e)
                try:
                    process.kill()
                except Exception:
                    pass
        self.processes.clear()

        for temp_file in self.temp_configs:
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                    log.debug("Removed temp config: %s", temp_file)
                except Exception:
                    pass
        self.temp_configs.clear()

        print(f"{Fore.GREEN}[+] Cleanup done.{Fore.RESET}")
        log.info("All workers stopped and temp configs cleaned up")
