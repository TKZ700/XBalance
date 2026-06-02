import json
import os
import subprocess
import sys
import urllib.request
import zipfile
from typing import Any, Dict, List

from colorama import Fore

XRAY_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "xray"
)
XRAY_PATH = os.path.join(XRAY_DIR, "xray.exe")

XRAY_DOWNLOAD_URL = (
    "https://github.com/XTLS/Xray-core/releases/latest/download/Xray-windows-64.zip"
)


def cleanup_stale_configs():
    """Removes any leftover temp_config_*.json files from previous runs."""
    import glob as globmod
    pattern = os.path.join(XRAY_DIR, "temp_config_*.json")
    for f in globmod.glob(pattern):
        try:
            os.remove(f)
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

    try:
        urllib.request.urlretrieve(XRAY_DOWNLOAD_URL, zip_path)
        print(f"{Fore.YELLOW}[*] Extracting...{Fore.RESET}")

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(XRAY_DIR)

        if os.path.exists(zip_path):
            os.remove(zip_path)

        print(f"{Fore.GREEN}[+] Xray-core downloaded successfully!{Fore.RESET}")
        return XRAY_PATH
    except Exception as e:
        print(f"{Fore.RED}[-] Error downloading Xray: {e}{Fore.RESET}")
        print(f"{Fore.RED}[-] Download xray.exe manually and place it in the 'xray/' folder.{Fore.RESET}")
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

    def start_workers(
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
                    }
                ],
                "outbounds": [outbound],
            }

            config_path = os.path.join(XRAY_DIR, f"temp_config_{port}.json")
            try:
                with open(config_path, "w", encoding="utf-8") as f:
                    json.dump(config_data, f, indent=2)
                self.temp_configs.append(config_path)

                # Capture stderr so we can log failures
                stderr_log = subprocess.PIPE

                process = subprocess.Popen(
                    [self.binary_path, "-config", config_path],
                    stdout=subprocess.DEVNULL,
                    stderr=stderr_log,
                    creationflags=0x08000000 if sys.platform == "win32" else 0,
                )

                # Check if process started successfully after a brief moment
                import time
                time.sleep(0.15)

                if process.poll() is not None:
                    # Process already exited — read error
                    stderr_data = b""
                    if process.stderr:
                        stderr_data = process.stderr.read()
                    err_msg = stderr_data.decode("utf-8", errors="replace").strip()
                    print(f"{Fore.RED}[-] Worker on port {port} failed to start: {err_msg}{Fore.RESET}")
                    continue

                self.processes.append(process)
                active_ports.append(port)

            except Exception as e:
                print(f"{Fore.RED}[-] Failed to start worker on port {port}: {e}{Fore.RESET}")

        return active_ports

    def stop_all(self):
        """Kills all workers and cleans up temp config files."""
        print(f"{Fore.YELLOW}[*] Terminating all background workers...{Fore.RESET}")

        for process in self.processes:
            try:
                process.terminate()
                process.wait(timeout=2)
            except Exception:
                try:
                    process.kill()
                except Exception:
                    pass
        self.processes.clear()

        for temp_file in self.temp_configs:
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception:
                    pass
        self.temp_configs.clear()

        print(f"{Fore.GREEN}[+] Cleanup done.{Fore.RESET}")
