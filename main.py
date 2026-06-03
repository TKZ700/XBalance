import asyncio
import base64
import logging
import os
import sys
import urllib.request
from typing import List

from colorama import Fore
from colorama import init as colorama_init

from utils.balancer import AsyncSOCKS5Balancer
from utils.parser import parse_uri_to_outbound
from utils.xray import (
    XrayWorkerManager,
    cleanup_stale_configs,
    ensure_xray_binary,
    kill_stale_xray,
)

BALANCER_HOST = "0.0.0.0"
BALANCER_PORT = 7070
CONFIGS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "configs.txt")
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "xbalance.log")

BANNER = f"""
{Fore.CYAN}================================ XBalance ================================
{Fore.CYAN}  Multi-Socket SOCKS5 Load Balancer  |  v0.1.1-alpha{Fore.RESET}
{Fore.CYAN}========================================================================={Fore.RESET}
"""


def setup_logging():
    """Configure logging to both file and console."""
    logger = logging.getLogger("xbalance")
    logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(logging.Formatter("%(message)s"))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logging.getLogger("xbalance.main")


def load_raw_configs() -> List[str]:
    """Reads configs from configs.txt. Fetches subscriptions if URLs are found."""
    log = logging.getLogger("xbalance.main")

    if not os.path.exists(CONFIGS_FILE):
        with open(CONFIGS_FILE, "w", encoding="utf-8") as f:
            f.write(
                "# Paste your V2Ray configs or Subscription URLs below, one per line.\n"
            )
        return []

    configs: List[str] = []
    with open(CONFIGS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            if line.startswith("http://") or line.startswith("https://"):
                print(f"{Fore.YELLOW}[*] Fetching subscription: {line}{Fore.RESET}")
                log.info("Fetching subscription: %s", line)
                try:
                    headers = {
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/110.0.0.0"
                    }
                    req = urllib.request.Request(line, headers=headers)
                    with urllib.request.urlopen(req, timeout=15) as response:
                        raw_data = response.read().strip()

                        try:
                            missing = len(raw_data) % 4
                            if missing:
                                raw_data += b"=" * (4 - missing)
                            decoded = base64.b64decode(raw_data).decode("utf-8")
                        except Exception:
                            decoded = raw_data.decode("utf-8")

                        sub_links = decoded.splitlines()
                        count = sum(
                            1
                            for ln in sub_links
                            if ln.strip() and not ln.strip().startswith("#")
                        )
                        print(
                            f"{Fore.GREEN}[+] Fetched {count} configs from subscription!{Fore.RESET}"
                        )
                        log.info("Fetched %d configs from subscription", count)
                        for sub_link in sub_links:
                            sub_link = sub_link.strip()
                            if sub_link and not sub_link.startswith("#"):
                                configs.append(sub_link)
                except Exception as e:
                    print(
                        f"{Fore.RED}[-] Failed to fetch subscription: {e}{Fore.RESET}"
                    )
                    log.error("Failed to fetch subscription %s: %s", line, e)
            else:
                configs.append(line)

    return configs


async def stats_printer_loop(balancer: AsyncSOCKS5Balancer):
    """Prints connection stats to console every 2 seconds."""
    try:
        while True:
            await asyncio.sleep(2)
            print("\033c", end="")
            print(BANNER)
            print(
                f"  {Fore.CYAN}Proxy Address :{Fore.RESET}  socks://Og@{balancer.host}:{balancer.port}#XBalance"
            )
            print(
                f"  {Fore.CYAN}Active        :{Fore.RESET}  {balancer.active_connections} connections"
            )
            print(
                f"  {Fore.CYAN}Total Routed  :{Fore.RESET}  {balancer.rr_index} requests"
            )
            print(
                f"  {Fore.CYAN}Workers       :{Fore.RESET}  {len(balancer.worker_ports)}"
            )
            print()
            print(f"  {Fore.WHITE}Worker Distribution:{Fore.RESET}")
            for port, count in balancer.connection_stats.items():
                bar = "█" * min(count, 50)
                print(
                    f"    {Fore.GREEN}Port {port}{Fore.RESET} : {count:>5} reqs  {Fore.YELLOW}{bar}{Fore.RESET}"
                )
            print()
            print(f"  {Fore.WHITE}Press Ctrl+C to stop.{Fore.RESET}")
    except asyncio.CancelledError:
        pass


async def main():
    """Main entrypoint: parse configs, start xray workers, run balancer."""
    log = setup_logging()
    colorama_init()
    print(BANNER)
    log.info("XBalance starting up")

    # 0. Kill any orphan xray processes from previous runs
    kill_stale_xray()

    # 1. Ensure xray binary
    xray_bin = ensure_xray_binary()

    # 2. Load raw config URIs
    raw_uris = load_raw_configs()
    if not raw_uris:
        print(f"{Fore.RED}[-] No configs found in configs.txt{Fore.RESET}")
        print(
            f"{Fore.RED}[-] Add your vmess://, vless://, trojan://, ss:// links or subscription URLs and try again.{Fore.RESET}"
        )
        return

    # 3. Parse into Xray outbound structures
    outbounds: List[dict] = []
    for uri in raw_uris:
        outbound = parse_uri_to_outbound(uri)
        if outbound:
            outbounds.append(outbound)

    if not outbounds:
        print(
            f"{Fore.RED}[-] None of the configs could be parsed. Check your config format.{Fore.RESET}"
        )
        return

    print(f"{Fore.GREEN}[+] Parsed {len(outbounds)} configs successfully.{Fore.RESET}")
    log.info("Parsed %d outbound configs", len(outbounds))

    # 4. Start Xray workers
    manager = XrayWorkerManager(xray_bin)
    print(f"{Fore.YELLOW}[*] Starting {len(outbounds)} local workers...{Fore.RESET}")
    worker_ports = await manager.start_workers(outbounds)

    if not worker_ports:
        print(
            f"{Fore.RED}[-] No workers started. Check your configs and try again.{Fore.RESET}"
        )
        return

    print(f"{Fore.GREEN}[+] {len(worker_ports)} workers running!{Fore.RESET}")
    log.info("%d workers running on ports %s", len(worker_ports), worker_ports)

    # 5. Start the SOCKS5 balancer
    balancer = AsyncSOCKS5Balancer(BALANCER_HOST, BALANCER_PORT, worker_ports)

    stats_task = asyncio.create_task(stats_printer_loop(balancer))

    try:
        await balancer.start()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        stats_task.cancel()
        manager.stop_all()
        log.info("XBalance shutdown complete")


def main_cli():
    """CLI entrypoint."""
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print(f"\n{Fore.YELLOW}[*] XBalance stopped.{Fore.RESET}")
    finally:
        cleanup_stale_configs()


if __name__ == "__main__":
    main_cli()
