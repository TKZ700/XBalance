import argparse
import asyncio
import base64
import logging
import os
import sys
import time
import urllib.request
from typing import List

from colorama import Fore
from colorama import init as colorama_init
from rich.console import Console, Group as RenderGroup
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.align import Align

from utils.balancer import AsyncSOCKS5Balancer
from utils.http_proxy import HTTPConnectProxy
from utils.parser import parse_uri_to_outbound
from utils.worker_selector import create_selector
from utils.xray import (
    XrayWorkerManager,
    cleanup_stale_configs,
    ensure_xray_binary,
    kill_stale_xray,
)

BALANCER_HOST = "0.0.0.0"
BALANCER_PORT = 7070
HTTP_PROXY_PORT = 7071
CONFIGS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "configs.txt")
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "xbalance.log")

console = Console()


def setup_logging():
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
    log = logging.getLogger("xbalance.main")

    if not os.path.exists(CONFIGS_FILE):
        with open(CONFIGS_FILE, "w", encoding="utf-8") as f:
            f.write("# Paste your V2Ray configs or Subscription URLs below, one per line.\n")
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
                        count = sum(1 for ln in sub_links if ln.strip() and not ln.strip().startswith("#"))
                        print(f"{Fore.GREEN}[+] Fetched {count} configs from subscription!{Fore.RESET}")
                        log.info("Fetched %d configs from subscription", count)
                        for sub_link in sub_links:
                            sub_link = sub_link.strip()
                            if sub_link and not sub_link.startswith("#"):
                                configs.append(sub_link)
                except Exception as e:
                    print(f"{Fore.RED}[-] Failed to fetch subscription: {e}{Fore.RESET}")
                    log.error("Failed to fetch subscription %s: %s", line, e)
            else:
                configs.append(line)

    return configs


def _make_worker_bar(up: int, total: int, width: int = 30) -> Text:
    """Build a colored progress bar text."""
    t = Text()
    ratio = up / total if total > 0 else 0
    filled = int(ratio * width)
    color = "green" if ratio > 0.8 else "yellow" if ratio > 0.5 else "red"
    t.append("[" , style="dim")
    t.append("#" * filled, style=color)
    t.append("-" * (width - filled), style="dim")
    t.append("]", style="dim")
    return t


def _make_mini_bar(conns: int, max_conns: int) -> Text:
    """Build a small bar for per-worker connection load."""
    t = Text()
    width = 10
    ratio = conns / max_conns if max_conns > 0 else 0
    filled = int(ratio * width)
    color = "green" if ratio > 0.6 else "cyan" if ratio > 0.3 else "dim"
    t.append("=" * filled, style=color)
    t.append("." * (width - filled), style="dim")
    return t


def _build_dashboard(balancer: AsyncSOCKS5Balancer, http_proxy, start_time: float):
    """Build the rich renderable for the live display."""
    selector = balancer.selector
    ports = balancer.worker_ports
    total_workers = len(ports)
    unhealthy = selector.stats.unhealthy
    up = total_workers - len(unhealthy)

    total_conns = sum(selector.stats.active_conns.values())
    total_reqs = sum(selector.stats.total_requests.values())

    measured = [selector.stats.avg_response_time(p) for p in ports if selector.stats.avg_response_time(p) > 0]
    avg_rt = sum(measured) / len(measured) if measured else 0.0

    strategy = type(selector).__name__.replace("Selector", "")
    uptime = int(time.time() - start_time)
    uptime_str = f"{uptime // 3600}h {(uptime % 3600) // 60}m {uptime % 60}s"

    # ── Header ──
    header = Text()
    header.append("  X B A L A N C E", style="bold white")
    header.append("  v0.2.0", style="dim")
    header.append(f"    {uptime_str}", style="dim")

    # ── Stats grid ──
    stats_table = Table(show_header=False, box=None, padding=(0, 2), expand=True)
    stats_table.add_column(ratio=1)
    stats_table.add_column(ratio=1)
    stats_table.add_column(ratio=1)
    stats_table.add_column(ratio=1)

    strat_text = Text(strategy, style="bold cyan")
    worker_text = Text()
    worker_text.append(str(up), style="bold green" if up == total_workers else "bold yellow")
    worker_text.append(f"/{total_workers}", style="dim")

    conns_text = Text(str(total_conns), style="bold white")
    rt_text = Text(f"{avg_rt:.0f}ms" if avg_rt > 0 else "-", style="bold magenta" if avg_rt > 0 else "dim")

    reqs_text = Text(f"{total_reqs:,}", style="bold white")

    stats_table.add_row(
        Text("strategy", style="dim"), strat_text,
        Text("workers", style="dim"), worker_text,
    )
    stats_table.add_row(
        Text("active", style="dim"), conns_text,
        Text("avg rt", style="dim"), rt_text,
    )
    stats_table.add_row(
        Text("total", style="dim"), reqs_text,
        Text("", style="dim"), Text(""),
    )

    # ── Worker bar ──
    bar_text = Text()
    bar_text.append("  ", style="dim")
    bar_text.append_text(_make_worker_bar(up, total_workers, 30))
    bar_text.append(f"  {up}/{total_workers}", style="dim")

    # ── Worker table ──
    max_conns = max((selector.stats.active_conns[p] for p in ports), default=1) or 1

    worker_table = Table(
        show_header=True, header_style="bold dim",
        expand=True, padding=(0, 1),
        show_edge=False,
    )
    worker_table.add_column("status", width=2, justify="center")
    worker_table.add_column("port", style="bold", justify="right")
    worker_table.add_column("conns", justify="right")
    worker_table.add_column("requests", justify="right")
    worker_table.add_column("latency", justify="right")
    worker_table.add_column("load", min_width=12)

    show_individual = total_workers <= 16

    if show_individual:
        for port in ports:
            is_down = port in unhealthy
            status = Text("x", style="bold red") if is_down else Text("o", style="bold green")
            port_text = Text(f"{port}", style="dim" if is_down else "white")

            conns = selector.stats.active_conns[port]
            total = selector.stats.total_requests[port]
            rt = selector.stats.avg_response_time(port)

            conns_text = Text(str(conns), style="dim" if conns == 0 else "cyan")
            total_text = Text(f"{total:,}", style="dim" if total == 0 else "white")
            rt_text = Text(f"{rt:.0f}ms", style="magenta") if rt > 0 else Text("-", style="dim")
            bar = _make_mini_bar(conns, max_conns)

            worker_table.add_row(status, port_text, conns_text, total_text, rt_text, bar)
    else:
        top = sorted(ports, key=lambda p: selector.stats.active_conns[p], reverse=True)[:8]
        for port in top:
            is_down = port in unhealthy
            status = Text("x", style="bold red") if is_down else Text("o", style="bold green")
            conns = selector.stats.active_conns[port]
            total = selector.stats.total_requests[port]
            rt = selector.stats.avg_response_time(port)

            worker_table.add_row(
                status,
                Text(f"{port}", style="white"),
                Text(str(conns), style="cyan" if conns > 0 else "dim"),
                Text(f"{total:,}", style="white" if total > 0 else "dim"),
                Text(f"{rt:.0f}ms", style="magenta") if rt > 0 else Text("-", style="dim"),
                _make_mini_bar(conns, max_conns),
            )
        if len(ports) > 8:
            remaining = len(ports) - 8
            worker_table.add_row(
                Text(""),
                Text(f"+{remaining}", style="dim italic"),
                Text(""), Text(""), Text(""), Text(""),
            )

    # ── Footer ──
    footer = Text()
    footer.append("  ctrl+c to stop", style="dim italic")

    # ── Assemble as group of renderables ──
    return Panel(
        RenderGroup(
            header,
            Text(""),
            stats_table,
            bar_text,
            Text(""),
            worker_table,
            footer,
        ),
        border_style="cyan",
        padding=(1, 2),
    )


async def stats_printer_loop(
    balancer: AsyncSOCKS5Balancer,
    http_proxy=None,
):
    """Live TUI dashboard using rich."""
    start_time = time.time()

    with Live(console=console, refresh_per_second=1, screen=True) as live:
        try:
            while True:
                await asyncio.sleep(1)
                dashboard = _build_dashboard(balancer, http_proxy, start_time)
                live.update(dashboard)
        except asyncio.CancelledError:
            pass


def parse_args():
    parser = argparse.ArgumentParser(
        description="XBalance - Multi-Socket Load Balancer for V2Ray/Xray",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--strategy",
        choices=["round-robin", "least-conn", "response-time", "weighted"],
        default="round-robin",
        help="Worker selection strategy (default: round-robin)",
    )
    parser.add_argument(
        "--socks-port",
        type=int,
        default=BALANCER_PORT,
        help=f"SOCKS5 proxy port (default: {BALANCER_PORT})",
    )
    parser.add_argument(
        "--http-port",
        type=int,
        default=HTTP_PROXY_PORT,
        help=f"HTTP CONNECT proxy port (default: {HTTP_PROXY_PORT}, 0 = disabled)",
    )
    parser.add_argument(
        "--parallel-downloads",
        action="store_true",
        default=False,
        help="Enable parallel download feature for large HTTP files",
    )
    parser.add_argument(
        "--parallel-threshold",
        type=int,
        default=10 * 1024 * 1024,
        help="Minimum file size in bytes for parallel download (default: 10MB)",
    )
    parser.add_argument(
        "--peek-http",
        action="store_true",
        default=False,
        help="Peek at SOCKS5 tunnel to detect HTTP traffic",
    )
    return parser.parse_args()


async def main():
    args = parse_args()
    log = setup_logging()
    colorama_init()
    console.print(
        Panel(
            Align.center(
                Text.from_markup("[bold white]X B A L A N C E[/] [dim]v0.2.0[/]\n[dim]Multi-Socket Load Balancer for V2Ray/Xray[/]")
            ),
            border_style="cyan",
            padding=(1, 4),
        )
    )
    log.info("XBalance starting up (strategy=%s)", args.strategy)

    kill_stale_xray()

    xray_bin = ensure_xray_binary()

    raw_uris = load_raw_configs()
    if not raw_uris:
        console.print("[bold red]No configs found in configs.txt[/]")
        console.print("[red]Add your vmess://, vless://, trojan://, ss:// links or subscription URLs and try again.[/]")
        return

    outbounds = []
    for uri in raw_uris:
        outbound = parse_uri_to_outbound(uri)
        if outbound:
            outbounds.append(outbound)

    if not outbounds:
        console.print("[bold red]None of the configs could be parsed. Check your config format.[/]")
        return

    console.print(f"[green]Parsed {len(outbounds)} configs successfully.[/]")
    log.info("Parsed %d outbound configs", len(outbounds))

    manager = XrayWorkerManager(xray_bin)
    console.print(f"[yellow]Starting {len(outbounds)} local workers...[/]")
    worker_ports = await manager.start_workers(outbounds)

    if not worker_ports:
        console.print("[bold red]No workers started. Check your configs and try again.[/]")
        return

    console.print(f"[green]{len(worker_ports)} workers running![/]")
    log.info("%d workers running on ports %s", len(worker_ports), worker_ports)

    selector = create_selector(args.strategy, worker_ports)

    balancer = AsyncSOCKS5Balancer(
        BALANCER_HOST,
        args.socks_port,
        selector,
        peek_http=args.peek_http,
    )

    http_proxy = None
    if args.http_port > 0:
        http_proxy = HTTPConnectProxy(
            BALANCER_HOST,
            args.http_port,
            selector,
            parallel_downloads=args.parallel_downloads,
            parallel_threshold=args.parallel_threshold,
        )

    stats_task = asyncio.create_task(stats_printer_loop(balancer, http_proxy))

    tasks = [asyncio.create_task(balancer.start())]
    if http_proxy:
        tasks.append(asyncio.create_task(http_proxy.start()))

    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        stats_task.cancel()
        manager.stop_all()
        log.info("XBalance shutdown complete")


def main_cli():
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
