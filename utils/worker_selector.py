import abc
import random
import time
from collections import defaultdict
from typing import Dict, List, Optional


class WorkerStats:
    """Shared stats container for all selectors."""

    def __init__(self, worker_ports: List[int]):
        self.active_conns: Dict[int, int] = dict.fromkeys(worker_ports, 0)
        self.total_requests: Dict[int, int] = dict.fromkeys(worker_ports, 0)
        self.failures: Dict[int, int] = dict.fromkeys(worker_ports, 0)
        self.response_times: Dict[int, list] = defaultdict(list)
        self.unhealthy: set = set()
        self.last_health_check: Dict[int, float] = dict.fromkeys(worker_ports, 0.0)

    def record_success(self, port: int, response_time_ms: float):
        self.total_requests[port] += 1
        self.response_times[port].append(response_time_ms)
        if len(self.response_times[port]) > 50:
            self.response_times[port] = self.response_times[port][-50:]
        self.failures[port] = 0
        self.unhealthy.discard(port)

    def record_failure(self, port: int):
        self.failures[port] += 1
        if self.failures[port] >= 3:
            self.unhealthy.add(port)

    def connection_opened(self, port: int):
        self.active_conns[port] += 1

    def connection_closed(self, port: int):
        self.active_conns[port] = max(0, self.active_conns[port] - 1)

    def avg_response_time(self, port: int) -> float:
        times = self.response_times.get(port, [])
        return sum(times) / len(times) if times else 0.0

    def healthy_workers(self, all_ports: List[int]) -> List[int]:
        return [p for p in all_ports if p not in self.unhealthy]


class WorkerSelector(abc.ABC):
    """Base class for worker selection strategies."""

    def __init__(self, worker_ports: List[int]):
        self.worker_ports = worker_ports
        self.stats = WorkerStats(worker_ports)

    @abc.abstractmethod
    def next_worker(self) -> int:
        """Return the port of the next worker to use."""

    def on_success(self, port: int, response_time_ms: float):
        self.stats.record_success(port, response_time_ms)

    def on_failure(self, port: int):
        self.stats.record_failure(port)

    def on_connection_open(self, port: int):
        self.stats.connection_opened(port)

    def on_connection_close(self, port: int):
        self.stats.connection_closed(port)


class RoundRobinSelector(WorkerSelector):
    """Original round-robin: sequential cycling through workers."""

    def __init__(self, worker_ports: List[int]):
        super().__init__(worker_ports)
        self._index = 0

    def next_worker(self) -> int:
        healthy = self.stats.healthy_workers(self.worker_ports)
        if not healthy:
            healthy = self.worker_ports
        port = healthy[self._index % len(healthy)]
        self._index += 1
        return port


class LeastConnectionsSelector(WorkerSelector):
    """Pick the worker with the fewest active connections.
    Tie-breaks by least total requests (spreads load over time)."""

    def next_worker(self) -> int:
        healthy = self.stats.healthy_workers(self.worker_ports)
        if not healthy:
            healthy = self.worker_ports
        return min(
            healthy,
            key=lambda p: (self.stats.active_conns[p], self.stats.total_requests[p]),
        )


class ResponseTimeSelector(WorkerSelector):
    """Pick the worker with the lowest average response time.
    Falls back to round-robin for workers with no data yet."""

    def next_worker(self) -> int:
        healthy = self.stats.healthy_workers(self.worker_ports)
        if not healthy:
            healthy = self.worker_ports

        scored = []
        for p in healthy:
            avg = self.stats.avg_response_time(p)
            if avg == 0.0:
                scored.append((p, float("inf")))
            else:
                scored.append((p, avg))

        scored.sort(key=lambda x: x[1])
        best_time = scored[0][1]

        candidates = [p for p, t in scored if t == best_time]
        return random.choice(candidates)


class WeightedSelector(WorkerSelector):
    """Combines metrics with configurable weights.
    Lower score = better candidate."""

    def __init__(
        self,
        worker_ports: List[int],
        weight_connections: float = 0.4,
        weight_response_time: float = 0.4,
        weight_failures: float = 0.2,
    ):
        super().__init__(worker_ports)
        self.w_conn = weight_connections
        self.w_rt = weight_response_time
        self.w_fail = weight_failures

    def next_worker(self) -> int:
        healthy = self.stats.healthy_workers(self.worker_ports)
        if not healthy:
            healthy = self.worker_ports

        max_conns = max(
            (self.stats.active_conns[p] for p in healthy), default=1
        ) or 1
        max_rt = max(
            (self.stats.avg_response_time(p) for p in healthy), default=1
        ) or 1
        max_fail = max(
            (self.stats.failures[p] for p in healthy), default=1
        ) or 1

        def score(port: int) -> float:
            conn_score = self.stats.active_conns[port] / max_conns
            rt_score = self.stats.avg_response_time(port) / max_rt
            fail_score = self.stats.failures[port] / max_fail
            return (
                self.w_conn * conn_score
                + self.w_rt * rt_score
                + self.w_fail * fail_score
            )

        return min(healthy, key=score)


STRATEGIES = {
    "round-robin": RoundRobinSelector,
    "least-conn": LeastConnectionsSelector,
    "response-time": ResponseTimeSelector,
    "weighted": WeightedSelector,
}


def create_selector(name: str, worker_ports: List[int], **kwargs) -> WorkerSelector:
    cls = STRATEGIES.get(name)
    if cls is None:
        raise ValueError(f"Unknown strategy '{name}'. Available: {list(STRATEGIES)}")
    return cls(worker_ports, **kwargs)
