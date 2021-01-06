from dataclasses import dataclass, field
from typing import List, Mapping


@dataclass
class Service:
    name: str
    task_count: int
    task_cpu_reservation: int = 0
    task_memory_reservation: int = 0
    cpu_utilization: float = 0
    memory_utilization: float = 0
    tags: List[Mapping[str, str]] = field(default_factory=list)

    @property
    def cpu_reservation(self) -> int:
        return self.task_count * self.task_cpu_reservation

    @property
    def memory_reservation(self) -> int:
        return self.task_count * self.task_memory_reservation

    @property
    def memory_per_vcpu(self) -> int:
        if self.task_cpu_reservation == 0:
            return 0
        return 1024 * self.task_memory_reservation / self.task_cpu_reservation
