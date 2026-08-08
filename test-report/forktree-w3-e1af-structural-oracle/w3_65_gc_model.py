#!/usr/bin/env python3
"""Pure W3/W5 64+suffix and one-debt model; no production imports."""

from dataclasses import dataclass, field


@dataclass
class Queue:
    blocked: list[bool]
    head: int = 0
    debt: int = 0
    calls: int = 0
    reclaimed: list[int] = field(default_factory=list)
    epoch: int = 0
    progress: int = 0

    def step(self, page: int = 64) -> tuple[bool, bool, int]:
        self.calls += 1
        if self.head == len(self.blocked):
            return False, True, 0
        if self.blocked[self.head]:
            self.debt = 1
            return False, False, 0
        old = self.head
        self.head = min(old + page, len(self.blocked))
        reclaimed = list(range(old, self.head))
        self.reclaimed.extend(reclaimed)
        self.epoch += 1
        self.progress += 1
        if self.debt:
            self.debt = 0
        return True, self.head == len(self.blocked), len(reclaimed)


def main() -> None:
    queue = Queue([False] * 65)
    assert queue.step() == (True, False, 64)
    assert queue.step() == (True, True, 1)
    assert queue.step() == (False, True, 0)
    assert len(queue.reclaimed) == 65
    assert (queue.epoch, queue.progress) == (2, 2)
    print("PASS 65_entry_prefix_suffix_drain")

    blocked = Queue([True, False])
    assert blocked.step() == (False, False, 0)
    assert blocked.debt == 1 and blocked.calls == 1 and not blocked.reclaimed
    assert blocked.step() == (False, False, 0)
    assert blocked.calls == 2 and blocked.debt == 1 and not blocked.reclaimed
    blocked.blocked[0] = False
    assert blocked.step() == (True, True, 2)
    assert blocked.debt == 0 and blocked.calls == 3
    assert blocked.step() == (False, True, 0)
    print("PASS blocked_debt_no_spin_release_cadence")


if __name__ == "__main__":
    main()
