#!/usr/bin/env python3
"""Async runtime — event loop, futures, and task scheduler from scratch.

One file. Zero deps. Does one thing well.

Build an async runtime like Tokio/asyncio: event loop with ready/waiting
queues, futures with poll semantics, combinators (join, select, timeout).
"""
import time, sys, heapq
from enum import Enum
from collections import deque

class PollResult(Enum):
    READY = "ready"
    PENDING = "pending"

class Future:
    """Base future — poll until ready."""
    def poll(self):
        return PollResult.READY, None

class TimerFuture(Future):
    def __init__(self, duration):
        self.deadline = time.time() + duration
        self.result = None
    def poll(self):
        if time.time() >= self.deadline:
            return PollResult.READY, "timer done"
        return PollResult.PENDING, None

class ValueFuture(Future):
    def __init__(self, value):
        self.value = value
    def poll(self):
        return PollResult.READY, self.value

class MapFuture(Future):
    def __init__(self, inner, fn):
        self.inner = inner
        self.fn = fn
    def poll(self):
        status, val = self.inner.poll()
        if status == PollResult.READY:
            return PollResult.READY, self.fn(val)
        return PollResult.PENDING, None

class JoinFuture(Future):
    """Wait for all futures to complete."""
    def __init__(self, futures):
        self.futures = futures
        self.results = [None] * len(futures)
        self.done = [False] * len(futures)
    def poll(self):
        for i, f in enumerate(self.futures):
            if not self.done[i]:
                status, val = f.poll()
                if status == PollResult.READY:
                    self.results[i] = val
                    self.done[i] = True
        if all(self.done):
            return PollResult.READY, self.results
        return PollResult.PENDING, None

class SelectFuture(Future):
    """Wait for first future to complete."""
    def __init__(self, futures):
        self.futures = futures
    def poll(self):
        for i, f in enumerate(self.futures):
            status, val = f.poll()
            if status == PollResult.READY:
                return PollResult.READY, (i, val)
        return PollResult.PENDING, None

class Task:
    _counter = 0
    def __init__(self, name, future):
        Task._counter += 1
        self.id = Task._counter
        self.name = name
        self.future = future
        self.result = None
        self.done = False

class EventLoop:
    """Single-threaded async runtime."""
    def __init__(self):
        self.ready = deque()
        self.timers = []  # min-heap of (deadline, task)
        self.tasks = {}
        self.completed = []
        self.polls = 0

    def spawn(self, name, future):
        task = Task(name, future)
        self.tasks[task.id] = task
        self.ready.append(task)
        return task.id

    def run_until_complete(self):
        """Drive all tasks to completion."""
        while self.tasks:
            # Poll ready tasks
            if self.ready:
                task = self.ready.popleft()
                self.polls += 1
                status, val = task.future.poll()
                if status == PollResult.READY:
                    task.result = val
                    task.done = True
                    self.completed.append(task)
                    del self.tasks[task.id]
                else:
                    # Re-enqueue
                    self.ready.append(task)
            else:
                # Busy wait (real runtime would use epoll/kqueue)
                time.sleep(0.001)
                for task in list(self.tasks.values()):
                    self.ready.append(task)

    def run_for(self, duration):
        """Run event loop for a fixed duration."""
        deadline = time.time() + duration
        while time.time() < deadline and self.tasks:
            if self.ready:
                task = self.ready.popleft()
                self.polls += 1
                status, val = task.future.poll()
                if status == PollResult.READY:
                    task.result = val
                    task.done = True
                    self.completed.append(task)
                    del self.tasks[task.id]
                else:
                    self.ready.append(task)
            else:
                time.sleep(0.0005)
                for task in list(self.tasks.values()):
                    self.ready.append(task)

def main():
    print("=== Async Runtime ===\n")
    loop = EventLoop()

    # Spawn timer tasks
    loop.spawn("fast", TimerFuture(0.05))
    loop.spawn("medium", TimerFuture(0.1))
    loop.spawn("slow", TimerFuture(0.15))

    # Join combinator
    loop.spawn("join_all", JoinFuture([
        TimerFuture(0.02),
        TimerFuture(0.04),
        ValueFuture(42),
    ]))

    # Select (race)
    loop.spawn("race", SelectFuture([
        TimerFuture(0.08),
        TimerFuture(0.03),
    ]))

    # Map
    loop.spawn("mapped", MapFuture(ValueFuture(10), lambda x: x * x))

    t0 = time.perf_counter()
    loop.run_until_complete()
    dt = time.perf_counter() - t0

    print(f"Completed {len(loop.completed)} tasks in {dt*1000:.0f}ms ({loop.polls} polls)")
    for task in loop.completed:
        print(f"  {task.name:10s}: {task.result}")

if __name__ == "__main__":
    main()
