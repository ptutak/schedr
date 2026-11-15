"""The watcher host's threads, sized from its wait steps (decision 489)."""

import threading
import time

from schedr.runner import watchers


def test_the_pool_lets_a_thread_run_for_each_wait_step_on_top_of_pythons_default() -> None:
    pool = watchers._WatcherThreads(thread_name_prefix="schedr-wait-step")  # pyright: ignore[reportPrivateUsage]
    base = pool._BASE  # pyright: ignore[reportPrivateUsage]
    steps = base + 8
    pool.fit(steps)
    running = 0
    most = 0
    lock = threading.Lock()
    release = threading.Event()

    def blocks() -> None:
        nonlocal running, most
        with lock:
            running += 1
            most = max(most, running)
        release.wait(5)
        with lock:
            running -= 1

    # One blocking call a step, all at once: each gets a thread, none waits for another.
    calls = [pool.submit(blocks) for _ in range(steps)]
    deadline = time.monotonic() + 5
    while most < steps and time.monotonic() < deadline:
        time.sleep(0.01)
    release.set()
    for call in calls:
        call.result(5)
    pool.shutdown()
    assert most == steps

    # Back to fewer steps: no more threads than they need are started anew.
    pool = watchers._WatcherThreads()  # pyright: ignore[reportPrivateUsage]
    pool.fit(0)
    assert pool._max_workers == base  # pyright: ignore[reportPrivateUsage]
    pool.shutdown()
