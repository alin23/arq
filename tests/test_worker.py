import asyncio
import logging
import re
from multiprocessing import Process

import pytest
from arq import Job
from arq.drain import TaskError
from arq.worker import import_string, start_worker

from .fixtures import kill_parent


async def test_run_job_burst(tmpworkdir, actor, worker):
    await actor.add_numbers(1, 2)
    assert not tmpworkdir.join("add_numbers").exists()
    await worker.run()
    assert tmpworkdir.join("add_numbers").read() == "3"
    assert worker.jobs_failed == 0


async def test_long_args(actor, worker, caplog):
    v = ",".join(map(str, range(20)))
    await actor.concat(a=v, b=v)
    await worker.run()
    log = re.sub(r"0.0\d\ds", "0.0XXs", caplog.text)
    print(log)
    assert (
        "dft  queued  0.0XXs → __id__ DemoActor.concat"
        "(a='0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19', b='0,1,2,3,4,5,6,7,8,9,1…)\n"
    ) in log
    assert (
        "dft  ran in  0.0XXs ← __id__ DemoActor.concat ● "
        "'0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19 + 0,1,2,3,4,5,6,7,8,9,10,11,…\n"
    ) in log


async def test_longer_args(actor, worker, caplog):
    worker.log_curtail = 90
    v = ",".join(map(str, range(20)))
    await actor.concat(a=v, b=v)
    await worker.run()
    log = re.sub(r"0.0\d\ds", "0.0XXs", caplog.text)
    assert (
        "dft  queued  0.0XXs → __id__ DemoActor.concat"
        "(a='0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19', b='0,1,2,3,4,5,6,7,8,9,10,11,12,13…)\n"
    ) in log
    assert (
        "dft  ran in  0.0XXs ← __id__ DemoActor.concat ● "
        "'0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19 + 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,1…\n"
    ) in log


async def test_stop_job_normal(actor, worker, caplog):
    caplog.set_level(logging.INFO, "arq.jobs")
    await actor.stop_job_normal()
    await worker.run()
    log = re.sub(r"0.0\d\ds", "0.0XXs", caplog.text)
    assert "dft  ran in  0.0XXs ■ __id__ DemoActor.stop_job_normal ● Stopped " in log
    assert "stopping job normally" in log


async def test_stop_job_warning(actor, worker, caplog):
    await actor.stop_job_warning()
    await worker.run()
    log = re.sub(r"0.0\d\ds", "0.0XXs", caplog.text)
    assert (
        "dft  ran in  0.0XXs ■ __id__ DemoActor.stop_job_warning ● Stopped Warning "
        in log
    )
    assert "stopping job with warning" in log


async def test_wrong_worker(worker, foobar_actor, caplog):
    assert await foobar_actor.concat("a", "b")
    await worker.run()
    assert worker.jobs_failed == 1
    assert (
        "Job Error: unable to find shadow for <Job __id__ foobar.concat(a, b) on dft>"
        in caplog.text
    )


async def test_queue_not_found(worker_quit):
    worker_quit.queues = ["foobar"]
    with pytest.raises(KeyError) as excinfo:
        await worker_quit.run()
    assert (
        "queue not found in queue lookups from shadows, queues: ['foobar']"
        in excinfo.value.args[0]
    )


def test_import_string_good(tmpworkdir, example_file):
    tmpworkdir.join("test.py").write(example_file)
    attr = import_string("test.py", "Worker")
    assert attr.__name__ == "Worker"
    assert attr.signature == "foobar"


def test_import_string_missing_attr(tmpworkdir, example_file):
    tmpworkdir.join("test.py").write(example_file)
    with pytest.raises(ImportError):
        import_string("test.py", "wrong")


def test_import_string_missing_file(tmpworkdir):
    with pytest.raises(ImportError):
        import_string("test.py", "wrong")


def test_import_start_worker(tmpworkdir, redis_conn, actor_test, loop, example_file):
    actor_test.loop = loop
    loop.run_until_complete(actor_test.foo(1, 2))

    assert loop.run_until_complete(redis_conn.exists(b"arq:q:dft"))
    dft_queue = loop.run_until_complete(redis_conn.lrange(b"arq:q:dft", 0, -1))
    assert len(dft_queue) == 1
    tmpworkdir.join("test.py").write(example_file)
    start_worker("test.py", "Worker", True, loop=loop)
    loop.run_until_complete(actor_test.close())


async def test_run_quit(tmpworkdir, redis_conn, actor, worker_quit, caplog):
    caplog.set_level(logging.DEBUG)

    await actor.save_slow(1, 0.1)
    await actor.save_slow(2, 0.1)
    await actor.save_slow(3, 0.1)
    await actor.save_slow(4, 0.1)
    assert await redis_conn.llen(b"arq:q:dft") == 4

    assert not tmpworkdir.join("save_slow").exists()
    await worker_quit.run()
    # the third job should be remove from the queue and readded
    assert tmpworkdir.join("save_slow").read() == "2"
    # assert '1 pending tasks, waiting for one to finish' in caplog
    assert await redis_conn.llen(b"arq:q:dft") == 2


async def test_task_exc(actor, worker_fail, caplog):
    caplog.set_level(logging.DEBUG)

    await actor.add_numbers(1, 2)

    with pytest.raises(RuntimeError):
        await worker_fail.run()
    assert 'Found task exception "foobar"' in caplog.text


def test_run_sigint(tmpworkdir, redis, actor_test, caplog, example_file):
    caplog.set_level(logging.DEBUG)

    loop = actor_test.loop
    loop.run_until_complete(actor_test.foo(1))
    loop.run_until_complete(actor_test.foo(1))
    loop.run_until_complete(actor_test.foo(1))
    loop.run_until_complete(actor_test.close())

    tmpworkdir.join("test.py").write(example_file)
    assert not tmpworkdir.join("foo").exists()
    start_worker("test.py", "WorkerSignalQuit", False, loop=loop)
    assert tmpworkdir.join("foo").exists()
    assert tmpworkdir.join("foo").read() == "1"
    assert "got signal: SIGINT, stopping..." in caplog.text


def test_run_sigint_twice(tmpworkdir, actor_test, caplog, example_file):
    caplog.set_level(logging.DEBUG)

    loop = actor_test.loop
    loop.run_until_complete(actor_test.foo(1))
    loop.run_until_complete(actor_test.foo(1))
    loop.run_until_complete(actor_test.foo(1))
    loop.run_until_complete(actor_test.close())

    tmpworkdir.join("test.py").write(example_file)
    with pytest.raises(BaseException):
        start_worker("test.py", "WorkerSignalTwiceQuit", False, loop=loop)
    assert tmpworkdir.join("foo").exists()
    assert tmpworkdir.join("foo").read() == "1"
    assert "Worker exiting after an unhandled error: ImmediateExit" in caplog.text


async def test_non_existent_function(actor, worker, caplog):
    await actor.enqueue_job("doesnt_exist")
    await worker.run()
    assert worker.jobs_failed == 1
    assert (
        'Job Error: shadow class "DemoActor" has no function "doesnt_exist"'
        in caplog.text
    )


def test_no_jobs(loop, actor, worker):
    loop.run_until_complete(actor.boom())
    loop.run_until_complete(actor.concat("a", "b"))
    worker.run_until_complete()

    loop.run_until_complete(worker.close())
    assert worker.jobs_complete == 2
    assert worker.jobs_failed == 1


async def test_shutdown_without_work(loop, worker):
    await worker.close()


async def test_worker_job_timeout(actor, worker, caplog):
    assert await actor.sleeper(0.5)
    assert await actor.sleeper(0.05)
    worker.timeout_seconds = 0.1
    worker._burst_mode = False
    worker.loop.create_task(worker.run())
    await asyncio.sleep(0.15)
    print(caplog.text)
    log = re.sub(r"(\d\.\d)\d\ds", r"\1XXs", caplog.text)
    assert "dft  queued  0.0XXs → __id__ DemoActor.sleeper(0.5)" in log
    assert "dft  queued  0.0XXs → __id__ DemoActor.sleeper(0.05)" in log
    assert "dft  ran in  0.0XXs ← __id__ DemoActor.sleeper ● 0.05" in log
    assert "task timed out <Job __id__ DemoActor.sleeper(0.5) on dft>" in log
    assert "! __id__ DemoActor.sleeper(0.5): CancelledError" in log
    assert "concurrent.futures._base.CancelledError" in log

    caplog.clear()
    await worker.close()
    log = re.sub(r"(\d\.\d)\d\ds", r"\1XXs", caplog.text)
    assert (
        "shutting down worker after 0.1XXs ◆ 2 jobs done ◆ 1 failed ◆ 1 timed out ◆ 0 expired"
        in log
    )


async def test_job_timeout(actor, worker, caplog):
    assert await actor.sleeper_with_timeout(0.5)
    assert await actor.sleeper_with_timeout(0.05)
    worker._burst_mode = False
    worker.loop.create_task(worker.run())
    await asyncio.sleep(0.15)
    print(caplog.text)
    log = re.sub(r"(\d\.\d)\d\ds", r"\1XXs", caplog.text)
    assert "dft  queued  0.0XXs → __id__ DemoActor.sleeper_with_timeout(0.5)" in log
    assert "dft  queued  0.0XXs → __id__ DemoActor.sleeper_with_timeout(0.05)" in log
    assert "dft  ran in  0.0XXs ← __id__ DemoActor.sleeper_with_timeout ● 0.05" in log
    assert (
        "task timed out <Job __id__ DemoActor.sleeper_with_timeout(0.5) on dft>" in log
    )
    assert (
        "dft  ran in  0.1XXs ! __id__ DemoActor.sleeper_with_timeout(0.5): CancelledError"
        in log
    )
    assert "concurrent.futures._base.CancelledError" in log

    caplog.clear()
    await worker.close()
    log = re.sub(r"(\d\.\d)\d\ds", r"\1XXs", caplog.text)
    assert (
        "shutting down worker after 0.1XXs ◆ 2 jobs done ◆ 1 failed ◆ 1 timed out ◆ 0 expired"
        in log
    )


async def test_job_expired(actor, worker, caplog):
    assert await actor.sleeper_with_expire_100ms(0.05)
    assert await actor.sleeper_with_expire_500ms(0.1)
    worker._burst_mode = False
    await asyncio.sleep(0.15)
    worker.loop.create_task(worker.run())
    await asyncio.sleep(0.15)
    print(caplog.text)
    log = re.sub(r"(\d\.\d)\d\ds", r"\1XXs", caplog.text)
    log = re.sub(r"(DEBUG|INFO|ERROR)\s+.+\.py:\d+\s+", "", log)
    assert (
        "dft  queued  0.1XXs → __id__ DemoActor.sleeper_with_expire_500ms(0.1)" in log
    )
    assert (
        "dft  ran in  0.1XXs ← __id__ DemoActor.sleeper_with_expire_500ms ● 0.1" in log
    )
    assert (
        "task expired <Job __id__ DemoActor.sleeper_with_expire_100ms(0.05) on dft>"
        in log
    )
    caplog.clear()
    await worker.close()
    log = re.sub(r"(\d\.\d)\d\ds", r"\1XXs", caplog.text)
    log = re.sub(r"(DEBUG|INFO|ERROR)\s+.+\.py:\d+\s+", "", log)
    assert (
        "shutting down worker after 0.1XXs ◆ 1 jobs done ◆ 0 failed ◆ 0 timed out ◆ 1 expired"
        in log
    )


async def test_job_unique(actor, worker, caplog):
    assert await actor.sleeper_unique(0.5)
    assert await actor.sleeper_unique(0.1)
    worker._burst_mode = False
    worker.loop.create_task(worker.run())
    await asyncio.sleep(0.15)
    print(caplog.text)
    log = re.sub(r"(\d\.\d)\d\ds", r"\1XXs", caplog.text)
    assert "dft  queued  0.0XXs → __id__ DemoActor.sleeper_unique(0.5)" in log
    assert "dft  queued  0.0XXs → __id__ DemoActor.sleeper_unique(0.1)" in log
    assert "dft  ran in  0.1XXs ← __id__ DemoActor.sleeper_unique ● 0.1" in log
    assert (
        "dft  ran in  0.0XXs ! __id__ DemoActor.sleeper_unique(0.5): CancelledError"
        in log
    )
    assert "concurrent.futures._base.CancelledError" in log

    caplog.clear()
    await worker.close()
    log = re.sub(r"(\d\.\d)\d\ds", r"\1XXs", caplog.text)
    assert (
        "shutting down worker after 0.1XXs ◆ 2 jobs done ◆ 1 failed ◆ 0 timed out ◆ 0 expired"
        in log
    )


async def test_job_cancel(actor, worker, caplog):
    caplog.set_level(logging.DEBUG, "arq.main")

    actor.job_class = Job
    job = await actor.sleeper(0.5)
    job2 = await actor.sleeper(0.1)
    assert job
    assert job2

    worker._burst_mode = False
    worker.cancel_queue_poll_seconds = 0.05
    worker.loop.create_task(worker.run())
    await asyncio.sleep(0.15)
    assert await actor.cancel(job.id) == 1
    await asyncio.sleep(0.1)

    print(caplog.text)
    log = re.sub(r"(\d\.\d)\d\ds", r"\1XXs", caplog.text)
    assert f"dft  queued  0.0XXs → {job.id[:6]} DemoActor.sleeper(0.5)" in log
    assert f"dft  queued  0.0XXs → {job2.id[:6]} DemoActor.sleeper(0.1)" in log
    assert f"dft  ran in  0.1XXs ← {job2.id[:6]} DemoActor.sleeper ● 0.1" in log
    assert f"Queued cancel task for job {job.id[:6]}" in log
    assert (
        f"dft  ran in  0.1XXs ! {job.id[:6]} DemoActor.sleeper(0.5): CancelledError"
        in log
    )
    assert "concurrent.futures._base.CancelledError" in log

    caplog.clear()
    await worker.close()
    log = re.sub(r"(\d\.\d)\d\ds", r"\1XXs", caplog.text)
    assert (
        "shutting down worker after 0.2XXs ◆ 2 jobs done ◆ 1 failed ◆ 0 timed out ◆ 0 expired"
        in log
    )


async def test_job_cancel_expires(actor, worker, caplog):
    caplog.set_level(logging.DEBUG, "arq.main")

    actor.job_class = Job
    actor.cancel_queue_expire_seconds = 0.05
    job = await actor.sleeper(0.5)
    job2 = await actor.sleeper(0.1)
    assert job
    assert job2

    worker._burst_mode = False
    worker.cancel_queue_poll_seconds = 0.1
    worker.loop.create_task(worker.run())
    await asyncio.sleep(0.15)
    assert await actor.cancel(job.id) == 1
    await asyncio.sleep(0.1)

    print(caplog.text)
    log = re.sub(r"(\d\.\d)\d\ds", r"\1XXs", caplog.text)
    assert f"dft  queued  0.0XXs → {job.id[:6]} DemoActor.sleeper(0.5)" in log
    assert f"dft  queued  0.0XXs → {job2.id[:6]} DemoActor.sleeper(0.1)" in log
    assert f"dft  ran in  0.1XXs ← {job2.id[:6]} DemoActor.sleeper ● 0.1" in log
    assert f"Queued cancel task for job {job.id[:6]}" in log
    assert (
        f"dft  ran in  0.1XXs ! {job.id[:6]} DemoActor.sleeper(0.5): CancelledError"
        not in log
    )
    assert "concurrent.futures._base.CancelledError" not in log

    caplog.clear()
    await worker.close()
    log = re.sub(r"(\d\.\d)\d\ds", r"\1XXs", caplog.text)
    assert "drain waiting 5.0s for 1 tasks to finish" in log


def test_repeat_worker_close(tmpworkdir, actor_test, caplog, example_file):
    tmpworkdir.join("test.py").write(example_file)
    loop = actor_test.loop

    async def enqueue_jobs():
        for i in range(1, 6):
            await actor_test.foo(0, i)
        await actor_test.close()

    loop.run_until_complete(enqueue_jobs())

    Process(target=kill_parent).start()
    start_worker("test.py", "Worker", False, loop=actor_test.loop)
    assert tmpworkdir.join("foo").exists()
    assert tmpworkdir.join("foo").read() == "5"  # because WorkerSignalQuit quit
    assert caplog.text.count("shutting down worker after") == 1


async def test_raise_worker_execute(actor, raise_worker):
    raise_worker.shadows = [actor.__class__]
    await actor.boom()
    with pytest.raises(RuntimeError) as excinfo:
        await raise_worker.run()
    assert excinfo.value.args[0] == "boom"
    await raise_worker.close()


async def test_raise_worker_prepare(actor, raise_worker):
    raise_worker.shadows = [actor.__class__]
    await actor.enqueue_job("foobar", 1, 2)
    with pytest.raises(RuntimeError) as excinfo:
        await raise_worker.run()
    assert (
        excinfo.value.args[0]
        == 'Job Error: shadow class "DemoActor" has no function "foobar"'
    )
    await raise_worker.close()


async def test_reusable_worker(tmpworkdir, worker, actor):
    worker.reusable = True

    await actor.add_numbers(1, 2)
    assert not tmpworkdir.join("add_numbers").exists()
    await worker.run()
    assert tmpworkdir.join("add_numbers").read() == "3"
    assert worker.jobs_failed == 0

    await actor.add_numbers(3, 4)
    await worker.run()
    assert tmpworkdir.join("add_numbers").read() == "7"
    assert worker.jobs_failed == 0
    await worker.close()


async def test_startup_shutdown(tmpworkdir, startup_worker, startup_actor):
    try:
        await startup_actor.concurrent_func("foobar")
        assert not tmpworkdir.join("events").exists()
        await startup_worker.run()
        assert (
            tmpworkdir.join("events").read()
            == "startup[True],concurrent_func[foobar],shutdown[True],"
        )
        assert startup_worker.jobs_failed == 0
    finally:
        await startup_actor.close(True)
    assert tmpworkdir.join("events").read() == (
        "startup[True],concurrent_func[foobar]," "shutdown[True],shutdown[False],"
    )


async def test_check_successful(redis_conn, worker):
    await redis_conn.set(b"arq:health-check", b"X")
    assert await worker._check_health() == 0


async def test_check_fails(redis_conn, worker):
    assert await redis_conn.exists(b"arq:health-check") == 0
    assert await worker._check_health() == 1


async def test_health_check_repeats(worker, caplog):
    worker.repeat_health_check_logs = True
    worker.drain = worker.drain_class(
        redis=await worker.get_redis(), cancel_queue=worker.CANCEL_QUEUE
    )
    async with worker.drain:
        await worker.record_health([b"a", b"b"], {b"a": "A", b"b": "B"})
        worker.last_health_check = 0
        await worker.record_health([b"a", b"b"], {b"a": "A", b"b": "B"})
        worker.last_health_check = 0
        await worker.record_health([b"a", b"b"], {b"a": "A", b"b": "B"})
    assert caplog.text.count("recording health:") == 3
    await worker.close()


async def test_health_check_repeats_hidden(worker, caplog):
    worker.repeat_health_check_logs = False
    worker.drain = worker.drain_class(
        redis=await worker.get_redis(), cancel_queue=worker.CANCEL_QUEUE
    )
    async with worker.drain:
        await worker.record_health([b"a", b"b"], {b"a": "A", b"b": "B"})
        worker.last_health_check = 0
        await worker.record_health([b"a", b"b"], {b"a": "A", b"b": "B"})
        worker.last_health_check = 0
        await worker.record_health([b"a", b"b"], {b"a": "A", b"b": "B"})
    assert caplog.text.count("recording health:") == 1
    await worker.close()


async def test_check_successful_real_value(redis_conn, worker):
    assert await redis_conn.exists(b"arq:health-check") == 0
    worker._burst_mode = False
    worker_task = worker.loop.create_task(worker.run())
    await asyncio.sleep(1)
    assert await redis_conn.exists(b"arq:health-check") == 1
    assert await worker._check_health() == 0
    await worker.close()
    worker_task.cancel()


async def test_does_re_enqueue_job(fast_shutdown_worker, re_enqueue_actor, redis_conn):
    fast_shutdown_worker.shadows = [re_enqueue_actor.__class__]

    await re_enqueue_actor.sleeper(0.2)

    with pytest.raises(TaskError):
        await fast_shutdown_worker.run()
    assert await redis_conn.llen(b"arq:q:dft") == 1


async def test_does_not_re_enqueue_job(redis_conn, fast_shutdown_worker, actor):
    fast_shutdown_worker.shadows = [actor.__class__]

    await actor.sleeper(0.2)

    with pytest.raises(TaskError):
        await fast_shutdown_worker.run()
    assert await redis_conn.llen(b"arq:q:dft") == 0
