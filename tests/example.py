from arq import Actor, BaseWorker, concurrent
from arq.utils import RedisSettings


class ActorTest(Actor):
    redis_settings = RedisSettings(port=6379)

    # pylint: disable=blacklisted-name
    @concurrent
    async def foo(self, a, b=0):
        with open("foo", "w") as f:
            r = a + b
            f.write("{}".format(r))


class Worker(BaseWorker):
    redis_settings = RedisSettings(port=6379)
    signature = "foobar"
    shadows = [ActorTest]


class WorkerSignalQuit(Worker):
    """
    worker which simulates receiving sigint after 2 jobs
    """

    max_concurrent_tasks = 1

    # pylint: disable=arguments-differ
    async def run_job(self, *args):
        # pylint: disable=no-value-for-parameter
        await super().run_job(*args)
        if self.jobs_complete >= 2:
            self.handle_sig(2)


class WorkerSignalTwiceQuit(Worker):
    """
    worker which simulates receiving sigint twice after 2 jobs
    """

    # pylint: disable=arguments-differ
    async def run_job(self, *args):
        # pylint: disable=no-value-for-parameter
        await super().run_job(*args)
        if self.jobs_complete >= 2:
            self.handle_sig_force(2, None)
