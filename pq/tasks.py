# -*- coding: utf-8 -*-
from logging import getLogger
from functools import wraps
from pydoc import locate
from . import (
    PQ as BasePQ,
    Queue as BaseQueue,
)


class RepeatType(Enum):
    DAILY = "DAILY"
    HOURLY = "HOURLY"


def task(
    queue,
    schedule_at=None,
    expected_at=None,
    max_retries=0,
    retry_in='30s',
):
    def decorator(f):
        f._path = "%s.%s" % (f.__module__, f.__qualname__)
        f._max_retries = max_retries
        f._retry_in = retry_in

        queue.handler_registry[f._path] = f

        @wraps(f)
        def wrapper(*args, **kwargs):
            _schedule_at = kwargs.pop('_schedule_at', None)
            _expected_at = kwargs.pop('_expected_at', None)

            if 'queue_job' not in kwargs or not kwargs['queue_job']:
                return f(*args, **kwargs)

            kwargs.pop('queue_job')
            put_kwargs = dict(
                schedule_at=_schedule_at or schedule_at,
                expected_at=_expected_at or expected_at,
            )

            return queue.put(
                dict(
                    function=f._path,
                    args=args,
                    kwargs=kwargs,
                    retried=0,
                    retry_in=f._retry_in,
                    max_retries=f._max_retries,
                ),
                **put_kwargs
            )

        return wrapper

    return decorator


class Queue(BaseQueue):
    handler_registry = dict()
    logger = getLogger('pq.tasks')

    def fail(self, job, data, e=None):
        retried = data['retried']
        if e:
            error = str(type(e).__name__) + ": " + str(e)
            self.update_error_attribute(job.id, error)
        if data.get('max_retries', 0) > retried:
            data.update(dict(
                retried=retried + 1,
            ))
            self.put_job(data)
            return False

        self.logger.warning("Failed to perform job %r :" % job)
        self.logger.exception(e)

        return False

    def put_job(self, data, schedule_at=None):
        id = self.put(data, schedule_at=schedule_at or data['retry_in'])
        self.logger.info("Rescheduled %r as `%s`" % (job, id))

    def perform(self, job):
        data = job.data
        function_path = data['function']

        f = self.handler_registry.get(function_path)

        if function_path not in self.handler_registry:
            f = self.handler_registry[function_path] = locate(function_path)

        if f is None:
            return self.fail(job, data, KeyError(
                "Job handler `%s` not found." % function_path,
            ))

        try:
            f(job.id, *data['args'], **data['kwargs'])
            self.complete(data)
            return True

        except Exception as e:
            return self.fail(job, data, e)

    task = task

    def complete(self, data):
        if "repeat" in data and data["repeat"]:
            if data["repeat"] == RepeatType.HOURLY.value:
                self.put_job(data, schedule_at="1h")
            elif data["repeat"] == RepeatType.DAILY.value:
                self.put_job(data, schedule_at="1d")

    def work(self, burst=False):
        """Starts processing jobs."""
        self.logger.info('`%s` starting to perform jobs' % self.name)

        for job in self:
            if job is None:
                if burst:
                    return

                continue

            self.perform(job)


class PQ(BasePQ):
    queue_class = Queue
