# -*- coding: utf-8 -*-
from logging import getLogger
from functools import wraps
from pydoc import locate
from . import (
    PQ as BasePQ,
    Queue as BaseQueue,
)
from enum import Enum
from psycopg2.errors import UniqueViolation


class RepeatType(Enum):
    DAILY = "DAILY"
    HOURLY = "HOURLY"


def task(
    queue,
    unique_key,
    schedule_at=None,
    expected_at=None,
    max_retries=0,
    retry_in='30s',
    repeat=None,
):
    def decorator(f):
        f._path = "%s.%s" % (f.__module__, f.__qualname__)
        f._max_retries = max_retries
        f._retry_in = retry_in
        f._repeat = repeat

        queue.handler_registry[f._path] = f

        @wraps(f)
        def wrapper(*args, **kwargs):
            _schedule_at = kwargs.pop('_schedule_at', None)
            _expected_at = kwargs.pop('_expected_at', None)

            put_kwargs = dict(
                schedule_at=_schedule_at or schedule_at,
                expected_at=_expected_at or expected_at,
                unique_key=unique_key,
            )

            try:
                return queue.put(
                    dict(
                        function=f._path,
                        args=args,
                        kwargs=kwargs,
                        retried=0,
                        retry_in=f._retry_in,
                        max_retries=f._max_retries,
                        repeat=f._repeat
                    ),
                    **put_kwargs
                )
            except UniqueViolation:
                return

        return wrapper

    return decorator


class Queue(BaseQueue):
    handler_registry = dict()
    logger = getLogger('pq.tasks')

    def fail(self, job, data, unique_key, e=None):
        self.reset_unique_key(job.id)
        retried = data['retried']
        if e:
            error = str(type(e).__name__) + ": " + str(e)
            self.update_error_attribute(job.id, error)
        if data.get('max_retries', 0) > retried:
            data.update(dict(
                retried=retried + 1,
            ))
            self.put_job(job, data, unique_key)
            return False

        self.logger.warning("Failed to perform job %r :" % job)
        self.logger.exception(e)

        return False

    def put_job(self, job, data, unique_key, schedule_at=None):
        id = self.put(data, unique_key=unique_key, schedule_at=schedule_at or data['retry_in'])
        self.logger.info("Rescheduled %r as `%s`" % (job, id))

    def perform(self, job):
        data = job.data
        unique_key = job.unique_key
        function_path = data['function']

        f = self.handler_registry.get(function_path)

        if function_path not in self.handler_registry:
            f = self.handler_registry[function_path] = locate(function_path)

        if f is None:
            return self.fail(job, data, KeyError(
                "Job handler `%s` not found." % function_path,
            ))

        try:
            f(*data['args'], **data['kwargs'])
            self.complete(job, data, unique_key)
            return True

        except Exception as e:
            return self.fail(job, data, unique_key, e)

    task = task

    def complete(self, job, data, unique_key):
        self.reset_unique_key(job.id)
        if "repeat" in data and data["repeat"]:
            if data["repeat"] == RepeatType.HOURLY.value:
                self.put_job(job, data, unique_key, schedule_at="1h")
            elif data["repeat"] == RepeatType.DAILY.value:
                self.put_job(job, data, unique_key, schedule_at="1d")

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
