# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2019 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Santiago Dueñas <sduenas@bitergia.com>
#     Alvaro del Castillo San Felix <acs@bitergia.com>
#

import collections
import datetime
import enum
import logging
import re
import pickle

from redis.exceptions import RedisError

from grimoirelab_toolkit.datetime import (InvalidDateError,
                                          datetime_to_utc,
                                          datetime_utcnow,
                                          str_to_datetime)
from grimoirelab_toolkit.introspect import find_class_properties

import perceval.backend
import perceval.backends

from .common import MAX_JOB_RETRIES, WAIT_FOR_QUEUING
from .errors import (AlreadyExistsError,
                     NotFoundError,
                     TaskRegistryError)
from .utils import RWLock


logger = logging.getLogger(__name__)


TASK_PREFIX = 'arthur:task:'


@enum.unique
class TaskStatus(enum.Enum):
    """Task life cycle statuses.

    The life cycle of a task starts when is created and added
    to the system as `NEW`.

    The next step will be to program a task job. Once this is
    done the task will be `SCHEDULED` to run at a defined time.
    It will remain in this status until its job is `ENQUEUED`.

    The job will advance in the queue while other jobs are
    executed. Right after it gets to the head of the queue and a
    worker is free it will execute. The task will be `RUNNING`.

    Depending on the result executing the job, the outcomes will
    be different. If the job executed successfully, the task
    will be set to `COMPLETED`. If there was an error the status
    will be `FAILED`.

    Recurring tasks, that were successful, will be re-scheduled
    again (`SCHEDULED`), stating a new cycle.
    """
    NEW = 1
    SCHEDULED = 2
    ENQUEUED = 3
    RUNNING = 4
    COMPLETED = 5
    FAILED = 6


class Task:
    """Basic class to store information about a task.

    This class stores the basic information needed to retrieve data
    from a repository. The parameters needed to run the backend
    are given in the dictionary `backend_args`. Other parameters
    can also given to configure the archive (with `archive_args`) or to
    define how this task will be scheduled (using an instance of
    `SchedulingTaskConfig`).

    The task will be identified by the `task_id` attribute.

    :param task_id: identifier of this task
    :param backend: backend used to fetch data from the repository
    :param category: category of the items to fetch
    :param backend_args: dict of arguments required to run the backend
    :param archiving_cfg: archiving config for this task, if needed
    :param scheduling_cfg: scheduling config for this task, if needed

    :raises NotFoundError: when the given backend is not available
    """
    def __init__(self, task_id, backend, category, backend_args,
                 archiving_cfg=None, scheduling_cfg=None):
        try:
            bklass = perceval.backend.find_backends(perceval.backends)[0][backend]
        except KeyError:
            raise NotFoundError(element=backend)

        self._task_id = task_id
        self._has_resuming = bklass.has_resuming()

        self.status = TaskStatus.NEW
        self.age = 0
        self.num_failures = 0
        self.jobs = []
        self.created_on = datetime_utcnow().timestamp()
        self.backend = backend
        self.category = category
        self.backend_args = backend_args
        self.archiving_cfg = archiving_cfg if archiving_cfg else None
        self.scheduling_cfg = scheduling_cfg if scheduling_cfg else None

    @property
    def task_id(self):
        return self._task_id

    def has_resuming(self):
        """Returns if the task can be resumed when it fails"""

        return self._has_resuming

    def set_job(self, job_id, job_number):
        """Adds a JobData tuple object to the job list of a task"""

        job_tuple = JobData(job_id, job_number)
        self.jobs.append(job_tuple)

    def to_dict(self):
        return {
            'task_id': self.task_id,
            'status': self.status.name,
            'age': self.age,
            'num_failures': self.num_failures,
            'jobs': [{
                'job_id': job.id,
                'job_number': job.number
            } for job in self.jobs],
            'created_on': self.created_on,
            'backend': self.backend,
            'backend_args': self.backend_args,
            'category': self.category,
            'archiving_cfg': self.archiving_cfg.to_dict() if self.archiving_cfg else None,
            'scheduling_cfg': self.scheduling_cfg.to_dict() if self.scheduling_cfg else None
        }


JobData = collections.namedtuple('JobData', ['id', 'number'])


class TaskRegistry:
    """Structure to register tasks.

    Tasks are stored using instances of `Task` class. Each task
    is added using its tag as unique identifier. Following accesses
    to the registry (i.e, to get or remove) will require of this
    identifier.

    The registry ensures mutual exclusion among threads using a
    reading-writing lock (`RWLock` on `utils` module).

    :param conn: connection Object to Redis
    """
    def __init__(self, conn):
        self.conn = conn
        self._rwlock = RWLock()

    @staticmethod
    def _task_key(task_id):
        return '{}:{}'.format(TASK_PREFIX, task_id)

    def add(self, task_id, backend, category, backend_args,
            archiving_cfg=None, scheduling_cfg=None):
        """Add a task to the registry.

        This method adds task using `task_id` as identifier. If a task
        with the same identifier already exists on the registry, a
        `AlreadyExistsError` exception will be raised.

        :param task_id: identifier of the task to add
        :param backend: backend used to fetch data from the repository
        :param category: category of the items to fetch
        :param backend_args: dictionary of arguments required to run the backend
        :param archiving_cfg: archiving config for the task, if needed
        :param scheduling_cfg: scheduling config for the task, if needed

        :returns: the new task added to the registry

        :raises AlreadyExistsError: raised when the given task identifier
            already exists in the registry
        :raises TaskRegistryError: raised when the given task identifier
            is not added to the registry
        """
        self._rwlock.writer_acquire()
        try:
            task_key = self._task_key(task_id)
            found = self.conn.exists(task_key)

            if found:
                raise AlreadyExistsError(element=str(task_id))

            task = Task(task_id, backend, category, backend_args,
                        archiving_cfg=archiving_cfg,
                        scheduling_cfg=scheduling_cfg)
            self.conn.set(task_key, pickle.dumps(task))

            logger.debug("Task %s added to the registry", str(task_id))
            return task
        except RedisError as e:
            msg = "Task {} not added: {}".format(task_id, e)
            logger.error(msg)
            raise TaskRegistryError(cause=msg)
        finally:
            self._rwlock.writer_release()

    def remove(self, task_id):
        """Remove a task from the registry.

        To remove it, pass its identifier with `task_id` parameter.
        When the identifier is not found, a `NotFoundError` exception
        is raised.

        :param task_id: identifier of the task to remove

        :raises NotFoundError: raised when the given task identifier
            is not found in the registry
        :raises TaskRegistryError: raised when the given task identifier
            is not removed from the registry
        """
        self._rwlock.writer_acquire()
        try:
            task_key = self._task_key(task_id)
            found = self.conn.exists(task_key)

            if not found:
                raise NotFoundError(element=str(task_id))

            self.conn.delete(task_key)

            logger.debug("Task %s removed from the registry", str(task_id))
        except RedisError as e:
            msg = "Task {} not removed: {}".format(task_id, e)
            logger.error(msg)
            raise TaskRegistryError(cause=msg)
        finally:
            self._rwlock.writer_release()

    def get(self, task_id):
        """Get a task from the registry.

        Retrieve a task from the registry using its task identifier. When
        the task does not exist, a `NotFoundError` exception will be
        raised.

        :param task_id: task identifier

        :returns: a task object

        :raises NotFoundError: raised when the requested task is not
-           found in the registry
        :raises TaskRegistryError: raised when the requested task is not
            retrieved from the registry
        """
        self._rwlock.reader_acquire()
        try:
            task_key = self._task_key(task_id)
            found = self.conn.exists(task_key)

            if not found:
                raise NotFoundError(element=str(task_id))

            task_dump = self.conn.get(task_key)
            task = pickle.loads(task_dump)

            return task
        except RedisError as e:
            msg = "Task {} not retrieved: {}".format(task_id, e)
            logger.error(msg)
            raise TaskRegistryError(cause=msg)
        finally:
            self._rwlock.reader_release()

    def update(self, task_id, task):
        """Update a task in the registry.

        Update a task stored in the registry using its task identifier. When
        the task does not exist, a `NotFoundError` exception will be
        raised.

        :param task_id: task identifier
        :param task: task object

        :returns: a task object

        :raises TaskRegistryError: raised when the task is not
            updated
        """
        self._rwlock.writer_acquire()
        try:
            task_key = self._task_key(task_id)
            found = self.conn.exists(task_key)

            if not found:
                logger.warning("Task %s not found, adding it", str(task_id))

            self.conn.set(task_key, pickle.dumps(task))

            logger.debug("Task %s updated", str(task_id))
        except RedisError as e:
            msg = "Task {} not updated: {}".format(task_id, e)
            logger.error(msg)
            raise TaskRegistryError(cause=msg)
        finally:
            self._rwlock.writer_release()

    @property
    def tasks(self):
        """Get the list of tasks

        Retrieve the list of tasks stored in the registry

        :returns: a list of tasks

        :raises TaskRegistryError: raised when the tasks cannot
            be listed
        """
        self._rwlock.reader_acquire()
        try:
            tasks = []
            keys = []

            match_prefix = "{}*".format(TASK_PREFIX)
            total, found = self.conn.scan(match=match_prefix)
            keys.extend([f.decode("utf-8") for f in found])
            while total != 0:
                total, found = self.conn.scan(cursor=total, match=match_prefix)
                keys.extend([f.decode("utf-8") for f in found])

            keys.sort()
            for k in keys:
                task_dump = self.conn.get(k)
                tasks.append(pickle.loads(task_dump))

            return tasks
        except RedisError as e:
            msg = "Tasks not listed: {}".format(e)
            logger.error(msg)
            raise TaskRegistryError(cause=msg)
        finally:
            self._rwlock.reader_release()


class _TaskConfig:
    """Abstract class to store task configuration options.

    This class defines how to store specific task configuration
    arguments such as scheduling or archiving options. It is not
    meant to be instantiated on its own.

    Configuration options must be defined using `property` and `setter`
    decorators. Setters must check whether the given value is valid
    or not. When it is invalid, a `ValueError` exception should be
    raised. The rationale behind this is to use these methods as
    parsers when `from_dict` class method is called. It will create
    a new instance of the subclass passing its properties from a
    dictionary.
    """
    KW_ARGS_ERROR_REGEX = re.compile(r"^.+ got an unexpected keyword argument '(.+)'$")

    def to_dict(self):
        """Returns a dict with the representation of this task configuration object."""

        properties = find_class_properties(self.__class__)
        config = {
            name: self.__getattribute__(name) for name, _ in properties
        }
        return config

    @classmethod
    def from_dict(cls, config):
        """Create an configuration object from a dictionary.

        Key,value pairs will be used to initialize a task configuration
        object. If 'config' contains invalid configuration parameters
        a `ValueError` exception will be raised.

        :param config: dictionary used to create an instance of this object

        :returns: a task config instance

        :raises ValueError: when an invalid configuration parameter is found
        """
        try:
            obj = cls(**config)
        except TypeError as e:
            m = cls.KW_ARGS_ERROR_REGEX.match(str(e))
            if m:
                raise ValueError("unknown '%s' task config parameter" % m.group(1))
            else:
                raise e
        else:
            return obj


class ArchivingTaskConfig(_TaskConfig):
    """Manages the archiving configuration of a task.

    A limited number of archiving parameters can be configured for a
    task.

    The `archive_path` option stores the path where the archive
    for this task is or will be stored.

    The `fetch_from_archive` option sets if the task will fetch
    items from the archive.

    When `archived_after` is set, only those items archived after
    this date will be fetched.

    :param archive_path: path where the archive is or will be stored
    :param fetch_from_archive: fetch items from the archive
    :param archived_after: fetch items archived after the given date
    """
    def __init__(self, archive_path, fetch_from_archive,
                 archived_after=None):
        self.archive_path = archive_path
        self.fetch_from_archive = fetch_from_archive
        self.archived_after = archived_after

    @property
    def archive_path(self):
        """Path where the archive for this task is or will be stored."""

        return self._archive_path

    @archive_path.setter
    def archive_path(self, value):
        if not isinstance(value, str):
            raise ValueError("'archive_path' must be a str; %s given"
                             % str(type(value)))
        self._archive_path = value

    @property
    def fetch_from_archive(self):
        """Defines if the task will fetch items from the archive."""

        return self._fetch_from_archive

    @fetch_from_archive.setter
    def fetch_from_archive(self, value):
        if not isinstance(value, bool):
            raise ValueError("'fetch_from_archive' must be a bool; %s given"
                             % str(type(value)))
        self._fetch_from_archive = value

    @property
    def archived_after(self):
        """Items archived after this date will be fetched."""

        return self._archived_after

    @archived_after.setter
    def archived_after(self, value):
        if value is None:
            self._archived_after = None
        elif isinstance(value, datetime.datetime):
            self._archived_after = datetime_to_utc(value)
        elif isinstance(value, str):
            try:
                self._archived_after = str_to_datetime(value)
            except InvalidDateError as e:
                raise ValueError("'archived_after' is invalid; %s" % str(e))
        else:
            raise ValueError("'archived_after' must be either a str or a datetime; %s given"
                             % str(type(value)))


class SchedulingTaskConfig(_TaskConfig):
    """Manages the scheduling configuration of a task.

    A limited number of parameters can be configured to schedule a task.

    The `delay` option stores the number of seconds a recurring
    task will be waiting before being scheduled again.

    The `max_retries` option configures the maximum number of attempts
    a job can execute before failing.

    The `max_age` option defines the maximum number of a task can run
    in the scheduler. Set it to `None` to run the task indefinitely.

    The 'queue' option defines the name of the queue where this
    task will run. Set it to `None` to let the scheduler decide
    what the best queue is.

    :param delay: seconds of delay
    :param max_retries: maximum number of job retries before failing
    :param max_age: maximum number of times the task can run in the scheduler
    :param queue: name of the queue to run this task
    """
    def __init__(self, delay=WAIT_FOR_QUEUING, max_retries=MAX_JOB_RETRIES,
                 max_age=None, queue=None):
        self.delay = delay
        self.max_retries = max_retries
        self.max_age = max_age
        self.queue = queue

    @property
    def delay(self):
        """Number of seconds a recurring task will be waiting before being scheduled again"""

        return self._delay

    @delay.setter
    def delay(self, value):
        if not isinstance(value, int):
            raise ValueError("'delay' must be an int; %s given" % str(type(value)))
        self._delay = value

    @property
    def max_retries(self):
        """Maximum  number of attempts this job can execute before failing."""

        return self._max_retries

    @max_retries.setter
    def max_retries(self, value):
        if not isinstance(value, int):
            raise ValueError("'max_retries' must be an int; %s given" % str(type(value)))
        self._max_retries = value

    @property
    def max_age(self):
        """Maximum number of times this task can run in the scheduler.

        When it is set to `None`, the task will run indefinitely.
        """
        return self._max_age

    @max_age.setter
    def max_age(self, value):
        if value is not None:
            if not isinstance(value, int):
                raise ValueError("'max_age' must be an int; %s given" % str(type(value)))
            elif value < 1:
                raise ValueError("'max_age' must have a positive value; %s given" % str(value))
        self._max_age = value

    @property
    def queue(self):
        """Name of the queue where the task will run.

        When it is set to `None`, the scheduler will decide which queue to use.
        """
        return self._queue

    @queue.setter
    def queue(self, value):
        if value is not None:
            if not isinstance(value, str):
                raise ValueError("'queue' must be a str; %s given" % str(type(value)))
        self._queue = value
