# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2016 Bitergia
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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#     Santiago Dueñas <sduenas@bitergia.com>
#     Alvaro del Castillo San Felix <acs@bitergia.com>
#

import datetime
import logging
import re

from grimoirelab_toolkit.datetime import (InvalidDateError,
                                          datetime_to_utc,
                                          str_to_datetime)
from grimoirelab_toolkit.introspect import find_class_properties


from .common import MAX_JOB_RETRIES, WAIT_FOR_QUEUING
from .errors import AlreadyExistsError, NotFoundError
from .utils import RWLock


logger = logging.getLogger(__name__)


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
    :param category: category of the items to fecth
    :param backend_args: dict of arguments required to run the backend
    :param archiving_cfg: archiving config for this task, if needed
    :param scheduling_cfg: scheduling config for this task, if needed
    """
    def __init__(self, task_id, backend, category, backend_args,
                 archiving_cfg=None, scheduling_cfg=None):
        self._task_id = task_id
        self.created_on = datetime.datetime.now().timestamp()
        self.backend = backend
        self.category = category
        self.backend_args = backend_args
        self.archiving_cfg = archiving_cfg if archiving_cfg else None
        self.scheduling_cfg = scheduling_cfg if scheduling_cfg else None

    @property
    def task_id(self):
        return self._task_id

    def to_dict(self):
        return {
            'task_id': self.task_id,
            'created_on': self.created_on,
            'backend': self.backend,
            'backend_args': self.backend_args,
            'category': self.category,
            'archiving_cfg': self.archiving_cfg.to_dict() if self.archiving_cfg else None,
            'scheduling_cfg': self.scheduling_cfg.to_dict() if self.scheduling_cfg else None
        }


class TaskRegistry:
    """Structure to register tasks.

    Tasks are stored using instances of `Task` class. Each task
    is added using its tag as unique identifier. Following accesses
    to the registry (i.e, to get or remove) will require of this
    identifier.

    The registry ensures mutual exclusion among threads using a
    reading-writting lock (`RWLock` on `utils` module).
    """
    def __init__(self):
        self._rwlock = RWLock()
        self._tasks = {}

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
            exists on the registry
        """
        self._rwlock.writer_acquire()

        if task_id in self._tasks:
            self._rwlock.writer_release()
            raise AlreadyExistsError(element=str(task_id))

        task = Task(task_id, backend, category, backend_args,
                    archiving_cfg=archiving_cfg,
                    scheduling_cfg=scheduling_cfg)
        self._tasks[task_id] = task

        self._rwlock.writer_release()

        logger.debug("Task %s added to the registry", str(task_id))

        return task

    def remove(self, task_id):
        """Remove a task from the registry.

        To remove it, pass its identifier with `taks_id` parameter.
        When the identifier is not found, a `NotFoundError` exception
        is raised.

        :param task_id: identifier of the task to remove

        :raises NotFoundError: raised when the given task identifier
            is not found on the registry
        """
        try:
            self._rwlock.writer_acquire()
            del self._tasks[task_id]
        except KeyError:
            raise NotFoundError(element=str(task_id))
        finally:
            self._rwlock.writer_release()

        logger.debug("Task %s removed from the registry", str(task_id))

    def get(self, task_id):
        """Get a task from the registry.

        Retrieve a task from the registry using its task identifier. When
        the task does not exist, a `NotFoundError` exception will be
        raised.

        :param task_id: task identifier

        :returns: a task object

        :raises NotFoundError: raised when the requested task is not
            found on the registry
        """
        try:
            self._rwlock.reader_acquire()
            task = self._tasks[task_id]
        except KeyError:
            raise NotFoundError(element=str(task_id))
        finally:
            self._rwlock.reader_release()

        return task

    @property
    def tasks(self):
        """Get the list of tasks"""

        self._rwlock.reader_acquire()
        tl = [v for v in self._tasks.values()]
        tl.sort(key=lambda x: x.task_id)
        self._rwlock.reader_release()

        return tl


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

    :param delay: seconds of delay
    :param max_retries: maximum number of job retries before failing
    """
    def __init__(self, delay=WAIT_FOR_QUEUING, max_retries=MAX_JOB_RETRIES):
        self.delay = delay
        self.max_retries = max_retries

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
