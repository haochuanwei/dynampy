"""
Use callbacks to dynamically organize and parallelize your whole program.
"""
from functools import wraps
from queue import Queue
import uuid
import wasabi
import deco

@deco.synchronized
def batch_callback(callback_list, lookup_dict):
    """Top-part of generic parallel processing of Callback objects.
    """
    retval_dict = dict()
    for _callback in callback_list:
        _callback.ready(lookup_dict)
        retval_dict[_callback._uuid] = execute_callback(_callback)
    return retval_dict

@deco.concurrent
def execute_callback(callback):
    """Bottom-part of generic parallel processing of Callback objects.
    """
    return callback._func(*callback._args, **callback._kwargs)

class Callback(object):
    """Specific data structure for dynamically organized callbacks.
    """
    def __init__(self, func, args, kwargs, serial=False):
        self._serial = serial
        self._func = func
        self._kwargs = kwargs
        self._uuid = uuid.uuid1()
        self._args = list()
        self._prereq = dict()
        for i, _arg in enumerate(args):
            if isinstance(_arg, Callback):
                self._prereq[_arg._uuid] = i
                self._args.append(_arg._uuid)
            else:
                self._args.append(_arg)

    def ready(self, lookup_dict):
        """Update arguments by looking up a dictionary.
        """
        for _uuid in self._prereq.keys():
            _idx = self._prereq.pop(_uuid)
            self._args[_idx] = lookup_dict[_uuid]

class CallbackQueue(object):
    """Makes callbacks using a queue.
    """
    def __init__(self):
        self.logger = wasabi.Printer()
        self.queue = Queue()
        self.lobby = dict()
        self.lookup = dict()

    def run(self):
        while not self.queue.empty():
            _callback_list = self.queue.get()
            _retval_dict = batch_callback(_callback_list, self.lookup)
            self.lookup.update(_retval_dict)

    def add(self, callback):
        """Add a callback without putting it into the queue.
        """
        self.lobby[callback._uuid] = callback

    def compile(self):
        """Identify dependent and/or parallelizable callbacks and group them.
        """
        assert self.queue.empty(), "Please call run() to clear the queue first."

        sent_to_queue = set()
        def sweep_for_independent():
            """Subroutine that gets one group of parallelizable callbacks.
            """
            callback_list = list()
            for _uuid, _callback in self.lobby.items():
                _independent = True
                for _prereq in _callback._prereq.keys():
                    if _prereq not in sent_to_queue and _prereq not in self.lookup:
                        _independent = False
                if _independent:
                    callback_list.append(_callback)
                    self.lobby.pop(_uuid)
            return callback_list

        while bool(self.lobby):
            callback_list = sweep_for_independent()
            assert callback_list, f"Expected at least one callback to be independent"
            self.queue.put(callback_list)
            sent_to_queue.update([_callback._uuid for _callback in callback_list])
