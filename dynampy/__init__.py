"""
Use callbacks to dynamically organize and parallelize your program.
"""
from functools import wraps
from queue import Queue
import uuid
import deco
import wasabi

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
    return callback()

class Callback(object):
    """Specific data structure for dynamically organized callbacks.
    """
    def __init__(self, func, args, kwargs, prereq=[]):
        """
        :param func: the function to call.
        :param args: arguments to be passed to the function, which can be callbacks themselves.
        :param kwargs: keyword arguments to be passed.
        :param prereq: other callbacks that are not args but must finish before this callback can run.
        """
        self._uuid = uuid.uuid1()
        self._func = func
        self._kwargs = kwargs
        self._args = list()
        self._prereq = {_callback._uuid : -1 for _callback in prereq}
        for i, _arg in enumerate(args):
            if isinstance(_arg, Callback):
                self._prereq[_arg._uuid] = i
                self._args.append(_arg._uuid)
            else:
                self._args.append(_arg)

    def ready(self, lookup_dict):
        """Update arguments and check other prerequisites by looking up a dictionary, then clear prerequisites.
        :param lookup_dict: {uuid : value} mapping.
        """
        for _uuid in self._prereq.keys():
            _idx = self._prereq[_uuid]
            _value = lookup_dict[_uuid]
            if _idx >= 0:
                self._args[_idx] = _value
        self._prereq = dict()

    def __call__(self):
        """Run this callback.
        To run in parallel with deco, use a function instead of a method.
        """
        assert not self._prereq
        return self._func(*self._args, **self._kwargs)

    @staticmethod
    def wrap(func):
        """Wraps a function so that instead of returning retval, it delays execution and returns a callback.
        """
        def lazy_func(*args, **kwargs):
            return Callback(func, args, kwargs)
        return lazy_func

class CallbackSession(object):
    """Makes callbacks using a queue.
    """
    def __init__(self):
        self.logger = wasabi.Printer()
        self.queue = Queue()
        self.lobby = dict()
        self.lookup = dict()

    def run(self):
        self.build_queue()
        self.serve_queue()

    def add(self, callback):
        """Add a callback without putting it into the queue.
        """
        self.lobby[callback._uuid] = callback

    def wrap(self, func):
        """Wraps a function so that instead of returning retval, it delays execution and returns a callback. Also puts the callback in the lobby automatically.
        """
        def lazy_func(*args, **kwargs):
            callback = Callback(func, args, kwargs)
            self.add(callback)
            return callback
        return lazy_func

    def build_queue(self):
        """Identify dependent and/or parallelizable callbacks and group them.
        """
        assert self.queue.empty(), "Please call serve_queue() to clear the queue first."

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
            for _callback in callback_list:
                self.lobby.pop(_callback._uuid)
            return callback_list

        while bool(self.lobby):
            callback_list = sweep_for_independent()
            assert callback_list, f"Expected at least one callback to be independent"
            self.queue.put(callback_list)
            sent_to_queue.update([_callback._uuid for _callback in callback_list])

    def serve_queue(self, verbose=True):
        if verbose:
            self.logger.divider(f"Serving queued callbacks")
        while not self.queue.empty():
            _callback_list = self.queue.get()
            _retval_dict = batch_callback(_callback_list, self.lookup)
            self.logger.good(f"Finished {len(_callback_list)} callbacks", show=verbose)
            self.lookup.update(_retval_dict)
