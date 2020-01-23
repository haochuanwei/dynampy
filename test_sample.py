from callback import Callback, CallbackSession
import time

def running_time(callback):
    tic = time.time()
    callback()
    toc = time.time()
    return toc - tic

def foo(a, b, add=True):
    time.sleep(0.01)
    if add:
        return a + b
    else:
        return a - b

class Bar:
    def __init__(self):
        self.value = 0

    def add(self, a, b):
        return a + b

def test_basic_components():
    assert foo(1, 2) == 3

def test_function_callback():
    foo_callback = Callback.wrap(foo)
    assert foo_callback(2, 1, add=False)() == 1

def test_method_callback():
    bar_callback = Callback.wrap(Bar().add)
    assert bar_callback(1, 2)() == 3

def test_additional_prereq():
    pass

def test_callback_session():
    session = CallbackSession()
    foo_sess = session.wrap(foo)
    callback = foo_sess(1, 2)
    session.run()
    assert len(session.lookup) == 1
    assert session.lookup[callback._uuid] == 3

def test_parallel_callback():
    foo_callback = Callback.wrap(foo)
    single_run = running_time(foo_callback(1, 1))

    tic = time.time()
    i_dim, j_dim = 20, 20
    assert i_dim * j_dim > 2
    session = CallbackSession()
    foo_sess = session.wrap(foo)
    for i in range(i_dim):
        for j in range(j_dim):
            callback = foo_sess(i, j)
    session.run()
    toc = time.time()
    parallel_run = toc - tic
    assert len(session.lookup) == i_dim * j_dim
    assert parallel_run < single_run * i_dim * j_dim

def test_serial_callback():
    foo_callback = Callback.wrap(foo)
    single_run = running_time(foo_callback(0.0001, 0.0))

    tic = time.time()
    i_dim = 100
    session = CallbackSession()
    callback_sum = 0.0
    for i in range(i_dim):
        increment = i * 0.0001
        callback_sum = foo_callback(callback_sum, increment)
        session.add(callback_sum)
    session.run()
    toc = time.time()
    serial_run = toc - tic
    assert len(session.lookup) == i_dim
    assert 0.4950 - 1e-8 <= session.lookup[callback_sum._uuid] < 0.4950 + 1e-8
    assert single_run * i_dim < serial_run < 1.5 * single_run * i_dim

if __name__ == '__main__':
    test_parallel_callback()
