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

foo_callback = Callback.wrap(foo)

def test_callback_functionality():
    assert foo(1, 2) == 3
    assert foo_callback(2, 1, add=False)() == 1

def test_parallel_callback():
    single_run = running_time(foo_callback(1, 1))

    tic = time.time()
    i_dim, j_dim = 20, 20
    assert i_dim * j_dim > 2
    queue = CallbackSession()
    for i in range(i_dim):
        for j in range(j_dim):
            queue.add(foo_callback(i, j))
    queue.compile()
    queue.run()
    toc = time.time()
    parallel_run = toc - tic
    assert len(queue.lookup) == i_dim * j_dim
    assert parallel_run < single_run * i_dim * j_dim

def test_serial_callback():
    single_run = running_time(foo_callback(0.0001, 0.0))

    tic = time.time()
    i_dim = 100
    queue = CallbackSession()
    callback_sum = 0.0
    for i in range(i_dim):
        increment = i * 0.0001
        callback_sum = foo_callback(callback_sum, increment)
        queue.add(callback_sum)
    queue.compile()
    queue.run()
    toc = time.time()
    serial_run = toc - tic
    assert len(queue.lookup) == i_dim
    assert 0.4950 - 1e-8 <= queue.lookup[callback_sum._uuid] < 0.4950 + 1e-8
    assert single_run * i_dim < serial_run < 1.5 * single_run * i_dim
