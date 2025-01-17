from threading import Thread, Lock
import time

# all threads can access this global variable
database_value = 0

def increase(lock: Lock):
    global database_value # needed to modify the global value

    # using with instead of lock.acquire_lock() / lock.release()
    with lock:
        # get a local copy (simulate data retrieving)
        local_copy = database_value

        # simulate some modifying operation
        local_copy += 1
        time.sleep(0.1)

        # write the calculated new value into the global variable
        database_value = local_copy


if __name__ == "__main__":

    print('Start value: ', database_value)

    l = Lock()

    t1 = Thread(target=increase, args=(l,))
    t2 = Thread(target=increase, args=(l,))

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    print('End value:', database_value)