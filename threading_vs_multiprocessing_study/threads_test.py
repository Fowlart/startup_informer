import os
from pickle import TUPLE2
from threading import Thread, current_thread
from time import sleep




def race_condition(a: list[str]):
    tr_name = str(Thread.name)
    for i in range(10):
        if "1" in tr_name:
            sleep(0.25)
        elif "2" in tr_name:
            sleep(0.55)
        elif "3" in tr_name:
            sleep(0.45)
        elif "4" in tr_name:
            sleep(0.35)
        else:
            sleep(0.1)
        a.append(f"{current_thread().name} [{i}]")


if __name__ == "__main__":
    threads: list[Thread]= []
    num_threads = 5

    a: list[str] = []

    # create threads and assign a function for each thread
    for i in range(num_threads):
        thread = Thread(target=race_condition, args=(a,))
        threads.append(thread)

    # start all threads
    for thread in threads:
        thread.start()

    # wait for all threads to finish
    # block the main thread until these threads are finished
    for thread in threads:
        thread.join()

    print(os.linesep.join(a))