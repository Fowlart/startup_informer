from multiprocessing import Process
import os
from time import sleep


def square_numbers():
    for i in range(1000):
        print(f"\nIteration => {i}, from the Process with PID {os.getpid()}")
        sleep(0.25)
        if i == 500:
            print(f"Process {os.getpid()} reached {i}")


if __name__ == "__main__":

    processes = []

    num_processes = 5

    print(f"Number of spawned process: {num_processes}")

    # create processes and asign a function for each process
    for i in range(num_processes):
        process = Process(target=square_numbers)
        processes.append(process)

    # start all processes
    for process in processes:
        process.start()

    # wait for all processes to finish
    # block the main thread until these processes are finished
    for process in processes:
        process.join()