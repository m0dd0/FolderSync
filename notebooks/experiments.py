import concurrent.futures
import time


def simple_function(n):
    return n * n


def parallelized():
    start = time.perf_counter()

    data = list(range(100_000))
    executer = concurrent.futures.ThreadPoolExecutor(max_workers=4)
    futures = [executer.submit(simple_function, d) for d in data]

    for _ in concurrent.futures.as_completed(futures):
        pass

    executer.shutdown(wait=True)
    results = [f.result() for f in futures]

    finish = time.perf_counter()
    print(f"Finished in {round(finish - start, 2)} second(s)")


def sequential():
    start = time.perf_counter()

    data = list(range(100_000))
    results = [simple_function(d) for d in data]

    finish = time.perf_counter()
    print(f"Finished in {round(finish - start, 2)} second(s)")

def 

if __name__ == "__main__":
    parallelized() # 4.12 seconds
    sequential() # 0.01 seconds
    # --> check how 
