from typing import Any, Callable, List, Tuple
import concurrent.futures
from collections import defaultdict

from tqdm import tqdm


def _sequential_execution(func: Callable, data: List[Any]) -> List[Any]:
    """Executes a function sequentially on all data.

    Args:
        func (Callable): The function to execute
        data (List[Any]): The data to pass to the function. The function is called with func(*data).

    Returns:
        List[Any]: The results of the function
    """
    return [func(*d) for d in data]


def _chunk_list(l: List[Any], chunk_size: int) -> List[List[Any]]:
    """Splits a list into chunks of a given size.

    Args:
        l (List[Any]): The list to split
        chunk_size (int): The size of each chunk

    Returns:
        List[List[Any]]: The list of chunks
    """
    return [l[i : i + chunk_size] for i in range(0, len(l), chunk_size)]


def run_executer_with_progress(
    func: Callable,
    data: List[Tuple[Any]],
    n_threads: int,
    order: List[int] = None,
    datapoints_per_future: int = 1,
) -> List[List[Any]]:
    """Executes a function in parallel on all data and shows a progress bar which is
    updaed for each finished task.

    Args:
        n_threads (int): The number of threads to use
        func (Callable): The function to execute
        data (List[Tuple[Any]]): The data to pass to the function
        order (List[int], optional): The order in which the data should be processed. Defaults to None.
            It is ensured that data with lower order is processed before data with higher order.
            Data with the same order can be processed in parallel.
        datapoints_per_future (int, optional): The number of datapoints which are processed in one future/single thread.
    """

    if order is None:
        order = [0] * len(data)

    if len(data) != len(order):
        raise ValueError("data and order must have the same length")
    # ordered_data = defaultdict(list)
    # for idx, (val, ord) in enumerate(zip(data, order)):
    #     ordered_data[ord].append((idx, val))

    final_results = [None] * len(data)

    with tqdm(total=len(data)) as pbar:
        for ord in sorted(set(order)):
            data = [d for d, o in zip(data, order) if o == ord]
            indices = [idx for idx, o in enumerate(order) if o == ord]
            data_chunks = _chunk_list(data, datapoints_per_future)

            executer = concurrent.futures.ThreadPoolExecutor(max_workers=n_threads)
            chunk_futures = [
                executer.submit(_sequential_execution, func, arg_chunk)
                for arg_chunk in argument_chunks
            ]

        # for ord, indexed_data in sorted(ordered_data.items()):
        #     indices, arguments = zip(*indexed_data)
        #     argument_chunks = _chunk_list(arguments, datapoints_per_future)

        #     executer = concurrent.futures.ThreadPoolExecutor(max_workers=n_threads)
        #     chunk_futures = [
        #         executer.submit(_sequential_execution, func, arg_chunk)
        #         for arg_chunk in argument_chunks
        #     ]

        #     for fut in concurrent.futures.as_completed(chunk_futures):
        #         pbar.update(len(fut.result()))

        #     ord_results = [r for fut in chunk_futures for r in fut.result()]
        #     executer.shutdown(wait=True)

        #     for idx, result in zip(indices, ord_results):
        #         final_results[idx] = result

    return final_results