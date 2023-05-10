import time
from pathlib import Path
from collections import defaultdict
from folder_sync import folder_sync as fs


# def simple_function(n):
#     return n * n


# def create_empty_folders(root, branching, levels):
#     if levels == 0:
#         return

#     for i in range(branching):
#         new_folder = root / f"level_{levels}_{i}"
#         new_folder.mkdir(exist_ok=True)
#         create_empty_folders(new_folder, levels - 1, branching)


# def get_path_batches(root):
#     root.mkdir(exist_ok=True)
#     create_empty_folders(root, 10, 4)
#     paths_by_depth = defaultdict(list)
#     for p in root.rglob("*"):
#         if len(p.parts) > len(root.parts):
#             paths_by_depth[len(p.parts)].append(p)
#     path_batches = []
#     for level in sorted(paths_by_depth.keys(), reverse=True):
#         path_batches.append(paths_by_depth[level])

#     return path_batches


# if __name__ == "__main__":
#     n_threads = 4

#     # data = list(range(100_000))

#     # start = time.perf_counter()
#     # fs._run_executer_with_progress(4, simple_function, [(d,) for d in data])
#     # print(f"Finished in {round(time.perf_counter() - start, 2)} second(s)")

#     # start = time.perf_counter()
#     # for d in data:
#     #     simple_function(d)
#     # print(f"Finished in {round(time.perf_counter() - start, 2)} second(s)")

#     root = Path(__file__).parent / "large_test_data" / "many_empty_folders"

#     path_batches = get_path_batches(root)

#     start = time.perf_counter()
#     for paths in path_batches:
#         for p in paths:
#             p.rmdir()
#     print(f"Finished in {round(time.perf_counter() - start, 2)} second(s)")

#     path_batches = get_path_batches(root)

#     start = time.perf_counter()
#     fs._run_executer_with_progress(
#         n_threads,
#         lambda path: path.rmdir(),
#         [[(p,) for p in batch] for batch in path_batches],
#         batched=True,
#     )
#     print(f"Finished in {round(time.perf_counter() - start, 2)} second(s)")
