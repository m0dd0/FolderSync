import argparse
from pathlib import Path

from folder_sync import sync_folders


def cli():
    parser = argparse.ArgumentParser(
        prog="FolderSync",
        description="A simple utility to sync folders",
    )

    parser.add_argument("source", help="Source folder", type=Path)
    parser.add_argument("destination", help="Destination folder", type=Path)
    parser.add_argument("--n_threads", help="Number of threads", type=int, default=100)
    parser.add_argument("--shallow", help="Shallow compare", action="store_true")
    parser.add_argument("--verbosity", help="Verbosity level", default=20)
    parser.add_argument("--quiet", help="Quiet mode", action="store_true")

    args = parser.parse_args()

    sync_folders(
        args.source,
        args.destination,
        n_threads=args.n_threads,
        ask=not args.quiet,
        shallow_comparison=args.shallow,
        max_logged_paths=args.verbosity,
    )


if __name__ == "__main__":
    cli()
