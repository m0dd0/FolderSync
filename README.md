# FolderSync
This small tool synchronizes two folders so that the target folder is an exact copy of the source folder. By comparing the files the number of actually executed file transactions is reduced to the files which have been changed since the last synchronisation.
Only files and folders are considered, links etc aren't.
I wrote this tool mainly out of curiosity and because I was disappointed by the speed and functionality of existing free tools.

## Usage
The module contains exactly one public function `sync_folders`. Below is a usage example.
```
```

A command line interface is also available.

## Performance
The tool uses multithreading to speed up the synchronization.
Different numbers of threads and file operations per thread can result in very different speed improvements.
The benchmark can be run with `pytest ./test_performance.py` in the test folder.
It comes clear that the copying of files is the most time-consuming part if a large amount of data has been changed.
Therefore we use the optimal parameters for this test case which are also often the best for other operations.
We use 100 threads and 10 operations per thread as default values.
