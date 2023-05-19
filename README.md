# FolderSync
This small tool synchronizes two folders so that the target folder is an exact copy of the source folder. By comparing the files the number of actually executed file transactions is minimized.
Only files and folders are considered, links etc aren't.
I wrote this tool mainly out of curiosity and because I was disappointed by the speed and functionality of existing free tools.

## Usage
The module contains exactly one public function `sync_folders`. Below is a usage example.
```
```

A command line interface is also available.

## Performance
The tool uses multithreading to speed up the synchronisation.
Different number of threads and affected files per thread can result in very different speed improvements.
The benchmark can be run with `pytest ./test_performance.py` in the test folder.  