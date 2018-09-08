# Random Access Indexed File

A variation of [random-access-file (RAF)](https://github.com/random-access-storage/random-access-file) which provides "continuous reading or writing to a file using random offsets and lengths".

RAF depends on sparse files for implementation simplicity. Some operating systems (eg MacOS before APFS) do not support sparse files. This can be detected with [supports-sparse-files](https://github.com/mafintosh/supports-sparse-files). This module provides an alternative to RAF which uses an index file to map chunks to a non-continuous content file.

This module is compatible with files created by RAF. If no index file is found, it will default to RAF's behaviors.