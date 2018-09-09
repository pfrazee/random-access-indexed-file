# Random Access Indexed File

A variation of [random-access-file (RAF)](https://github.com/random-access-storage/random-access-file) which provides "continuous reading or writing to a file using random offsets and lengths".

RAF depends on sparse files for implementation simplicity. Some operating systems (eg MacOS before APFS) do not support sparse files. This can be detected with [supports-sparse-files](https://github.com/mafintosh/supports-sparse-files).

This module provides an alternative to RAF which uses an index file to map chunks to a non-continuous content file.

This module is compatible with files created by RAF. If no index file is found, it will default to RAF's behaviors.

## `.index` file schema

The .index file is a list of 6-byte slots which map blocks to offsets in the content file. To lookup some bytes in the file, you use the following formula:

```js
function lookupContent (targetOffset) {
  // which block do we want
  var block = Math.floor(targetOffset / blocksize)
  // where does that block start in the content file
  var blockOffset = readIndexSlot(block + 2) // +2 to skip header
  // add the remainder offset
  var contentOffset = blockOffset + (targetOffset % blocksize)
  // read from content
  return readContent(contentOffset)
}
```

The .index has a header of 12 bytes which indicates the block size in bytes (6 bytes) and the next available offset (6 bytes).

This is an example .index map with 6 blocks.

|index slot|value|
|-|-|
|0|1024 (block size)|
|1|6144 (next available offset)
|2|0 (offset to block 0)|
|3|5120 (offset to block 1)|
|4|2048 (offset to block 2)|
|5|3072 (offset to block 3)|
|6|4096 (offset to block 4)|
|7|1024 (offset to block 5)|
