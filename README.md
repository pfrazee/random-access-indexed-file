# Random Access Indexed File

A variation of [random-access-file (RAF)](https://github.com/random-access-storage/random-access-file) which provides "continuous reading or writing to a file using random offsets and lengths".

RAF depends on sparse files for implementation simplicity. Some operating systems (eg MacOS before APFS) do not support sparse files. This can be detected with [supports-sparse-files](https://github.com/mafintosh/supports-sparse-files). This module provides an alternative to RAF which uses an index file to map chunks to a non-continuous content file.

This module is compatible with files created by RAF. If no index file is found, it will default to RAF's behaviors.

## `.index` file schema

The .index file is a list of 6-byte pointers which maps blocks to offsets in the content file. To lookup some bytes in the file, you use the following formula:

```js
function lookupContent (targetOffset) {
  // which block do we want
  var block = Math.floor(targetOffset / blocksize)
  // where does that block start in the content file
  var blockOffset = block > 0 ? readIndex(block) : 0
  // add the remainder offset
  var contentOffset = blockOffset + (targetOffset % blocksize)
  // read from content
  return readContent(contentOffset)
}
```

Block zero of the .index file is special. It contains a pointer to the last-used offset. To counter-act this, the zero block always maps to the zero-offset of the content.

This is an example .index map with 6 block. Note, again, that the 0th block is not represented in this table, because it always maps to 0.

|index block|content offset|
|-|-|
|0|5120|
|1|1024|
|2|5120|
|3|2048|
|4|3072|
|5|4096|
