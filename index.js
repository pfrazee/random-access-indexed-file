const inherits = require('util').inherits
const promisify = require('util').promisify
const RandomAccess = require('random-access-storage')
const fs = require('fs')
const fsp = {
  open: promisify(fs.open),
  close: promisify(fs.close),
  unlink: promisify(fs.unlink),
  rmdir: promisify(fs.rmdir),
  fstat: promisify(fs.fstat),
  read: promisify(fs.read),
  write: promisify(fs.write),
  ftruncate: promisify(fs.ftruncate)
}
const mkdirp = require('mkdirp')
const path = require('path')
const uint48be = require('uint48be')
const AwaitLock = require('await-lock')
const constants = fs.constants || require('constants')

const READONLY = constants.O_RDONLY
const READWRITE = constants.O_RDWR | constants.O_CREAT

const DEFAULT_BLOCK_SIZE = 1024 * 1024 // 1mb, in bytes
const BLOCK_SIZE_SLOT = 0
const NEXT_PTR_SLOT = 1
const OFFSET_TO_SLOT = (offset, blockSize) => Math.floor(offset / blockSize) + 2

const acquireBlockLock = new AwaitLock()

module.exports = RandomAccessNonsparseFile

function RandomAccessNonsparseFile (filename, opts) {
  if (!(this instanceof RandomAccessNonsparseFile)) return new RandomAccessNonsparseFile(filename, opts)
  RandomAccess.call(this)

  if (!opts) opts = {}
  if (opts.directory) filename = path.join(opts.directory, filename)

  this.directory = opts.directory || null
  this.contentFilename = filename
  this.contentFd = 0
  this.indexFilename = filename + '.index'
  this.indexFd = 0
  this._nextPtr = 0

  // makes random-access-storage open in writable mode first
  if (opts.writable) this.preferReadonly = false

  this._blockSize = opts.blockSize || DEFAULT_BLOCK_SIZE
  this._rmdir = !!opts.rmdir
}

inherits(RandomAccessNonsparseFile, RandomAccess)

RandomAccessNonsparseFile.prototype._open = function (req) {
  var self = this

  mkdirp(path.dirname(this.contentFilename), ondir)

  function ondir (err) {
    if (err) return req.callback(err)
    open(self, READWRITE, req)
  }
}

RandomAccessNonsparseFile.prototype._openReadonly = function (req) {
  open(this, READONLY, req)
}

RandomAccessNonsparseFile.prototype._write = async function (req) {
  var data = req.data

  try {
    while (req.size) {
      // read/create the pointer
      try {
        await acquireBlockLock.acquireAsync()
        var ptr = await readPointer(this, req.offset)
        if (!ptr) {
          ptr = await allocatePointer(this, req.offset)
        }
      } finally {
        acquireBlockLock.release()
      }

      // cap the size by the blocksize
      var size = Math.min(req.size, this._blockSize - ptr.blockOffset)

      // write the data
      var res = await fsp.write(this.contentFd, data, data.length - req.size, size, ptr.fileOffset)
      req.size -= res.bytesWritten
      req.offset += res.bytesWritten
    }
  } catch (err) {
    return req.callback(err)
  }

  return req.callback(null)
}

RandomAccessNonsparseFile.prototype._read = async function (req) {
  var data = req.data || Buffer.allocUnsafe(req.size)

  if (!req.size) {
    return process.nextTick(readEmpty, req)
  }

  try {
    while (req.size) {
      // read the pointer
      var ptr = await readPointer(this, req.offset)
      if (!ptr) {
        return req.callback(new Error('Could not satisfy length'))
      }

      // cap the size by the blocksize
      var size = Math.min(req.size, this._blockSize - ptr.blockOffset)

      // read the block
      var res = await fsp.read(this.contentFd, data, data.length - req.size, size, ptr.fileOffset)
      if (!res || !res.bytesRead) return req.callback(new Error('Could not satisfy length'))
      req.size -= res.bytesRead
      req.offset += res.bytesRead
    }
  } catch (err) {
    return req.calback(err)
  }
  
  return req.callback(null, data)
}

RandomAccessNonsparseFile.prototype._del = async function (req) {
  try {
    var st = await fsp.fstat(this.contentFd)
    if (req.offset + req.size < st.size) {
      return req.callback(null)
    }
    await fsp.ftruncate(this.contentFd, req.offset)
  } catch (err) {
    return req.callback(err)
  }
  req.callback(null)
}

RandomAccessNonsparseFile.prototype._stat = function (req) {
  fs.fstat(this.contentFd, onstat)

  function onstat (err, st) {
    req.callback(err, st)
  }
}

RandomAccessNonsparseFile.prototype._close = async function (req) {
  try {
    await fsp.close(this.contentFd)
    this.contentFd = 0
  } catch (err) {
    return req.callback(err)
  }
  req.callback(null)
}

RandomAccessNonsparseFile.prototype._destroy = async function (req) {
  var root = this.directory && path.resolve(path.join(this.directory, '.'))
  var dir = path.resolve(path.dirname(this.contentFilename))

  try {
    if (this.indexFd) {
      await fsp.unlink(this.indexFilename)
    }
  } catch (err) {
    return req.callback(err)
  }

  try {
    await fsp.unlink(this.contentFilename)
  } catch (err) {
    return req.callback(err)
  }

  if (!this._rmdir || !root || dir === root) {
    return req.callback(null)
  }

  try {
    while (dir !== root) {
      await fsp.rmdir(dir)
      dir = path.join(dir, '..')
    }
  } catch (err) {
    // ignore
  }

  return req.callback()
}

async function open (self, mode, req) {
  try {
    // open the content file
    var oldContentFd = self.contentFd
    self.contentFd = await fsp.open(self.contentFilename, mode)
    if (oldContentFd) await fsp.close(oldContentFd)

    // did we create the content file?
    var st = await fsp.fstat(self.contentFd)
    var isContentFileNew = st.size === 0
    var indexMode = mode
    if (!isContentFileNew) {
      // dont create the index file if we're opening an existing dataset
      // this is because we may be opening a random access file (RAF) and not a random access indexed file
      indexMode &= ~constants.O_CREAT
    }

    // open the index file
    var oldIndexFd = self.indexFd
    try {
      self.indexFd = await fsp.open(self.indexFilename, indexMode)
      if (oldIndexFd) await fsp.close(oldIndexFd)
    } catch (err) {
      if (isContentFileNew) {
        throw err
      }
      // ignore this error if the content file is not new
      // a missing .index file means that we're interacting with a non-indexed RAF
      // therefore we should fallback into RAF behavior
    }

    if (self.indexFd) {
      if (isContentFileNew) {
        // write the index header
        self._nextPtr = self._blockSize
        await writeIndexSlot(self, BLOCK_SIZE_SLOT, self._blockSize)
        await writeIndexSlot(self, NEXT_PTR_SLOT, self._nextPtr)
      } else {
        // read the index header
        self._blockSize = await readIndexSlot(self, BLOCK_SIZE_SLOT)
        self._nextPtr = await readIndexSlot(self, NEXT_PTR_SLOT)
      }
    }

    req.callback(null)
  } catch (err) {
    await safeClose(self, 'indexFd')
    await safeClose(self, 'contentFd')
    req.callback(err)
  }
}

async function safeClose (self, fdname) {
  if (self[fdname]) {
    try { await fsp.close(self[fdname]) }
    catch (e) {/*ignore*/}
    self[fdname] = 0
  }
}

async function allocatePointer (self, offset) {
  // allocate the next available block pointer
  var pointer = self._nextPtr
  self._nextPtr += self._blockSize

  // update the index file
  await writeIndexSlot(self, NEXT_PTR_SLOT, self._nextPtr)
  await writeIndexSlot(self, OFFSET_TO_SLOT(offset, self._blockSize), pointer)

  // return the pointer
  var blockOffset = offset % self._blockSize
  return {
    fileOffset: pointer + blockOffset, // where in the content file to read
    blockOffset // where within the block are we reading?
  }
}

async function readPointer (self, offset) {
  if (!self.indexFd) {
    // no index, fallback to the default sparse-file offsets
    return {
      fileOffset: offset,
      blockOffset: offset % self._blockSize,
    }
  }

  if (offset < self._blockSize) {
    // special case the block 0
    // we interpret 0s in the index as unassigned blocks
    // which works fine except for block 0, where 0 is actually the assignment
    // so by special casing index 0 to always map to block 0, we avoid that confusion
    return {fileOffset: offset, blockOffset: offset}
  }

  // read the pointer
  var pointer = await readIndexSlot(self, OFFSET_TO_SLOT(offset, self._blockSize))
  if (!pointer) return null // no pointer assigned

  var blockOffset = offset % self._blockSize
  return {
    fileOffset: pointer + blockOffset, // where in the content file to read
    blockOffset // where within the block are we reading?
  }
}

async function readIndexSlot (self, slot) {
  var buf = Buffer.alloc(6)
  var res = await fsp.read(self.indexFd, buf, 0, 6, slot * 6)
  if (res.bytesRead !== 6) return 0 // no data received
  var value = uint48be.decode(buf)
  if (!value) return 0 // no pointer assigned
  return value
}

async function writeIndexSlot (self, slot, value) {
  var res = await fsp.write(self.indexFd, uint48be.encode(value), 0, 6, slot * 6)
  if (res.bytesWritten !== 6) {
    throw new Error('Failed to write index')
  }
}

function readEmpty (req) {
  req.callback(null, Buffer.alloc(0))
}