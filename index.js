var inherits = require('util').inherits
var promisify = require('util').promisify
var RandomAccess = require('random-access-storage')
var fs = require('fs')
var fsp = {
  open: promisify(fs.open),
  close: promisify(fs.close),
  unlink: promisify(fs.unlink),
  rmdir: promisify(fs.rmdir),
  fstat: promisify(fs.fstat),
  read: promisify(fs.read),
  write: promisify(fs.write),
  ftruncate: promisify(fs.ftruncate)
}
var mkdirp = require('mkdirp')
var path = require('path')
var uint48be = require('uint48be')
var AwaitLock = require('await-lock')
var constants = fs.constants || require('constants')

var READONLY = constants.O_RDONLY
var READWRITE = constants.O_RDWR | constants.O_CREAT

var DEFAULT_BLOCK_SIZE = 1024 // 1mb

var acquireBlockLock = new AwaitLock()

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
      var ptr = await readPointer(this, req.offset)
      if (!ptr) {
        ptr = await allocatePointer(this, req.offset)
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
  // find the next available block
  var st = await fsp.fstat(self.contentFd)
  var pointer = (st.size + self._blockSize - (st.size % self._blockSize))
  // ^ we find the next available block, and never use block 0 (that's special cased)

  // write the pointer
  var res = await fsp.write(self.indexFd, uint48be.encode(pointer), 0, 6, Math.floor(offset / self._blockSize) * 6)
  if (res.bytesWritten !== 6) {
    throw new Error('Failed to write index')
  }

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

  await acquireBlockLock.acquireAsync()
  try {
    // read the pointer
    var buf = Buffer.alloc(6)
    var res = await fsp.read(self.indexFd, buf, 0, 6, Math.floor(offset / self._blockSize) * 6)
    if (res.bytesRead !== 6) return null // no data received
    var pointer = uint48be.decode(buf)
    if (!pointer) return null // no pointer assigned

    var blockOffset = offset % self._blockSize
    return {
      fileOffset: pointer + blockOffset, // where in the content file to read
      blockOffset // where within the block are we reading?
    }
  } finally {
    acquireBlockLock.release()
  }
}

function readEmpty (req) {
  req.callback(null, Buffer.alloc(0))
}