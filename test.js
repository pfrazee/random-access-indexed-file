var raif = require('./')
var raf = require('random-access-file')
var tape = require('tape')
var os = require('os')
var path = require('path')
var fs = require('fs')
var mkdirp = require('mkdirp')

var tmp = path.join(os.tmpdir(), 'random-access-indexed-file-' + process.pid + '-' + Date.now())
var i = 0

mkdirp.sync(tmp)

tape('write and read', function (t) {
  var file = raif(gen())

  file.write(0, Buffer.from('hello'), function (err) {
    t.error(err, 'no error')
    file.read(0, 5, function (err, buf) {
      t.error(err, 'no error')
      t.same(buf, Buffer.from('hello'))
      file.destroy(() => t.end())
    })
  })
})

tape('read empty', function (t) {
  var file = raif(gen(), {writable: true})

  file.read(0, 0, function (err, buf) {
    t.error(err, 'no error')
    t.same(buf, Buffer.alloc(0), 'empty buffer')
    file.destroy(() => t.end())
  })
})

tape('read range > file', function (t) {
  var file = raif(gen())

  file.read(0, 5, function (err, buf) {
    t.ok(err, 'not satisfiable')
    file.destroy(() => t.end())
  })
})

tape('read range > file with data', function (t) {
  var file = raif(gen())

  file.write(0, Buffer.from('hello'), function (err) {
    t.error(err, 'no error')
    file.read(0, 10, function (err, buf) {
      t.ok(err, 'not satisfiable')
      file.destroy(() => t.end())
    })
  })
})

tape('random access write and read', function (t) {
  var file = raif(gen())

  file.write(10, Buffer.from('hi'), function (err) {
    t.error(err, 'no error')
    file.write(0, Buffer.from('hello'), function (err) {
      t.error(err, 'no error')
      file.read(10, 2, function (err, buf) {
        t.error(err, 'no error')
        t.same(buf, Buffer.from('hi'))
        file.read(0, 5, function (err, buf) {
          t.error(err, 'no error')
          t.same(buf, Buffer.from('hello'))
          file.read(5, 5, function (err, buf) {
            t.error(err, 'no error')
            t.same(buf, Buffer.from([0, 0, 0, 0, 0]))
            t.end()
          })
        })
      })
    })
  })
})

tape('write on block boundary', function (t) {
  var file = raif(gen(), {blockSize: 3})

  file.write(9, Buffer.from('hi'), function (err) {
    t.error(err, 'no error')
    file.write(3, Buffer.from('aaabbb'), function (err) {
      t.error(err, 'no error')
      file.read(9, 2, function (err, buf) {
        t.error(err, 'no error')
        t.same(buf, Buffer.from('hi'))
        file.read(3, 6, function (err, buf) {
          t.error(err, 'no error')
          t.same(buf, Buffer.from('aaabbb'))
          t.end()
        })
      })
    })
  })
})

tape('random access write and read across blocks', function (t) {
  var file = raif(gen(), {blockSize: 6})

  file.write(10, Buffer.from('hi'), function (err) {
    t.error(err, 'no error')
    file.write(0, Buffer.from('hello'), function (err) {
      t.error(err, 'no error')
      file.write(33, Buffer.from('longerthanonechunk'), function (err) {
        t.error(err, 'no error')
        file.read(10, 2, function (err, buf) {
          t.error(err, 'no error')
          t.same(buf, Buffer.from('hi'))
          file.read(0, 5, function (err, buf) {
            t.error(err, 'no error')
            t.same(buf, Buffer.from('hello'))
            file.read(5, 5, function (err, buf) {
              t.error(err, 'no error')
              t.same(buf, Buffer.from([0, 0, 0, 0, 0]))
              file.read(33, 18, function (err, buf) {
                t.error(err, 'no error')
                t.same(buf, Buffer.from('longerthanonechunk'))
                t.end()
              })
            })
          })
        })
      })
    })
  })
})

tape('re-open', function (t) {
  var name = gen()
  var file = raif(name)

  file.write(10, Buffer.from('hello'), function (err) {
    t.error(err, 'no error')
    var file2 = raif(name)
    file2.read(10, 5, function (err, buf) {
      t.error(err, 'no error')
      t.same(buf, Buffer.from('hello'))
      t.end()
    })
  })
})

tape('bad open', function (t) {
  var file = raif(tmp, {writable: true})

  file.open(function (err) {
    t.ok(err)
    file.close(() => t.end())
  })
})

tape('mkdir path', function (t) {
  var name = path.join(tmp, ++i + '-folder', 'test.txt')
  var file = raif(name)

  file.write(0, Buffer.from('hello'), function (err) {
    t.error(err, 'no error')
    file.read(0, 5, function (err, buf) {
      t.error(err, 'no error')
      t.same(buf, Buffer.from('hello'))
      t.end()
      file.destroy()
    })
  })
})

tape('write/read big chunks', function (t) {
  var file = raif(gen())
  var bigBuffer = Buffer.alloc(10 * 1024 * 1024)
  var missing = 2

  bigBuffer.fill('hey. hey. how are you doing?. i am good thanks how about you? i am good')

  file.write(0, bigBuffer, function (err) {
    t.error(err, 'no error')
    file.read(0, bigBuffer.length, function (err, buf) {
      t.error(err, 'no error')
      t.same(buf, bigBuffer)
      done()
    })
  })
  file.write(bigBuffer.length * 2, bigBuffer, function (err) {
    t.error(err, 'no error')
    file.read(bigBuffer.length * 2, bigBuffer.length, function (err, buf) {
      t.error(err, 'no error')
      t.same(buf, bigBuffer)
      done()
    })
  })

  function done () {
    if (!--missing) file.destroy(() => t.end())
  }
})

tape('write/read big chunks at a slight offset', function (t) {
  var file = raif(gen())
  var bigBuffer = Buffer.alloc(10 * 1024 * 1024)
  var missing = 2

  bigBuffer.fill('hey. hey. how are you doing?. i am good thanks how about you? i am good')

  file.write(15, bigBuffer, function (err) {
    t.error(err, 'no error')
    file.read(15, bigBuffer.length, function (err, buf) {
      t.error(err, 'no error')
      t.same(buf, bigBuffer)
      done()
    })
  })
  file.write(bigBuffer.length * 2 + 15, bigBuffer, function (err) {
    t.error(err, 'no error')
    file.read(bigBuffer.length * 2 + 15, bigBuffer.length, function (err, buf) {
      t.error(err, 'no error')
      t.same(buf, bigBuffer)
      done()
    })
  })

  function done () {
    if (!--missing) file.destroy(() => t.end())
  }
})

tape('rmdir option', function (t) {
  var name = path.join('rmdir', ++i + '', 'folder', 'test.txt')
  var file = raif(name, {rmdir: true, directory: tmp})

  file.write(0, Buffer.from('hi'), function (err) {
    t.error(err, 'no error')
    file.read(0, 2, function (err, buf) {
      t.error(err, 'no error')
      t.same(buf, Buffer.from('hi'))
      file.destroy(ondestroy)
    })
  })

  function ondestroy (err) {
    t.error(err, 'no error')
    fs.stat(path.join(tmp, 'rmdir'), function (err) {
      t.same(err && err.code, 'ENOENT', 'should be removed')
      t.end()
    })
  }
})

tape('write/read big chunks with overlap', function (t) {
  var file = raif(gen())
  var bigBuffer = Buffer.alloc(10 * 1024 * 1024)
  var missing = 2

  bigBuffer.fill('hey. hey. how are you doing?. i am good thanks how about you? i am good')

  file.write(0, bigBuffer, function (err) {
    t.error(err, 'no error')
    file.read(0, bigBuffer.length, function (err, buf) {
      t.error(err, 'no error')
      t.same(buf, bigBuffer)

      file.write(50, bigBuffer, function (err) {
        t.error(err, 'no error')
        file.read(50, bigBuffer.length, function (err, buf) {
          t.error(err, 'no error')
          t.same(buf, bigBuffer)
          file.destroy(() => t.end())
        })
      })
    })
  })
})

tape('rmdir option with non empty parent', function (t) {
  var name = path.join('rmdir', ++i + '', 'folder', 'test.txt')
  var nonEmpty = path.join(tmp, name, '../..')
  var file = raif(name, {rmdir: true, directory: tmp})

  file.write(0, Buffer.from('hi'), function (err) {
    t.error(err, 'no error')
    fs.writeFileSync(path.join(nonEmpty, 'thing'), '')
    file.read(0, 2, function (err, buf) {
      t.error(err, 'no error')
      t.same(buf, Buffer.from('hi'))
      file.destroy(ondestroy)
    })
  })

  function ondestroy (err) {
    t.error(err, 'no error')
    fs.stat(path.join(tmp, 'rmdir'), function (err) {
      t.error(err, 'should not be removed')
      fs.readdir(nonEmpty, function (err, list) {
        t.error(err, 'no error')
        t.same(list, ['thing'], 'should only be one entry')
        t.end()
      })
    })
  }
})

tape('del', function (t) {
  var file = raif(gen())

  file.write(0, Buffer.alloc(100), function (err) {
    t.error(err, 'no error')
    file.stat(function (err, st) {
      t.error(err, 'no error')
      t.same(st.size, 100)
      file.del(0, 40, function (err) {
        t.error(err, 'no error')
        file.stat(function (err, st) {
          t.error(err, 'no error')
          t.same(st.size, 100, 'inplace del, same size')
          file.del(50, 50, function (err) {
            t.error(err, 'no error')
            file.stat(function (err, st) {
              t.error(err, 'no error')
              t.same(st.size, 50)
              file.destroy(() => t.end())
            })
          })
        })
      })
    })
  })
})

tape('open and close many times', function (t) {
  var name = gen()
  var file = raif(name)
  var buf = Buffer.alloc(4)

  file.write(0, buf, function (err) {
    t.error(err, 'no error')
    loop(5000, function (err) {
      t.error(err, 'no error')
      file.destroy(() => t.end())
    })
  })

  function loop (n, cb) {
    var file = raif(name)
    file.read(0, 4, function (err, buffer) {
      if (err) return cb(err)
      if (!buf.equals(buffer)) {
        t.same(buffer, buf)
        return cb()
      }
      buf.writeUInt32BE(n)
      file.write(0, buf, function (err) {
        if (err) return cb(err)
        file.close(function (err) {
          if (!n || err) return cb(err)
          loop(n - 1, cb)
        })
      })
    })
  }
})

tape('trigger bad open', function (t) {
  var file = raif(gen(), {writable: true})

  file.contentFd = 10000
  file.open(function (err) {
    t.ok(err, 'should error trying to close old fd')
    file.open(function (err) {
      t.error(err, 'no error')
      file.destroy(() => t.end())
    })
  })
})

tape('correctly open a random-access-file', function (t) {
  var datapath = gen()
  var file = raf(datapath)
  run(function () {
    file.close(function (err) {
      t.error(err, 'no error')
      file = raif(datapath)
      run(function () {
        t.end()
      })
    })
  })

  function run (cb) {
    file.write(10, Buffer.from('hi'), function (err) {
      t.error(err, 'no error')
      file.write(0, Buffer.from('hello'), function (err) {
        t.error(err, 'no error')
        file.read(10, 2, function (err, buf) {
          t.error(err, 'no error')
          t.same(buf, Buffer.from('hi'))
          file.read(0, 5, function (err, buf) {
            t.error(err, 'no error')
            t.same(buf, Buffer.from('hello'))
            file.read(5, 5, function (err, buf) {
              t.error(err, 'no error')
              t.same(buf, Buffer.from([0, 0, 0, 0, 0]))
              cb()
            })
          })
        })
      })
    })
  }
})

function gen () {
  return path.join(tmp, ++i + '.txt')
}