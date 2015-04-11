// Copyright Joyent, Inc. and other Node contributors.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the
// following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
// USE OR OTHER DEALINGS IN THE SOFTWARE.


//var isWindows = process.platform === 'win32';
var isArray           = require('util-ex/lib/is/type/array');
var isString          = require('util-ex/lib/is/type/string');
var isNullOrUndefined = require('util-ex/lib/is/type/null-or-undefined');

var SEP = require("./codec/separator");
//var PATH_SEP = SEP.PATH_SEP

//convert path string to path array.
function toArray(aPath) {
  var PATH_SEP = SEP.PATH_SEP;
  while (aPath.length && aPath[0] === PATH_SEP) {
    aPath = aPath.substring(1);
  }
  while (aPath.length && aPath[aPath.length - 1] === PATH_SEP) {
    aPath = aPath.substring(0, aPath.length - 1);
  }
  if (aPath.length) {
    return aPath.split(PATH_SEP);
  } else {
    return [];
  }
}
exports.toArray = toArray;

// resolves . and .. elements in a path array with directory names there
// must be no slashes, empty elements, or device names (c:\) in the array
// (so also no leading and trailing slashes - it does not distinguish
// relative and absolute paths)
// if parts[0] is "." means relative path and allowAboveRoot.
function normalizeArray(parts, allowAboveRoot) {
  // if the path tries to go above the root, `up` ends up > 0
  var up = 0;
  if (isNullOrUndefined(allowAboveRoot)) {
      switch (parts[0]) {
        case '.':
            allowAboveRoot = true;
            break;
        case SEP.PATH_SEP:
            allowAboveRoot =false;
            parts.splice(0,1);
            break;
      }
  }
  for (var i = parts.length - 1; i >= 0; i--) {
    var last = parts[i];
    if (last === '.') {
      parts.splice(i, 1);
    } else if (last === '..') {
      parts.splice(i, 1);
      up++;
    } else if (up) {
      parts.splice(i, 1);
      up--;
    }
  }

  // if the path is allowed to go above the root, restore leading ..s
  if (allowAboveRoot) {
    for (; up--; up) {
      parts.unshift('..');
    }
  }

  return parts;
}

exports.normalizeArray = normalizeArray;

/* posix */

  // Split a filename into [root, dir, basename, ext], unix version
  // 'root' is just a slash, or nothing.
  //var splitPathRe =
  //    /^(\/?|)([\s\S]*?)((?:\.{1,2}|[^\/]+?|)(\.[^.\/]*|))(?:[\/]*)$/;
  //    /^(\\/?|)([\\s\\S]*?)((?:\\.{1,2}|[^\\/]+?|)(\\.[^.\\/]*|))(?:[\\/]*)$/
  var splitPathRe = new RegExp("^(\\"+SEP.PATH_SEP+"?|)([\\s\\S]*?)((?:\\.{1,2}|[^\\"+SEP.PATH_SEP+"]+?|)(\\.[^.\\"+SEP.PATH_SEP+"]*|))(?:[\\"+SEP.PATH_SEP+"]*)$");
  var splitPath = function(filename) {
    return splitPathRe.exec(filename).slice(1);
  };

  // path.resolve([from ...], to)
  // posix version
  // return [resolvedAbsolute, parts...]
  // the arguments can be string or array
  resolveArray = exports.resolveArray = function() {
    var resolvedPath = [],
        resolvedAbsolute = false;

    for (var i = arguments.length - 1; i >= -1 && !resolvedAbsolute; i--) {
      var path = (i >= 0) ? arguments[i] : ".";

      // Skip empty and invalid entries
      if (isArray(path)) {
          if (path.length === 0) {
            //treat empty array as root path
            resolvedAbsolute = true;
          } else {
            resolvedPath = path.filter(Boolean).concat(resolvedPath);
            resolvedAbsolute = path[0] === "" || path[0] !== '.'
          }
          continue;
      } else if (path == null) {
        continue;
      } else if (!isString(path)) {
        throw new TypeError('Arguments to path.resolve must be string or array');
      }

      resolvedPath = path.split(SEP.PATH_SEP).filter(Boolean).concat(resolvedPath);
      //resolvedPath = path + '/' + resolvedPath;
      resolvedAbsolute = path.charAt(0) === SEP.PATH_SEP;
    }

    // At this point the path should be resolved to a full absolute path, but
    // handle relative paths to be safe (might happen when process.cwd() fails)

    // Normalize the path
    //resolvedPath = normalizeArray(resolvedPath.split('/').filter(function(p) {
    //  return !!p;
    //}), !resolvedAbsolute).join('/');
    resolvedPath = normalizeArray(resolvedPath, !resolvedAbsolute);
    resolvedPath.unshift(resolvedAbsolute);
    return resolvedPath;
  };

  exports.resolve = function() {
    var resolvedPath = resolveArray.apply(null,arguments);
    var resolvedAbsolute = resolvedPath[0];
    resolvedPath.shift(0,1);
    return ((resolvedAbsolute ? SEP.PATH_SEP : '') + resolvedPath.join(SEP.PATH_SEP)) || '.';
  };

  // path.normalize(path)
  // posix version
  exports.normalize = function(path) {
    var isAbsolute = exports.isAbsolute(path),
        trailingSlash = path[path.length - 1] === SEP.PATH_SEP,
        segments = path.split(SEP.PATH_SEP),
        nonEmptySegments = [];

    // Normalize the path
    for (var i = 0; i < segments.length; i++) {
      if (segments[i]) {
        nonEmptySegments.push(segments[i]);
      }
    }
    path = normalizeArray(nonEmptySegments, !isAbsolute).join(SEP.PATH_SEP);

    if (!path && !isAbsolute) {
      path = '.';
    }
    if (path && trailingSlash) {
      path += SEP.PATH_SEP;
    }

    return (isAbsolute ? SEP.PATH_SEP : '') + path;
  };

  // posix version
  exports.isAbsolute = function(path) {
    return path.charAt(0) === SEP.PATH_SEP;
  };

  // path.join can mixture array and string arguments now!
  // path.join("path1", ["path2", "path3"], "path4")
  // = path1/path2/path3/path4
  exports.join = function() {
    var path = '';
    var vPathSep = SEP.PATH_SEP;
    for (var i = 0; i < arguments.length; i++) {
      var segment = arguments[i];
      if (isArray(segment)) {
          if (segment.length === 0 && path.length === 0) {
              path = vPathSep;
          } else {
            segment = segment.filter(Boolean).join(vPathSep);
            if (segment) {
              path += vPathSep + segment;
            }
          }
          continue;
      } else if (isNullOrUndefined(segment)) {
        // Skip empty and invalid entries
        continue;
      } else if (!isString(segment)) {
        throw new TypeError('Arguments to path.join must be string or arrays');
      }

      if (segment) {
        if (!path) {
          path += segment;
        } else {
          path += vPathSep + segment;
        }
      }
    }
    return exports.normalize(path);
  };


  // path.relative(from, to)
  // posix version
  exports.relative = function(from, to) {
    var vPathSep = SEP.PATH_SEP;
    var fromParts = resolveArray(from);
    var toParts = resolveArray(to);
    fromParts.shift(0,1);
    toParts.shift(0,1);

    /*
    function trim(arr) {
      var start = 0;
      for (; start < arr.length; start++) {
        if (arr[start] !== '') break;
      }

      var end = arr.length - 1;
      for (; end >= 0; end--) {
        if (arr[end] !== '') break;
      }

      if (start > end) return [];
      return arr.slice(start, end + 1);
    }

    var fromParts = trim(from.split(vPathSep));
    var toParts = trim(to.split(vPathSep));
    */

    var length = Math.min(fromParts.length, toParts.length);
    var samePartsLength = length;
    var i;
    for (i = 0; i < length; i++) {
      if (fromParts[i] !== toParts[i]) {
        samePartsLength = i;
        break;
      }
    }

    var outputParts = [];
    for (i = samePartsLength; i < fromParts.length; i++) {
      outputParts.push('..');
    }

    outputParts = outputParts.concat(toParts.slice(samePartsLength));

    return outputParts.join(vPathSep);
  };

  exports.sep = SEP.PATH_SEP;
  exports.delimiter = ':';

exports.dirname = function(path) {
  var result = splitPath(path),
      root = result[0],
      dir = result[1];
  if (!root && !dir) {
    // No dirname whatsoever
    return '.';
  }

  if (dir) {
    // It has a dirname, strip trailing slash
    dir = dir.substr(0, dir.length - 1);
  }
  return root + dir;
};


exports.basename = function(path, ext) {
  var f = splitPath(path)[2];
  if (ext && f.substr(-1 * ext.length) === ext) {
    f = f.substr(0, f.length - ext.length);
  }
  return f;
};


exports.extname = function(path) {
  return splitPath(path)[3];
};
