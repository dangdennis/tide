// Learn more about moon.mod configuration:
// https://docs.moonbitlang.com/en/latest/toolchain/moon/module.html
//
// To add a dependency, run this command in your terminal:
//   moon add moonbitlang/x
//
// Or manually declare it in `import`, for example:
// import {
//   "moonbitlang/x@0.4.6",
// }

name = "dangdennis/tide"

version = "0.1.0"

preferred_target = "native"

supported_targets = ["native"]

readme = "README.mbt.md"

repository = ""

license = "Apache-2.0"

keywords = [ ]

description = ""

import {
  "moonbitlang/async@0.19.1",
  "moonbit-community/postgres@0.0.6",
  "moonbit-community/sqlite3@0.1.5",
}
