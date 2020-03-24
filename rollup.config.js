import resolve from "@rollup/plugin-node-resolve";
import commonjs from "@rollup/plugin-commonjs";
import json from "@rollup/plugin-json";
import builtins from "builtin-modules";

module.exports = [
  {
    input: "src/index.js",
    output: {
      file: "build/ckb-js-toolkit-contrib.node.js",
      format: "cjs",
      sourcemap: true
    },
    plugins: [
      resolve({
        preferBuiltins: true,
      }),
      commonjs(),
      json()
    ],
    external: builtins.concat(["ckb-js-toolkit", "nohm", "blake2b"])
  }
];
