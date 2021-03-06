import { Reader } from "ckb-js-toolkit";

export function DenormalizeScript(script) {
  return {
    code_hash: new Reader(script.getCodeHash().raw()).serializeJson(),
    hash_type: script.getHashType() === 0 ? "data" : "type",
    args: new Reader(script.getArgs().raw()).serializeJson()
  };
}
