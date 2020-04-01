import blake2b from "blake2b";
import { Reader } from "ckb-js-toolkit";

export class Hasher {
  constructor() {
    this.h = blake2b(
      32,
      null,
      null,
      new Uint8Array(Reader.fromRawString("ckb-default-hash").toArrayBuffer())
    );
  }

  update(data) {
    this.h.update(new Uint8Array(new Reader(data).toArrayBuffer()));
    return this;
  }

  digest() {
    const out = new Uint8Array(32);
    this.h.digest(out);
    return new Reader(out.buffer);
  }

  static hash(data) {
    return new Hasher().update(data).digest();
  }
}
