import {
  Reader,
  BigIntToHexString,
  HexStringToBigInt,
  validators,
  normalizers
} from "ckb-js-toolkit";
const { ValidateOutPoint, ValidateScript } = validators;
const { NormalizeScript } = normalizers;
import { Nohm, NohmModel } from "nohm";
import blake2b from "blake2b";
import JSBI from "jsbi";
import { promisify } from "util";
import { SerializeScript } from "../blockchain";
import { ValidateCollectorCell, ValidateOutputCell } from "./utils";

const MAXIMUM_KEPT_BYTES = 128;
const MAXIMUM_KEPT_HEX_SIZE = MAXIMUM_KEPT_BYTES * 2 + 2;

export const KEY_LIVE_CELL = "LC";
export const KEY_OUT_POINT = "o";
export const KEY_CAPACITY = "c";
export const KEY_LOCK_HASH = "l";
export const KEY_LOCK_CODE_HASH = "lc";
export const KEY_LOCK_HASH_TYPE = "lh";
export const KEY_TYPE_HASH = "t";
export const KEY_TYPE_CODE_HASH = "tc";
export const KEY_TYPE_HASH_TYPE = "th";
export const KEY_DATA = "d";
export const KEY_DATA_LENGTH = "dl";
export const KEY_LOCK_ARGS = "la";
export const KEY_LOCK_ARGS_LENGTH = "ll";
export const KEY_TYPE_ARGS = "ta";
export const KEY_TYPE_ARGS_LENGTH = "tl";
export const KEY_BLOCK_HASH = "b";
export const KEY_BLOCK_NUMBER = "n";
export const KEY_SPENT = "s";

// This is at least 11 epochs
const OLD_CELLS_TO_PURGE = JSBI.BigInt(20000);

function serializeOutPoint(outPoint) {
  if (outPoint instanceof Object) {
    ValidateOutPoint(outPoint);
    return outPoint.tx_hash + outPoint.index;
  } else {
    return new Reader(outPoint).serializeJson();
  }
}

function deserializeOutPoint(serializedOutPoint) {
  return {
    tx_hash: serializedOutPoint.substring(0, 66),
    index: serializedOutPoint.substring(66)
  };
}

export function calculateScriptHash(validatedScript) {
  const h = blake2b(
    32,
    null,
    null,
    new Uint8Array(Reader.fromRawString("ckb-default-hash").toArrayBuffer())
  );
  const data = SerializeScript(NormalizeScript(validatedScript));
  h.update(new Uint8Array(data));
  const out = new Uint8Array(32);
  h.digest(out);
  return new Reader(out.buffer).serializeJson();
}

class LiveCellClass extends NohmModel {
  setCell(cell) {
    ValidateCollectorCell(cell, { requireData: true });
    this.cell = cell;
    this.setOutPoint(cell.out_point);
    this.property(KEY_CAPACITY, cell.cell_output.capacity);
    this.setLock(cell.cell_output.lock);
    if (cell.cell_output.type) {
      this.setType(cell.cell_output.type);
    }
    const dataLength = new Reader(cell.data).length();
    if (dataLength <= MAXIMUM_KEPT_HEX_SIZE) {
      this.property(KEY_DATA, cell.data);
    }
    this.property(KEY_DATA_LENGTH, dataLength);
  }

  outPoint() {
    return deserializeOutPoint(this.property(KEY_OUT_POINT));
  }

  setOutPoint(outPoint) {
    this.property(KEY_OUT_POINT, serializeOutPoint(outPoint));
    return this;
  }

  async lock(rpc = null) {
    let args = this.property(KEY_LOCK_ARGS);
    if (!args && !rpc) {
      throw new Error("RPC is needed to fetch lock args!");
    }
    if (!args) {
      await this._loadCell(rpc);
      args = this.cell.lock.args;
    }
    return {
      code_hash: this.property(KEY_LOCK_CODE_HASH),
      hash_type: this.property(KEY_LOCK_HASH_TYPE),
      args
    };
  }

  setLock(lock) {
    ValidateScript(lock);
    this.property(KEY_LOCK_CODE_HASH, lock.code_hash);
    this.property(KEY_LOCK_HASH_TYPE, lock.hash_type);
    const length = new Reader(lock.args).length();
    this.property(KEY_LOCK_ARGS_LENGTH, length);
    if (length <= MAXIMUM_KEPT_HEX_SIZE) {
      this.property(KEY_LOCK_ARGS, lock.args);
    }
    this.property(KEY_LOCK_HASH, calculateScriptHash(lock));
  }

  async type(rpc = null) {
    const code_hash = this.property(KEY_TYPE_CODE_HASH);
    if (!code_hash) {
      return null;
    }
    let args = this.property(KEY_TYPE_ARGS);
    if (!args && !rpc) {
      throw new Error("RPC is needed to fetch lock args!");
    }
    if (!args) {
      await this._loadCell(rpc);
      args = this.cell.type.args;
    }
    return {
      code_hash,
      hash_type: this.property(KEY_TYPE_HASH_TYPE),
      args
    };
  }

  setType(type) {
    ValidateScript(type);
    this.property(KEY_TYPE_CODE_HASH, type.code_hash);
    this.property(KEY_TYPE_HASH_TYPE, type.hash_type);
    const length = new Reader(type.args).length();
    this.property(KEY_LOCK_ARGS_LENGTH, length);
    if (length <= MAXIMUM_KEPT_HEX_SIZE) {
      this.property(KEY_TYPE_ARGS, type.args);
    }
    this.property(KEY_TYPE_HASH, calculateScriptHash(type));
  }

  async data(rpc = null) {
    let data = this.property(KEY_DATA);
    if (!data && !rpc) {
      throw new Error("RPC is needed to fetch data!");
    }
    if (!data) {
      await this._loadCell(rpc);
      data = this.cell.data;
    }
    return data;
  }

  async _loadCell(rpc, forceReload = false) {
    if (this.cell && !forceReload) {
      return;
    }
    const liveCell = await rpc.get_live_cell(this.outPoint(), true);
    this.cell = {
      cell_output: liveCell.cell.output,
      data: liveCell.cell.data.content
    };
    // Just a pre-caution here
    ValidateOutputCell(this.cell);
  }
}

LiveCellClass.modelName = KEY_LIVE_CELL;
LiveCellClass.definitions = {
  [KEY_OUT_POINT]: {
    type: "string",
    index: true,
    unique: true,
    validations: [
      {
        name: "length",
        options: {
          min: 69,
          max: 76
        }
      },
      {
        name: "regexp",
        options: {
          regex: /^0x([0-9a-fA-F][0-9a-fA-F])*0x[0-9a-fA-F]*$/
        }
      }
    ]
  },
  [KEY_CAPACITY]: {
    type: "string"
  },
  [KEY_LOCK_HASH]: {
    type: "string",
    index: true
  },
  [KEY_LOCK_CODE_HASH]: {
    type: "string",
    index: true
  },
  [KEY_LOCK_HASH_TYPE]: {
    type: "string",
    index: true
  },
  [KEY_TYPE_HASH]: {
    type: "string",
    index: true
  },
  [KEY_TYPE_CODE_HASH]: {
    type: "string",
    index: true
  },
  [KEY_TYPE_HASH_TYPE]: {
    type: "string",
    index: true
  },
  // Only values that are less than 128 bytes are stored in the model fields
  // below. Longer values only have the length field here set, and requires
  // manual fetching from CKB.
  [KEY_DATA]: {
    type: "string"
  },
  [KEY_DATA_LENGTH]: {
    type: "integer"
  },
  [KEY_LOCK_ARGS]: {
    type: "string",
    index: true
  },
  [KEY_LOCK_ARGS_LENGTH]: {
    type: "integer"
  },
  [KEY_TYPE_ARGS]: {
    type: "string",
    index: true
  },
  [KEY_TYPE_ARGS_LENGTH]: {
    type: "integer"
  },
  [KEY_BLOCK_HASH]: {
    type: "string",
    index: true
  },
  [KEY_BLOCK_NUMBER]: {
    type: "integer",
    index: true
  },
  [KEY_SPENT]: {
    type: "boolean",
    defaultValue: false,
    index: true
  }
};

export const LiveCell = Nohm.register(LiveCellClass);

function asyncSleep(ms = 1) {
  return new Promise(r => setTimeout(r, ms));
}

export class Indexer {
  constructor(
    rpc,
    redisClient,
    { registerNohm = true, purgeOldBlocks = OLD_CELLS_TO_PURGE, log = console.log } = {}
  ) {
    this.rpc = rpc;
    this.redisClient = redisClient;
    this.registerNohm = registerNohm;
    this.purgeOldBlocks = purgeOldBlocks;
    this.log = log;
  }

  start() {
    if (this.registerNohm) {
      Nohm.setClient(this.redisClient);
    }
    const timeout = 1000;
    const f = () => {
      this.loop()
        .then(() => {
          timeout = 1000;
        })
        .catch(e => {
          console.log(`Error encountered while indexing: ${e} ${e.stack}`);
          timeout *= 2;
        })
        .finally(() => {
          setTimeout(f, timeout);
        });
    };
    setTimeout(f, 1);
  }

  async loop() {
    const getAsync = promisify(this.redisClient.get).bind(this.redisClient);
    const setAsync = promisify(this.redisClient.set).bind(this.redisClient);
    const delAsync = promisify(this.redisClient.del).bind(this.redisClient);

    let lastProcessedBlockNumber = JSBI.BigInt(
      (await getAsync("LAST_PROCESSED_NUMBER")) || "-1"
    );
    while (true) {
      const blockNumber = JSBI.add(lastProcessedBlockNumber, JSBI.BigInt(1));
      const block = await this.rpc.get_block_by_number(
        BigIntToHexString(blockNumber)
      );
      if (!block) {
        await asyncSleep(1000);
        continue;
      }

      if (JSBI.greaterThan(blockNumber, JSBI.BigInt(0))) {
        const previousIndexedBlockHash = await getAsync(
          `BLOCK:${BigIntToHexString(lastProcessedBlockNumber)}:HASH`
        );
        if (previousIndexedBlockHash !== block.header.parent_hash) {
          const lastUnpurgedBlockNumber = await getAsync(
            "LAST_UNPURGED_BLOCK_NUMBER"
          );
          if (lastUnpurgedBlockNumber) {
            if (
              JSBI.lessThan(
                lastProcessedBlockNumber,
                HexStringToBigInt(lastUnpurgedBlockNumber)
              )
            ) {
              throw new Error(
                `The block ${previousIndexedBlockHash} to revert has already been purged!`
              );
            }
          }
          // To revert a block, all we need here is:
          // * Locate all spent cells and unspent cells via previousIndexedBlockHash
          // * Mark those spent as unspent, and delete those unspent cells.
          // * Delete block hash field for previousIndexedBlockHash
          // * Revise LAST_PROCESSED_NUMBER
          const cells = await LiveCell.findAndLoad({
            [KEY_BLOCK_HASH]: previousIndexedBlockHash
          });
          for (const cell of cells) {
            if (cell.property(KEY_SPENT)) {
              cell.property(KEY_SPENT, false);
              await cell.save();
            } else {
              await cell.remove();
            }
          }
          await delAsync(
            `BLOCK:${BigIntToHexString(lastProcessedBlockNumber)}:HASH`
          );
          await setAsync(
            "LAST_PROCESSED_NUMBER",
            BigIntToHexString(JSBI.add(blockNumber, JSBI.BigInt(-1)))
          );
          continue;
        }
      }

      let spentIds = [];
      const cells = [];

      for (const transaction of block.transactions) {
        for (const input of transaction.inputs) {
          const ids = await LiveCell.find({
            [KEY_OUT_POINT]: serializeOutPoint(input.previous_output)
          });
          spentIds = spentIds.concat(ids);
        }

        for (let i = 0; i < transaction.outputs.length; i++) {
          cells.push({
            block_hash: block.header.hash,
            out_point: {
              tx_hash: transaction.hash,
              index: BigIntToHexString(JSBI.BigInt(i))
            },
            cell_output: transaction.outputs[i],
            data: transaction.outputs_data[i]
          });
        }
      }

      for (const cell of cells) {
        const c = await Nohm.factory(KEY_LIVE_CELL);
        const ids = await LiveCell.find({
          [KEY_OUT_POINT]: serializeOutPoint(cell.out_point)
        });
        if (ids.length > 0) {
          await c.load(ids[0]);
        }
        c.setCell(cell);
        c.property(KEY_BLOCK_HASH, block.header.hash);
        // TODO: check if block number can be held in double
        c.property(
          KEY_BLOCK_NUMBER,
          JSBI.toNumber(HexStringToBigInt(block.header.number))
        );
        await c.save();
      }
      const spentCells = await LiveCell.loadMany(spentIds);
      for (const spentCell of spentCells) {
        spentCell.property(KEY_SPENT, true);
        await spentCell.save();
      }

      await setAsync(
        `BLOCK:${BigIntToHexString(blockNumber)}:HASH`,
        block.header.hash
      );
      await setAsync("LAST_PROCESSED_NUMBER", BigIntToHexString(blockNumber));

      this.log("Indexed block: ", blockNumber.toString());
      lastProcessedBlockNumber = blockNumber;

      if (this.purgeOldBlocks) {
        const blockToPurge = JSBI.subtract(blockNumber, this.purgeOldBlocks);
        if (JSBI.toNumber(blockToPurge) % 1000 > 1) {
          continue;
        }
        if (JSBI.greaterThanOrEqual(blockToPurge, JSBI.BigInt(0))) {
          const blockHash = await getAsync(
            `BLOCK:${BigIntToHexString(blockToPurge)}:HASH`
          );
          await setAsync(
            "LAST_UNPURGED_BLOCK_NUMBER",
            JSBI.add(blockToPurge, JSBI.BigInt(1))
          );
          const cells = await LiveCell.findAndLoad({
            [KEY_SPENT]: true,
            [KEY_BLOCK_NUMBER]: {
              min: 0,
              max: JSBI.toNumber(blockToPurge)
            }
          });
          for (const cell of cells) {
            await cell.remove();
          }
          await delAsync(`BLOCK:${BigIntToHexString(blockToPurge)}:HASH`);
        }
      }
    }
  }
}

export class Collector {
  constructor(
    rpc,
    filters,
    { skipCellWithContent = true, loadData = false } = {}
  ) {
    this.rpc = rpc;
    this.filters = Object.assign({}, filters);
    this.filters[KEY_SPENT] = false;
    this.skipCellWithContent = skipCellWithContent;
    this.loadData = loadData;
  }

  async *collect() {
    const cells = await LiveCell.findAndLoad(this.filters);
    for (const cell of cells) {
      const lock = await cell.lock(this.rpc);
      const type = await cell.type(this.rpc);
      let data = null;
      if (this.loadData || this.skipCellWithContent) {
        data = await cell.data(this.rpc);
      }
      // TODO: investigate later why filter does not work.
      if (this.skipCellWithContent) {
        if (data && new Reader(data).length() > 0) {
          continue;
        }
      }
      yield {
        cell_output: {
          capacity: cell.property(KEY_CAPACITY),
          lock,
          type
        },
        out_point: cell.outPoint(),
        block_hash: cell.property(KEY_BLOCK_HASH),
        data
      };
    }
  }
}
