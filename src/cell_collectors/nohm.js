import { Reader, BigIntToHexString, validators } from "ckb-js-toolkit";
const { ValidateOutPoint, ValidateScript } = validators;
import { Nohm, NohmModel } from "nohm";
import JSBI from "jsbi";
import { promisify } from "util";
import { ValidateCollectorCell, ValidateOutputCell } from "./utils";

const MAXIMUM_KEPT_BYTES = 128;
const MAXIMUM_KEPT_HEX_SIZE = MAXIMUM_KEPT_BYTES * 2 + 2;

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

class LiveCellClass extends NohmModel {
  setCell(cell) {
    ValidateCollectorCell(cell, { requireData: true });
    this.cell = cell;
    this.setOutPoint(cell.out_point);
    this.property("capacity", cell.cell_output.capacity);
    this.setLock(cell.cell_output.lock);
    if (cell.cell_output.type) {
      this.setType(cell.cell_output.type);
    }
    if (cell.data.length <= MAXIMUM_KEPT_HEX_SIZE) {
      this.property("data", cell.data);
    }
    this.property("data_length", cell.data.length);
  }

  outPoint() {
    return deserializeOutPoint(this.property("out_point"));
  }

  setOutPoint(outPoint) {
    this.property("out_point", serializeOutPoint(outPoint));
    return this;
  }

  async lock(rpc = null) {
    let args = this.property("lock_args");
    if (!args && !rpc) {
      throw new Error("RPC is needed to fetch lock args!");
    }
    if (!args) {
      await this._loadCell(rpc);
      args = this.cell.lock.args;
    }
    return {
      code_hash: this.property("lock_code_hash"),
      hash_type: this.property("lock_hash_type"),
      args
    };
  }

  setLock(lock) {
    ValidateScript(lock);
    this.property("lock_code_hash", lock.code_hash);
    this.property("lock_hash_type", lock.hash_type);
    if (lock.args.length <= MAXIMUM_KEPT_HEX_SIZE) {
      this.property("lock_args", lock.args);
    }
  }

  async type(rpc = null) {
    const code_hash = this.property("type_code_hash");
    if (!code_hash) {
      return null;
    }
    let args = this.property("type_args");
    if (!args && !rpc) {
      throw new Error("RPC is needed to fetch lock args!");
    }
    if (!args) {
      await this._loadCell(rpc);
      args = this.cell.type.args;
    }
    return {
      code_hash,
      hash_type: this.property("type_hash_type"),
      args
    };
  }

  setType(type) {
    ValidateScript(type);
    this.property("type_code_hash", type.code_hash);
    this.property("type_hash_type", type.hash_type);
    if (type.args.length <= MAXIMUM_KEPT_HEX_SIZE) {
      this.property("type_args", type.args);
    }
  }

  async data(rpc = null) {
    let data = this.property("data");
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

LiveCellClass.modelName = "LiveCell";
LiveCellClass.definitions = {
  out_point: {
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
  capacity: {
    type: "string"
  },
  lock_hash: {
    type: "string",
    index: true
  },
  lock_code_hash: {
    type: "string",
    index: true
  },
  lock_hash_type: {
    type: "string",
    index: true
  },
  type_hash: {
    type: "string",
    index: true
  },
  type_code_hash: {
    type: "string",
    index: true
  },
  type_hash_type: {
    type: "string",
    index: true
  },
  // Only values that are less than 128 bytes are stored in the model fields
  // below. Longer values only have the length field here set, and requires
  // manual fetching from CKB.
  data: {
    type: "string"
  },
  data_length: {
    type: "integer"
  },
  lock_args: {
    type: "string"
  },
  lock_args_length: {
    type: "integer"
  },
  type_args: {
    type: "string"
  },
  type_args_length: {
    type: "integer"
  },
  block_hash: {
    type: "string"
  },
  spent: {
    type: "boolean",
    defaultValue: false
  }
};

export const LiveCell = Nohm.register(LiveCellClass);

function asyncSleep(ms = 1) {
  return new Promise(r => setTimeout(r, ms));
}

export class Indexer {
  constructor(rpc, redisClient, registerNohm = true) {
    this.rpc = rpc;
    this.redisClient = redisClient;
    this.registerNohm = registerNohm;
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
          throw new Error("TODO: handle fork!");
        }
      }

      let spentIds = [];
      const cells = [];

      for (const transaction of block.transactions) {
        for (const input of transaction.inputs) {
          const ids = await LiveCell.find({
            out_point: serializeOutPoint(input.previous_output)
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
        const c = await Nohm.factory("LiveCell");
        const ids = await LiveCell.find({
          out_point: serializeOutPoint(cell.out_point)
        });
        if (ids.length > 0) {
          await c.load(ids[0]);
        }
        c.setCell(cell);
        c.property("block_hash", block.header.hash);
        await c.save();
      }
      const spentCells = await LiveCell.loadMany(spentIds);
      for (const spentCell of spentCells) {
        spentCell.spent = true;
        await spentCell.save();
      }

      // TODO: purge spent cells that are too old for a fork.
      await setAsync(
        `BLOCK:${BigIntToHexString(blockNumber)}:HASH`,
        block.header.hash
      );
      await setAsync("LAST_PROCESSED_NUMBER", BigIntToHexString(blockNumber));

      console.log("Indexed block: ", blockNumber.toString());
      lastProcessedBlockNumber = blockNumber;
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
    this.filters = Object.assign({}, this.filters);
    if (skipCellWithContent) {
      this.filters.data_length = 0;
    }
    this.loadData = loadData;
  }

  async *collect() {
    const cells = await LiveCell.findAndLoad(this.filters);
    for (const cell of cell) {
      const lock = await cell.lock(rpc);
      const type = await cell.type(rpc);
      let data = null;
      if (this.loadData) {
        data = await cell.data(rpc);
      }
      yield {
        cell_output: {
          capacity: cell.property("capacity"),
          lock,
          type
        },
        out_point: cell.outPoint(),
        block_hash: cell.property("block_hash"),
        data
      };
    }
  }
}
