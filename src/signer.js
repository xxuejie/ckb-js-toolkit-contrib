import { Reader, normalizers, validators } from "ckb-js-toolkit";
const { NormalizeRawTransaction } = normalizers;
const { ValidateTransaction } = validators;
import JSBI from "jsbi";
import { SerializeRawTransaction } from "./blockchain";
import { Hasher } from "./hasher";
import { ValidateCollectorCell } from "./cell_collectors/utils";

function serializeBigInt(i) {
  i = JSBI.BigInt(i);
  const view = new DataView(new ArrayBuffer(8));
  view.setBigUint64(0, i, true);
  return view.buffer;
}

export function GenerateSigningMessages(
  tx,
  inputCells,
  { validateTransaction: true } = {}
) {
  if (validateTransaction) {
    ValidateTransaction(tx);
    if (tx.inputs.length != inputCells.length) {
      throw new Error("Input number does not match!");
    }
    for (const inputCell of inputCells) {
      ValidateCollectorCell(inputCell);
    }
  }
  const txHash = Hasher.hash(
    new Reader(SerializeRawTransaction(NormalizeRawTransaction(tx)))
  );
  const messages = [];
  const used = tx.inputs.map(_input => false);
  for (let i = 0; i < tx.inputs.length; i++) {
    if (used[i]) {
      continue;
    }
    if (i >= tx.witnesses.length) {
      throw new Error(
        `Input ${i} starts a new script group, but witness is missing!`
      );
    }
    used[i] = true;
    const hasher = new Hasher();
    hasher.update(txHash);
    const firstWitness = new Reader(tx.witnesses[i]);
    hasher.update(serializeBigInt(firstWitness.length()));
    hasher.update(firstWitness);
    for (let j = i + 1; j < tx.inputs.length && j < tx.witnesses.length; j++) {
      if (
        inputCells[i].cell_output.lock.code_hash ===
          inputCells[j].cell_output.lock.code_hash &&
        inputCells[i].cell_output.lock.hash_type ===
          inputCells[j].cell_output.lock.hash_type &&
        inputCells[i].cell_output.lock.args ===
          inputCells[j].cell_output.lock.args
      ) {
        used[j] = true;
        const currentWitness = new Reader(tx.witnesses[j]);
        hasher.update(serializeBigInt(currentWitness.length()));
        hasher.update(currentWitness);
      }
    }
    messages.push({
      index: i,
      message: hasher.digest().serializeJson(),
      lock: inputCells[i].cell_output.lock
    });
  }
  return { tx, messages };
}

export function FillSignedWitnesses(tx, messages, witnesses) {
  if (messages.length != witnesses.length) {
    throw new Error("Invalid number of witnesses!");
  }
  for (let i = 0; i < messages.length; i++) {
    tx.witnesses[messages.index] = witnesses[i];
  }
  return tx;
}
