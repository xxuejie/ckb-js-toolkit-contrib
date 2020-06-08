import { validators } from "ckb-js-toolkit";
const { ValidateCellOutput, ValidateOutPoint, ValidateScript } = validators;

function validateHexString(debugPath, string, length = 0) {
  if (!/^0x([0-9a-fA-F][0-9a-fA-F])*$/.test(string)) {
    throw new Error(`${debugPath} must be a hex string!`);
  }
  if (length > 0 && string.length != length) {
    throw new Error(`${debugPath} must be ${length} bytes long!`);
  }
}

export function ValidateCollectorCell(cell, { requireData = false } = {}) {
  ValidateOutputCell(cell, { requireData });
  ValidateOutPoint(cell.out_point);
  validateHexString("cell.block_hash", cell.block_hash, 66);
}

export function ValidateOutputCell(cell, { requireData = false } = {}) {
  ValidateCellOutput(cell.cell_output);
  if (requireData && !cell.data) {
    throw new Error("Required cell data is missing!");
  }
  if (cell.data) {
    validateHexString("cell.data", cell.data);
  }
}
