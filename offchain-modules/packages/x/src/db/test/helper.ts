import os from 'os';
import { createConnection } from 'typeorm';
import { genRandomHex } from '../../utils';
import { CkbBurn, CkbMint, EthLock, EthUnlock, TronLock, TronUnlock, AdaLock, AdaUnlock } from '../model';

export async function getTmpConnection(path = `${os.tmpdir()}/${genRandomHex(32)}/db.sqlite`) {
  const connection = await createConnection({
    type: 'sqlite',
    database: path,
    entities: [CkbBurn, CkbMint, EthLock, EthUnlock, TronLock, TronUnlock, AdaLock, AdaUnlock],
    synchronize: true,
    logging: true,
  });
  return {
    path,
    connection,
  };
}
