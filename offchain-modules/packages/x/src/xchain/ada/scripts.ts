import { ForceBridgeCore } from '../../core';
import { AdaUnlock, AdaUnlockStatus } from '../../db/entity/AdaUnlock';
import { asserts, nonNullable } from '../../errors';
import { logger } from '../../utils/logger';
import { WalletServer, AddressWallet, Transaction } from 'cardano-wallet-js';
import { AdaDb } from '../../db';
import { createConnection } from 'typeorm';
import {AdaUnlockResult} from "./types"

const AdaLockEventMark = 'ck';
const CkbTxHashLen = 64;
let walletServer;
export class ADAChain {
    protected readonly config;
    //initializing cardano wallet connection and configuration to be accessible throughout the class.
    constructor(){
        this.config = ForceBridgeCore.config.ada;
        const clientParams = this.config.clientParams;
        walletServer = WalletServer.init(`http://${clientParams.host}:${clientParams.port}/v2`);
    }


    /**
   * Locking the asset on ada i.e-sending it to the locked wallet
   * @param {number} amount, amount to be send to other wallet
   * @param {string} passphrase, passphrase to import wallet so that we can send payment as authorized personal
   * @param {string} data_ada, CKB address to on which the xada will be minted.
   */
  async sendLockTxs(id: string, amount: number, passphrase: string, data_ada: string): Promise<string> {
    logger.debug(`lock tx params: amount ${amount}.`);
    //need to lock the amount using transaction and then sign the transaction.

    //need to fetch the amount before locking the amount.
    const wallet = await walletServer.getShelleyWallet(id);
    logger.debug({ wallet });
    const totalBalance = await wallet.getAvailableBalance();
    logger.debug({ totalBalance });
    try {
      // receiver address
      //checking if the amount is good to proceed with or not
      const address = new AddressWallet(ForceBridgeCore.config.ada.lockAddress);
      const estimatedFees = await wallet.estimateFee([address], [amount]);
      logger.debug(`Transaction fee for locking the amount ${amount} ada is : ${estimatedFees}`);
      logger.debug({ estimatedFees });
    } catch (e) {
        logger.debug({ e });
      throw new Error('Insufficient balance..');
    }

    // receiver address
    const addresses = [new AddressWallet(ForceBridgeCore.config.ada.lockAddress)];
    const amounts = [amount];

    const transaction: any = await wallet.sendPayment(passphrase, addresses, amounts);
    logger.debug(`user lock ${amount} ada; transactions details are ${transaction}`);

    //need to create a record with cardano transaction status
    const conn = await createConnection();
    const adaDb = new AdaDb(conn);
    const data = await adaDb.createAdaLock([
      {
        txId: transaction.id,
        sender: id,
        amount: (amount * this.config.ada.multiplier).toString(),
        data: data_ada,
        status: 'pending',
        bridgeFee: this.config.bridgeFee.out,
        recipient: '',
        direction: 'in'
      },
    ]);
    console.log({ data });
    await conn.close();
    return transaction.id;
  }



  /**
   * Unlocking the asset on ada, and sending back to the client
   * @param records
   */
  async sendUnlockTxs(records: AdaUnlock[]): Promise<AdaUnlockResult> {
    if (records.length === 0) {
      throw new Error('the unlock records should not be null');
    }
    if (records.length > 2) {
      throw new Error('the limit of op_return output size is 80 bytes which can contain 2 ckb tx hash (32*2 bytes)');
    }
    logger.debug('database records which need exec unlock:', records);
    //fetch balance from locked wallet
    const wallet = await walletServer.getShelleyWallet(ForceBridgeCore.config.ada.wallet.public_key);
    const balance = await wallet.getAvailableBalance();
    logger.debug(`collect live balance: ${JSON.stringify(balance, null, 2)}`);

    //need to fetch record from database, burnt the balance on CKB and release the token on ada chain.
    const accounts = [];
    const amounts = [];
    records.map((r) => {
      accounts.push(new AddressWallet(r.recipientAddress));
      amounts.push(r.amount);
    });

    try {
      // receiver address
      //checking if the amount is good to proceed with or not
      const estimatedFees = await wallet.estimateFee(accounts, amounts);
      logger.debug(`Transaction fee for Unlocking the transaction is : ${estimatedFees}`);
      logger.debug({ estimatedFees });

      const transaction: any = await wallet.sendPayment(
        ForceBridgeCore.config.ada.wallet.passphrase,
        accounts,
        amounts,
      );
      logger.debug(`user Unlock ada; transactions details are ${transaction}`);
      return transaction.id;
    } catch (e) {
      throw new Error('Insufficient balance..');
    }
  }
}