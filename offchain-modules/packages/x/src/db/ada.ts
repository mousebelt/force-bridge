// invoke in eth handler
import { Connection, In, Repository, UpdateResult } from 'typeorm';
import { ForceBridgeCore } from '../core';
import { CollectorCkbMint } from './entity/CkbMint';
import { CollectorAdaUnlock, AdaUnlockStatus } from './entity/AdaUnlock';
import {
  CkbBurn,
  CkbMint,
  AdaLock,
  AdaUnlock,
  ICkbMint,
  IAdaLock,
  IAdaUnlock,
  IQuery,
  LockRecord,
  AdaTxConfirmStatus,
  UnlockRecord,
} from './model';

export class AdaDb implements IQuery {
  private ckbMintRepository: Repository<CkbMint>;
  private adaLockRepository: Repository<AdaLock>;
  private AdaUnlockRepository: Repository<AdaUnlock>;
  private collectorAdaUnlockRepository: Repository<CollectorAdaUnlock>;
  private collectorCkbMintRepository: Repository<CollectorCkbMint>;

  constructor(private connection: Connection) {
    this.ckbMintRepository = connection.getRepository(CkbMint);
    this.adaLockRepository = connection.getRepository(AdaLock);
    this.AdaUnlockRepository = connection.getRepository(AdaUnlock);
    this.collectorAdaUnlockRepository = connection.getRepository(CollectorAdaUnlock);
    this.collectorCkbMintRepository = connection.getRepository(CollectorCkbMint);
  }


  async createCollectorCkbMint(records: ICkbMint[]): Promise<void> {
    const dbRecords = records.map((r) => this.collectorCkbMintRepository.create(r));
    await this.collectorCkbMintRepository.save(dbRecords);
  }

  async createAdaUnlock(records: IAdaUnlock[]): Promise<void> {
    const AdaUnlockRepo = this.connection.getRepository(AdaUnlock);
    const dbRecords = records.map((r) => AdaUnlockRepo.create(r));
    await AdaUnlockRepo.save(dbRecords);
  }

  async saveCollectorAdaUnlock(records: IAdaUnlock[]): Promise<void> {
    await this.collectorAdaUnlockRepository.save(records.map((r) => this.collectorAdaUnlockRepository.create(r)));
  }

  async createAdaLock(records: IAdaLock[]): Promise<void> {
    const dbRecords = records.map((r) => this.adaLockRepository.create(r));
    await this.adaLockRepository.save(dbRecords);
  }

  async updateCollectorUnlockStatus(ckbTxHash: string,  status: AdaUnlockStatus): Promise<void> {
    await this.connection
      .getRepository(CollectorAdaUnlock)
      .createQueryBuilder()
      .update()
      .set({status: status })
      .where({ ckbTxHash })
      .execute();
  }

  async updateLockConfirmNumber(
    records: { txnId: string; confirmedNumber: number; confirmStatus: AdaTxConfirmStatus }[],
  ): Promise<UpdateResult[]> {
    const updateResults = new Array(0);
    for (const record of records) {
      const result = await this.adaLockRepository
        .createQueryBuilder()
        .update()
        .set({ confirmNumber: record.confirmedNumber, confirmStatus: record.confirmStatus })
        .where('txid = :txid', { txid: record.txnId })
        .execute();
        updateResults.push(result);
    }
    return updateResults;
  }

  async updateBridgeInRecord(
    uniqueId: string,
    amount: string,
    token: string,
    recipient: string,
    sudtExtraData: string,
  ): Promise<void> {
    const mintRecord = await this.connection
      .getRepository(CkbMint)
      .createQueryBuilder()
      .select()
      .where({ id: uniqueId })
      .getOne();
    if (mintRecord) {
      const bridgeFee = (BigInt(amount) - BigInt(mintRecord.amount)).toString();
      await this.updateLockBridgeFee(uniqueId, bridgeFee);
      await this.connection
        .getRepository(CkbMint)
        .createQueryBuilder()
        .update()
        .set({ asset: token, recipientLockscript: recipient, sudtExtraData: sudtExtraData })
        .where({ id: uniqueId })
        .execute();
    }
  }

  async updateLockBridgeFee(txid: string, bridgeFee: string): Promise<void> {
    await this.adaLockRepository
      .createQueryBuilder()
      .update()
      .set({ bridgeFee: bridgeFee })
      .where({ txid: txid })
      .execute();
  }

  async updateBurnBridgeFee(burnTxHash: string, unlockAmount: string): Promise<void> {
    const query = await this.connection.getRepository(CkbBurn).createQueryBuilder();
    const row = await query.select().where('ckb_tx_hash = :ckbTxHash', { ckbTxHash: burnTxHash }).getOne();
    if (row) {
      const bridgeFee = (BigInt(row.amount) - BigInt(unlockAmount)).toString();
      await query
        .update()
        .set({ bridgeFee: bridgeFee })
        .where('ckb_tx_hash = :ckbTxHash', { ckbTxHash: burnTxHash })
        .execute();
    }
  }

  async getAdaUnlockRecordsToUnlock(status: AdaUnlockStatus, take = 10): Promise<AdaUnlock[]> {
    return await this.collectorAdaUnlockRepository.find({
      where: {
        status,
      },
      take,
    });
  }

  async getLockRecordsByCkbAddress(ckbRecipientAddr: string): Promise<LockRecord[]> {
    return await this.adaLockRepository
      .createQueryBuilder('ada')
      .leftJoinAndSelect('ckb_mint', 'ckb', 'ada.txid = ckb.id')
      .where('ada.recipient = :recipient', {
        recipient: ckbRecipientAddr
      })
      .select(
        `
        ada.sender as sender, 
        ada.recipient as recipient, 
        ada.amount as lock_amount,
        ada.amount as mint_amount,
        ada.txid as lock_id,
        ckb.mint_hash as mint_hash,
        ada.updated_at as lock_time, 
        ada.confirm_number as lock_confirm_number,
        ada.status as lock_confirm_status,
        ckb.updated_at as mint_time, 
        case when isnull(ckb.amount) then null else 'success' end as status,
        '' as message,
        ada.bridge_fee as bridge_fee
      `,
      )
      .orderBy('ckb.updated_at', 'DESC')
      .getRawMany();
  }

  async getUnlockRecordsByCkbAddress(ckbAddress: string, XChainAsset: string): Promise<UnlockRecord[]> {
    return await this.connection
      .getRepository(CkbBurn)
      .createQueryBuilder('ckb')
      .leftJoinAndSelect('ada_unlock', 'ada', 'ada.ckb_tx_hash = ckb.ckb_tx_hash')
      .where('ckb.sender_address = :sender_address AND ckb.asset = :asset', {
        sender_address: ckbAddress,
        asset: XChainAsset,
      })
      .select(
        `
        ckb.sender_address as sender, 
        ckb.recipient_address as recipient , 
        ckb.amount as burn_amount, 
        ada.amount as unlock_amount,
        ckb.ckb_tx_hash as burn_hash,
        ada.ada_tx_id as unlock_tx_id,
        ada.updated_at as unlock_time, 
        ckb.updated_at as burn_time, 
        ckb.confirm_number as burn_confirm_number,
        ckb.confirm_status as burn_confirm_status,
        ckb.asset as asset,
        case when isnull(ada.amount) then null else 'success' end as status,
        '' as message,
        ckb.bridge_fee as bridge_fee
      `,
      )
      .orderBy('ckb.updated_at', 'DESC')
      .getRawMany();
  }

  async getLockRecordsByXChainAddress(XChainSender: string): Promise<LockRecord[]> {
    return await this.adaLockRepository
      .createQueryBuilder('ada')
      .leftJoinAndSelect('ckb_mint', 'ckb', 'ada.id = ckb.id')
      .where('ada.sender = :sender', { sender: XChainSender })
      .select(
        `
        ada.sender as sender, 
        ada.recipient as recipient, 
        ada.amount as lock_amount,
        ckb.amount as mint_amount,
        ada.txid as lock_id,
        ckb.mint_hash as mint_hash,
        ada.updated_at as lock_time, 
        ada.confirm_number as lock_confirm_number,
        ada.status as lock_confirm_status,
        ckb.updated_at as mint_time, 
        case when isnull(ckb.amount) then null else 'success' end as status,
        '' as message,
        ada.bridge_fee as bridge_fee
      `,
      )
      .orderBy('ckb.updated_at', 'DESC')
      .getRawMany();
  }

  async getUnlockRecordsByXChainAddress(XChainRecipientAddr: string): Promise<UnlockRecord[]> {
    return await this.connection
      .getRepository(CkbBurn)
      .createQueryBuilder('ckb')
      .leftJoinAndSelect('ada_unlock', 'ada', 'ada.ckb_tx_hash = ckb.ckb_tx_hash')
      .where('ckb.recipient_address = :recipient_address', {
        recipient_address: XChainRecipientAddr
      })
      .select(
        `
        ckb.sender_address as sender, 
        ckb.recipient_address as recipient , 
        ckb.amount as burn_amount, 
        ada.amount as unlock_amount,
        ckb.ckb_tx_hash as burn_hash,
        ada.ada_tx_id as unlock_tx_id,
        ada.updated_at as unlock_time, 
        ckb.updated_at as burn_time, 
        ckb.confirm_number as burn_confirm_number,
        ckb.confirm_status as burn_confirm_status,
        ckb.asset as asset,
        case when isnull(ada.amount) then null else 'success' end as status,
        '' as message,
        ckb.bridge_fee as bridge_fee
      `,
      )
      .orderBy('ckb.updated_at', 'DESC')
      .getRawMany();
  }

  async getAdaLocksByUniqueIds(uniqueIds: string[]): Promise<AdaLock[]> {
    return await this.connection.getRepository(AdaLock).find({
      where: {
        txid: In(uniqueIds),
      },
    });
  }

  async getAdaUnlockByCkbTxHashes(ckbTxHashes: string[]): Promise<AdaUnlock[]> {
    return await this.connection.getRepository(AdaUnlock).find({
      where: {
        ckbTxHash: In(ckbTxHashes),
      },
    });
  }

  async setCollectorAdaUnlockToSuccess(ckbTxHashes: string[]): Promise<void> {
    await this.connection
      .getRepository(CollectorAdaUnlock)
      .createQueryBuilder()
      .update()
      .set({ status: 'success' })
      .where({ ckbTxHash: In(ckbTxHashes) })
      .execute();
  }
}
