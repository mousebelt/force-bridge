import { Entity, Column, PrimaryColumn, CreateDateColumn, UpdateDateColumn } from 'typeorm';
import { dbTxStatus } from './CkbMint';

export type AdaUnlockStatus = dbTxStatus;

@Entity()
export class AdaUnlock {
  @PrimaryColumn()
  ckbTxHash: string;

  @Column()
  chain: number;

  @Column()
  asset: string;

  @Column()
  amount: string;

  @Column()
  recipientAddress: string;

  @Column({ nullable: true })
  adaTxId: string;

  @Column({ default: 'pending' })
  status: AdaUnlockStatus;

  @Column({ type: 'text', nullable: true })
  message: string;

  @CreateDateColumn()
  createdAt: string;

  @UpdateDateColumn()
  updatedAt: string;
}


@Entity()
export class CollectorAdaUnlock extends AdaUnlock {
  @Column({ default: 'pending' })
  status: AdaUnlockStatus;

  @Column({ type: 'text', nullable: true })
  message: string;
}