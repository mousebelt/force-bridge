import { Column, CreateDateColumn, Entity, Index, PrimaryColumn, UpdateDateColumn } from 'typeorm';


export type AdaTxConfirmStatus = 'pending' | 'submitted' | 'in_ledger' | 'expired';

@Entity()
export class AdaLock {
  @PrimaryColumn()
  txid: string;

  @Index()
  @Column()
  sender: string;

  @Column()
  amount: string;
  
  @Column({ default: '0' })
  bridgeFee: string;
  
  @Column('varchar', { length: 10240 })
  recipient: string;

  @Column('varchar', { length: 10240, default: '' })
  sudtExtraData: string;

  @Column('varchar', { length: 10240 })
  data: string;

  @Column()
  direction: string;

  @Column({default: 'pending'})
  status: AdaTxConfirmStatus;

  @Column({ default: 0 })
  confirmNumber: number;

  @CreateDateColumn()
  createdAt: string;

  @UpdateDateColumn()
  updatedAt: string;
}
