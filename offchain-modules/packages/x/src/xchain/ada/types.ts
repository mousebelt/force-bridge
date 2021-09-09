export type AdaLockData = {
    txId: string;
    sender: string;
    amount: number;
    data: string;
    status: string;
  };
  export type AdaUnlockResult = {
    txId: string;
  };
  
  export type AdaTx = {
    txId: string;
  };
  
  export type IInput = {
    address: string;
    amount: IAmount;
    assets: Array<any>;
    id: string;
    index: number;
  };
  
  export type IOutput = {
    address: string;
    amount: IAmount;
    assets: Array<any>;
  };
  
  export type IAmount = {
    quantity: number;
    unit: string;
  };
  