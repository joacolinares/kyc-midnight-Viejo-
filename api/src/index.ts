// This file is part of midnightntwrk/example-counter.
// Copyright (C) 2025 Midnight Foundation
// SPDX-License-Identifier: Apache-2.0
// Licensed under the Apache License, Version 2.0 (the "License");
// You may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * Provides types and utilities for working with bulletin board contracts.
 *
 * @packageDocumentation
 */

import contractModule from '../../contract/src/managed/bboard/contract/index.cjs';
const { Contract, ledger, pureCircuits } = contractModule;
// import { Contract, ledger, pureCircuits, State } from '../../contract/src/index';

import { type ContractAddress, convert_bigint_to_Uint8Array } from '@midnight-ntwrk/compact-runtime';
import { type Logger } from 'pino';
import {
  type BBoardDerivedState,
  type BBoardContract,
  type BBoardProviders,
  type DeployedBBoardContract,
  bboardPrivateStateKey,
} from './common-types.js';
// import { Contract, ledger, pureCircuits, State } from '../../contract/src/managed/bboard/contract/index.cjs';
import { type BBoardPrivateState, createBBoardPrivateState, witnesses } from '../../contract/src/index';
import * as utils from './utils/index.js';
import { deployContract, findDeployedContract } from '@midnight-ntwrk/midnight-js-contracts';
import { combineLatest, map, tap, from, type Observable, empty } from 'rxjs';
import { toHex } from '@midnight-ntwrk/midnight-js-utils';
import { encodeCoinPublicKey, sampleCoinPublicKey } from '@midnight-ntwrk/ledger';




/** @internal */
const bboardContractInstance: BBoardContract = new Contract(witnesses);


function hexToBytes(hex: string): Uint8Array {
  const clean = hex.trim().toLowerCase().replace(/^0x/, "");
  if (!/^[0-9a-f]*$/.test(clean) || clean.length % 2 !== 0) {
    throw new Error("Hex inválido");
  }
  const out = new Uint8Array(clean.length / 2);
  for (let i = 0; i < out.length; i++) {
    out[i] = parseInt(clean.substr(i * 2, 2), 16);
  }
  return out;
}

// (opcional) exigir largo exacto
function expectLen(u8: Uint8Array, len: number, label = "bytes"): Uint8Array {
  if (u8.length !== len) throw new Error(`${label} debe tener ${len} bytes, tiene ${u8.length}`);
  return u8;
}
const hex = "8f24d209ca61d8b2ecf641583d63c0b072558a9653059e6e3b7586e42d4a31c3";
const bytes32 = expectLen(hexToBytes(hex), 32, "clave");
const country2 = new Uint8Array([..."AR"].map(c => c.charCodeAt(0)));

/**
 * An API for a deployed bulletin board.
 */
export interface DeployedBBoardAPI {
  readonly deployedContractAddress: ContractAddress;
  readonly state$: Observable<BBoardDerivedState>;

  bumpEpoch(): Promise<void>;
  revokeUpk(uPk: Uint8Array): Promise<void>;
  setAdultFlag(uPk: Uint8Array, v: 0 | 1 | bigint): Promise<void>;
  setCountryFlag(uPk: Uint8Array, v: 0 | 1 | bigint): Promise<void>;
  setAllowMinAge(age: bigint): Promise<void>;
  setAllowCountry(country: Uint8Array): Promise<void>;

  // --- KYC lecturas ---
  getEpoch(): Promise<bigint>;
  getInstanceBytes(): Promise<Uint8Array>;
  getAllowedCountry(): Promise<Uint8Array>;

  // --- KYC usuario ---
  enrollOnce(): Promise<void>;
  checkAdultByUpk(uPk: Uint8Array): Promise<bigint>;
  checkAdultSelf(): Promise<bigint>;

  checkCountrySelf(): Promise<bigint>;
  checkElegibleSelf(): Promise<bigint>;
  checkElegibleSelf(): Promise<bigint>;
}

/**
 * Provides an implementation of {@link DeployedBBoardAPI} by adapting a deployed bulletin board
 * contract.
 *
 * @remarks
 * The `BBoardPrivateState` is managed at the DApp level by a private state provider. As such, this
 * private state is shared between all instances of {@link BBoardAPI}, and their underlying deployed
 * contracts. The private state defines a `'secretKey'` property that effectively identifies the current
 * user, and is used to determine if the current user is the owner of the message as the observable
 * contract state changes.
 *
 * In the future, Midnight.js will provide a private state provider that supports private state storage
 * keyed by contract address. This will remove the current workaround of sharing private state across
 * the deployed bulletin board contracts, and allows for a unique secret key to be generated for each bulletin
 * board that the user interacts with.
 */
// TODO: Update BBoardAPI to use contract level private state storage.
export class BBoardAPI implements DeployedBBoardAPI {
  
  private readonly providers: BBoardProviders; 
  /** @internal */
  private constructor(
    public readonly deployedContract: DeployedBBoardContract,
    providers: BBoardProviders,
    private readonly logger?: Logger,
  ) {
    this.providers = providers; // <—
    this.deployedContractAddress = deployedContract.deployTxData.public.contractAddress;
    this.state$ = combineLatest(
      [
        // Combine public (ledger) state with...
        providers.publicDataProvider.contractStateObservable(this.deployedContractAddress, { type: 'latest' }).pipe(
          map((contractState) => ledger(contractState.data)),
          tap((ledgerState) =>
            logger?.trace({
              ledgerStateChanged: {
                epoch: ledgerState.epoch,
                instance: ledgerState.instance,
                allowedCountryHex: ledgerState.allowedCountry,
                allowedMinAge: ledgerState.allowedMinAge,
              },
            }),
          ),
        ),
        // ...private state...
        //    since the private state of the bulletin board application never changes, we can query the
        //    private state once and always use the same value with `combineLatest`. In applications
        //    where the private state is expected to change, we would need to make this an `Observable`.
        from(providers.privateStateProvider.get(bboardPrivateStateKey) as Promise<BBoardPrivateState>),
      ],
      // ...and combine them to produce the required derived state.
      (ledgerState, privateState) => {
        const hashedSecretKey = pureCircuits.publicKey(
          privateState.secretKey,
          convert_bigint_to_Uint8Array(32, ledgerState.epoch),
        );
        const DEFAULT_MIN_AGE = "21";
        return {
          epoch: ledgerState.epoch as bigint,
          instance: ledgerState.instance as bigint,
          allowedCountryHex: ledgerState.allowedCountry as Uint8Array,               // Uint8Array -> "0x…"
          allowedMinAge: BigInt(ledgerState.allowedMinAge ?? DEFAULT_MIN_AGE) // bigint|undefined -> string
        };
      },
    );
  }

  /**
   * Gets the address of the current deployed contract.
   */
  readonly deployedContractAddress: ContractAddress;

  /**
   * Gets an observable stream of state changes based on the current public (ledger),
   * and private state data.
   */
  readonly state$: Observable<BBoardDerivedState>;

  /**
   * Attempts to post a given message to the bulletin board.
   *
   * @param message The message to post.
   *
   * @remarks
   * This method can fail during local circuit execution if the bulletin board is currently occupied.
   */
  async post(age: Uint8Array, country: Uint8Array): Promise<void> {
    const txData = await this.deployedContract.callTx.enrollOnce();

    this.logger?.trace({
      transactionAdded: {
        circuit: 'enrollOnce',
        txHash: txData.public.txHash,
        blockHeight: txData.public.blockHeight,
      },
    });
  }


  private async getLedgerSnapshot() {
    const { publicDataProvider } = this.providers;
    const cs = await publicDataProvider.queryContractState(this.deployedContractAddress);
    if (!cs) throw new Error('No on-chain state found for this contract');
    return ledger(cs.data);
  }


  // Helper 0/1 → bigint
  private to01(v: 0 | 1 | number | bigint): bigint {
    const n = typeof v === 'bigint' ? v : BigInt(v);
    if (n !== 0n && n !== 1n) throw new Error('value must be 0 or 1');
    return n;
  }

  // --- KYC admin ---
  async bumpEpoch(): Promise<void> {
    const tx = await this.deployedContract.callTx.bumpEpoch();
    this.logger?.trace({ transactionAdded: { circuit: 'bumpEpoch', txHash: tx.public.txHash, blockHeight: tx.public.blockHeight } });
  }

  async revokeUpk(uPk: Uint8Array): Promise<void> {
    const tx = await (this.deployedContract.callTx as any).revokeUpk?.(uPk);
    if (!tx) throw new Error('revokeUpk circuit not available in this artifact');
    this.logger?.trace({ transactionAdded: { circuit: 'revokeUpk', txHash: tx.public.txHash, blockHeight: tx.public.blockHeight } });
  }

  async setAdultFlag(uPk: Uint8Array, v: 0 | 1 | bigint): Promise<void> {
    const vv = this.to01(v);
    const tx = await (this.deployedContract.callTx as any).setAdultFlag?.(uPk, vv);
    if (!tx) throw new Error('setAdultFlag circuit not available in this artifact');
    this.logger?.trace({ transactionAdded: { circuit: 'setAdultFlag', txHash: tx.public.txHash, blockHeight: tx.public.blockHeight } });
  }

  async setCountryFlag(uPk: Uint8Array, v: 0 | 1 | bigint): Promise<void> {
    const vv = this.to01(v);
    const tx = await (this.deployedContract.callTx as any).setCountryFlag?.(uPk, vv);
    if (!tx) throw new Error('setCountryFlag circuit not available in this artifact');
    this.logger?.trace({ transactionAdded: { circuit: 'setCountryFlag', txHash: tx.public.txHash, blockHeight: tx.public.blockHeight } });
  }

  async setAllowMinAge(age: bigint): Promise<void> {
    const tx = await (this.deployedContract.callTx as any).setAllowedMinAge?.(age);
    if (!tx) throw new Error('setAllowedMinAge circuit not available in this artifact');
    this.logger?.trace({ transactionAdded: { circuit: 'setAllowedMinAge', txHash: tx.public.txHash, blockHeight: tx.public.blockHeight } });
  }

  async setAllowCountry(country: Uint8Array): Promise<void> {
    const tx = await (this.deployedContract.callTx as any).setAllowedCountry?.(country);
    if (!tx) throw new Error('setAllowedCountry circuit not available in this artifact');
    this.logger?.trace({ transactionAdded: { circuit: 'setAllowedCountry', txHash: tx.public.txHash, blockHeight: tx.public.blockHeight } });
  }

  // --- KYC lecturas ---
  async getEpoch(): Promise<bigint> {
    const ls = await this.getLedgerSnapshot();
    return ls.epoch as bigint;
  }

  async getInstanceBytes(): Promise<Uint8Array> {
    const ls = await this.getLedgerSnapshot();
    return convert_bigint_to_Uint8Array(32, ls.instance);
  }

  async getAllowedCountry(): Promise<Uint8Array> {
    const ls = await this.getLedgerSnapshot();
    return ls.allowedCountry;
  }

  // --- KYC usuario ---
  // Si tu circuito enrollOnce NO recibe params (usa witnesses), llamá sin args.
  // Si tu circuito SÍ recibe (age, country) como Bytes, dejamos ambos caminos:
  async enrollOnce(): Promise<void> {
    const callTxAny = this.deployedContract.callTx as any;
    let tx =
      (await callTxAny.enrollOnce?.()) ??
      (await callTxAny.enrollOnce?.()); // fallback si la firma no lleva args
    if (!tx) throw new Error('enrollOnce circuit not available in this artifact');
    this.logger?.trace({ transactionAdded: { circuit: 'enrollOnce', txHash: tx.public.txHash, blockHeight: tx.public.blockHeight } });
  }

  async checkAdultByUpk(uPk: Uint8Array): Promise<bigint> {
    const ls = await this.getLedgerSnapshot();
    if (!ls.attest.member(uPk)) throw new Error('no record for uPk');
    return ls.attest.lookup(uPk).adult;
  }

  async checkAdultSelf(): Promise<bigint> {
    const ls = await this.getLedgerSnapshot();
    // Derivar tu uPk si ya tenés helper; si no, simplemente lanza si no existe
    // (Ajustá a tu lógica de deriveMyUpk() si la tenés en esta clase)
    for (const [uPk, att] of ls.attest) {
      // Devuelve el primero (ejemplo); en tu código real usá deriveMyUpk()
      return att.adult;
    }
    throw new Error('no self attestation found');
  }

  async checkCountrySelf(): Promise<bigint> {
    const ls = await this.getLedgerSnapshot();
    for (const [uPk, att] of ls.attest) {
      return BigInt(Number(att.inCountry));
    }
    throw new Error('no self attestation found');
  }

  async checkElegibleSelf(): Promise<bigint> {
    const ls = await this.getLedgerSnapshot();
    for (const [uPk, a] of ls.attest) {
      const ok = (a.adult === (1 as any) && a.inCountry === (1 as any)) ? 1n : 0n;
      return ok;
    }
    return 0n;
  }










  /**
   * Attempts to take down any currently posted message on the bulletin board.
   *
   * @remarks
   * This method can fail during local circuit execution if the bulletin board is currently vacant,
   * or if the currently posted message isn't owned by the owner computed from the current private
   * state.
   */
  // async takeDown(): Promise<void> {
  //   this.logger?.info('takingDownMessage');

  //   const txData = await this.deployedContract.callTx.takeDown();

  //   this.logger?.trace({
  //     transactionAdded: {
  //       circuit: 'takeDown',
  //       txHash: txData.public.txHash,
  //       blockHeight: txData.public.blockHeight,
  //     },
  //   });
  // }

  /**
   * Deploys a new bulletin board contract to the network.
   *
   * @param providers The bulletin board providers.
   * @param logger An optional 'pino' logger to use for logging.
   * @returns A `Promise` that resolves with a {@link BBoardAPI} instance that manages the newly deployed
   * {@link DeployedBBoardContract}; or rejects with a deployment error.
   */
  static async deploy(providers: BBoardProviders, logger?: Logger): Promise<BBoardAPI> {
    logger?.info('deployContract');


    // const ownerPkHex =
    // "6e69671bbc5746b77ab9e560472b089b97bf5d95f2adce05c52c10468873c8a1";
  
    // function hexToBytes32(hex: string): Uint8Array {
    //   const clean = hex.startsWith("0x") ? hex.slice(2) : hex;
    //   if (clean.length !== 64) throw new Error("Se esperan 32 bytes (64 hex chars)");
    //   const out = new Uint8Array(32);
    //   for (let i = 0; i < 32; i++) {
    //     out[i] = parseInt(clean.slice(2*i, 2*i + 2), 16);
    //   }
    //   return out;
    // }
    // const initialOwner: { is_left: boolean; left: { bytes: Uint8Array }; right: { bytes: Uint8Array } } = {
    //   // Elegís una de las dos ramas del Either.
    //   // Aquí lo dejo como 'left' (ZswapCoinPublicKey); reemplazá los bytes por tu PK real.
    //   is_left: true,
    //   left:  { bytes: hexToBytes32(ownerPkHex) },     // <- tus 32 bytes
    //   right: { bytes: new Uint8Array(32) }, // no se usa si is_left=true, pero el tipo lo pide
    // };


    // EXERCISE 5: FILL IN THE CORRECT ARGUMENTS TO deployContract



  // helper
function hexToBytes32(hex: string): Uint8Array {
  const clean = hex.startsWith("0x") ? hex.slice(2) : hex;
  if (clean.length !== 64) throw new Error("Se esperan 32 bytes (64 hex chars)");
  const out = new Uint8Array(32);
  for (let i = 0; i < 32; i++) out[i] = parseInt(clean.slice(2*i, 2*i + 2), 16);
  return out;
}

// tu pk (32 bytes en hex)
const ownerPkHex =
  "1f2b36b9a0a392eca711a902930bcde677b6f0cd00a305321adbecef65b6e557";

// ESTE es el objeto que tu artefacto acepta:
const initialOwner = {
  is_left: true,                            // dueño = ZswapCoinPublicKey (LEFT)
  left:  { bytes: hexToBytes32(ownerPkHex) },
  right: { bytes: new Uint8Array(32) },     // 32 bytes en cero (no usado)
} as const;

// To represent a left value (number)
const leftValue = {
  is_left: true,
  left: {bytes: encodeCoinPublicKey(sampleCoinPublicKey())},
  right: {bytes: new Uint8Array} // default for string
};

// To represent a right value (string)
const rightValue = {
  isLeft: false,
  left: 0,    // default for number
  right: "hello"
};



const deployedBBoardContract = await deployContract<typeof bboardContractInstance>(providers, {
  privateStateId: bboardPrivateStateKey,
  contract: bboardContractInstance,
  initialPrivateState: await BBoardAPI.getPrivateState(providers),
  args: [leftValue], // sin encodeCoinPublicKey
});


    // const dummyPubKeyHex = "1111111111111111111111111111111111111111111111111111111111111111";
    // const dummyPubKey = Uint8Array.from(Buffer.from(dummyPubKeyHex, "hex"));
    
    // const initialOwner = {
    //   is_left: true,
    //   left: { bytes: dummyPubKey },
    //   right: { bytes: new Uint8Array(32) }, // placeholder vacío
    // };
    
    // const deployedBBoardContract = await deployContract<typeof bboardContractInstance>(providers, {
    //   privateStateId: bboardPrivateStateKey,
    //   contract: bboardContractInstance,
    //   initialPrivateState: await BBoardAPI.getPrivateState(providers),
    //   args: [initialOwner],
    // });



    logger?.trace({
      contractDeployed: {
        finalizedDeployTxData: deployedBBoardContract.deployTxData.public,
      },
    });

    return new BBoardAPI(deployedBBoardContract, providers, logger);
  }

  /**
   * Finds an already deployed bulletin board contract on the network, and joins it.
   *
   * @param providers The bulletin board providers.
   * @param contractAddress The contract address of the deployed bulletin board contract to search for and join.
   * @param logger An optional 'pino' logger to use for logging.
   * @returns A `Promise` that resolves with a {@link BBoardAPI} instance that manages the joined
   * {@link DeployedBBoardContract}; or rejects with an error.
   */
  static async join(providers: BBoardProviders, contractAddress: ContractAddress, logger?: Logger): Promise<BBoardAPI> {
    logger?.info({
      joinContract: {
        contractAddress,
      },
    });

    const deployedBBoardContract = await findDeployedContract<BBoardContract>(providers, {
      contractAddress,
      contract: bboardContractInstance,
      privateStateId: bboardPrivateStateKey,
      initialPrivateState: await BBoardAPI.getPrivateState(providers),
    });

    logger?.trace({
      contractJoined: {
        finalizedDeployTxData: deployedBBoardContract.deployTxData.public,
      },
    });

    return new BBoardAPI(deployedBBoardContract, providers, logger);
  }

  private static async getPrivateState(providers: BBoardProviders): Promise<BBoardPrivateState> {
    const existingPrivateState = await providers.privateStateProvider.get(bboardPrivateStateKey);
    return existingPrivateState ?? createBBoardPrivateState(utils.randomBytes(32));
  }
}

/**
 * A namespace that represents the exports from the `'utils'` sub-package.
 *
 * @public
 */
export * as utils from './utils/index.js';

export * from './common-types.js';
