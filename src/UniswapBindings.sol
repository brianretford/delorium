// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.26;

import {IPoolManager} from "@uniswap/v4-core/src/interfaces/IPoolManager.sol";
import {IStateView} from "v4-periphery/src/interfaces/IStateView.sol";
import {IV4Quoter} from "v4-periphery/src/interfaces/IV4Quoter.sol";

// This shim pulls the relevant Uniswap v4 interfaces into Foundry artifacts
// so `forge bind` can generate Rust bindings from official sources.
contract UniswapBindings {
    IPoolManager internal immutable poolManager;
    IStateView internal immutable stateView;
    IV4Quoter internal immutable quoter;

    constructor(IPoolManager _poolManager, IStateView _stateView, IV4Quoter _quoter) {
        poolManager = _poolManager;
        stateView = _stateView;
        quoter = _quoter;
    }
}
