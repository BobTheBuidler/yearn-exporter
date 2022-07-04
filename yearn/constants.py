from brownie import chain
from yearn.networks import Network

WRAPPED_GAS_COIN = {
    Network.Mainnet:            "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    Network.Fantom:             "0x21be370D5312f44cB42ce377BC9b8a0cEF1A4C83",
    Network.Arbitrum:           "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
    Network.Gnosis:             "0xe91D153E0b41518A2Ce8Dd3D7944Fa863463a97d",
}.get(chain.id, None)


BTC_LIKE = {
    "0xEB4C2781e4ebA804CE9a9803C67d0893436bB27D", # renbtc
    "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599", # wbtc
    "0xfE18be6b3Bd88A2D2A7f928d00292E7a9963CfC6", # sbtc
    "0x8064d9Ae6cDf087b1bcd5BDf3531bD5d8C537a68", # obtc
    "0x9BE89D2a4cd102D8Fecc6BF9dA793be995C22541", # bbtc
    "0x0316EB71485b0Ab14103307bf65a021042c6d380", # hbtc
    "0x5228a22e72ccC52d415EcFd199F99D0665E7733b", # pbtc
    "0x8dAEBADE922dF735c38C80C7eBD708Af50815fAa", # tbtc
}

ETH_LIKE = {
    "0x5e74C9036fb86BD7eCdcb084a0673EFc32eA31cb", # seth
    "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE", # eth
    "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84", # steth
    "0x9559Aaa82d9649C7A7b220E7c461d2E74c9a3593", # reth
    "0xE95A203B1a91a908F9B9CE46459d101078c2c3cb", # ankreth
}

YEARN_ADDRESSES_PROVIDER = "0x9be19Ee7Bc4099D62737a7255f5c227fBcd6dB93"
CURVE_ADDRESSES_PROVIDER = "0x0000000022D53366457F9d5E68Ec105046FC4383"

# EVENTS
ERC20_TRANSFER_EVENT_HASH  = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
ERC677_TRANSFER_EVENT_HASH = '0xe19260aff97b920c7df27010903aeb9c8d2be5d310a2c67824cf3f15396e4c16'

# ADDRESSES
STRATEGIST_MULTISIG = {
    Network.Mainnet: {
        "0x16388463d60FFE0661Cf7F1f31a7D658aC790ff7",
    },
    Network.Fantom: {
        "0x72a34AbafAB09b15E7191822A679f28E067C4a16",
    },
    Network.Gnosis: {
        "0xFB4464a18d18f3FF439680BBbCE659dB2806A187"
    }
}.get(chain.id,set())

YCHAD_MULTISIG = {
    Network.Mainnet:    "0xFEB4acf3df3cDEA7399794D0869ef76A6EfAff52",
    Network.Fantom:     "0xC0E2830724C946a6748dDFE09753613cd38f6767",
    Network.Gnosis:     "0x22eAe41c7Da367b9a15e942EB6227DF849Bb498C",
}.get(chain.id, None)

TREASURY_MULTISIG = {
    Network.Mainnet:    "0x93A62dA5a14C80f265DAbC077fCEE437B1a0Efde",
    Network.Fantom:     "0x89716Ad7EDC3be3B35695789C475F3e7A3Deb12a",
}.get(chain.id, None)

TREASURY_WALLETS = {
    Network.Mainnet: {
        TREASURY_MULTISIG,
        YCHAD_MULTISIG,
        "0xb99a40fcE04cb740EB79fC04976CA15aF69AaaaE", # Yearn Treasury V1  
        "0x5f0845101857d2A91627478e302357860b1598a1", # Yearn KP3R Wallet
        "0x7d2aB9CA511EBD6F03971Fb417d3492aA82513f0", # ySwap Multisig
        "0x2C01B4AD51a67E2d8F02208F54dF9aC4c0B778B6", # yMechs Multisig
    },
    Network.Fantom: {
        TREASURY_MULTISIG,
        YCHAD_MULTISIG,
    },
    Network.Gnosis: {
        YCHAD_MULTISIG,
        "0x5FcdC32DfC361a32e9d5AB9A384b890C62D0b8AC", # Yearn Treasury (EOA?)
    },
}.get(chain.id,set())
