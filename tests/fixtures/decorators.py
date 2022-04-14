
import pytest
from brownie import chain
from yearn.networks import Network
from yearn.ironbank import addresses as ironbank_registries

mainnet_only = pytest.mark.skipif(
    chain.id != Network.Mainnet,
    reason="Only applicable on Mainnet."
)

ib_chains_only = pytest.mark.skipif(
    chain.id not in ironbank_registries,
    reason='Not applicable on chains without IronBank deployments.'
)
