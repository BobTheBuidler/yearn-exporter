from collections import defaultdict
from pprint import pprint
from sys import builtin_module_names
from typing import Dict, List, Set

from brownie.exceptions import CompilerError
from pandas import DataFrame
from pony.orm import db_session, select
from pony.orm.core import QueryResult
from tqdm import tqdm
from yearn.cache import memory
from yearn.entities import UserTx
from yearn.outputs.postgres.utils import last_recorded_block
from yearn.utils import contract, is_contract
from yearn.v2.registry import Registry

ZAP_CONTRACT_BUILD_NAMES = [
    "yVault_ZapInOut_General_V1_5",
    "yVault_ZapIn_V2",
    "yVault_ZapIn_V2_0_1",
    "yVault_ZapIn_V3",
    "yVault_ZapIn_V4",
    "yVault_ZapIn_V5",
    "yVault_ZapIn_V5_1",
]


@db_session
def main():
    # Initialize registry
    v2 = Registry()

    # Decide which vaults to include in this analysis
    included_vaults = v2.vaults + v2.experiments
    included_vault_addresses = [vault.vault.address for vault in included_vaults]

    # Fetch depositors from db
    depositors = fetch_depositor_addresses(included_vault_addresses)
    
    # Decide which wallets to include in this analysis
    strategies_for_included_vaults = [
        strategy.strategy.address
        for vault in included_vaults
        for strategy in vault.strategies + vault.revoked_strategies
    ]

    contract_depositors = {
        address
        for address
        in tqdm(depositors)
        if is_contract(address)
    }
    usable_contract_depositors = {
        address
        for address
        in tqdm(contract_depositors)
        if contract_is_verified_and_has_no_errors(address)
    }
    contract_depositor_build_names = {
        address: contract(address)._build['contractName']
        for address in tqdm(usable_contract_depositors)
    }
    migrators = [
        address
        for address, build_name
        in contract_depositor_build_names.items()
        if build_name == 'TrustedVaultMigrator'
    ]

    zap_contracts = [
        address
        for address, build_name
        in contract_depositor_build_names.items()
        if build_name in ZAP_CONTRACT_BUILD_NAMES
    ]

    excluded_addresses = set(strategies_for_included_vaults + migrators + zap_contracts)
    included_addresses = depositors - excluded_addresses

    included_contract_depositor_build_names = {
        address: build_name
        for address, build_name
        in contract_depositor_build_names.items()
        if address not in excluded_addresses
    }

    # Fetch transfers from db
    transfers = fetch_transfers(included_vault_addresses, included_addresses)

    # Print stats
    pprint(calc_stats(depositors, included_addresses, transfers), sort_dicts=False)

    # Export a csv with all transfers included in this analysis
    df = create_dataframe_from_transfers(transfers)
    print(df)
    df.to_csv('vault_transfers.csv')

    # Print popular non-migrator contract depositors
    print_popular_non_migrator_contract_depositors(included_contract_depositor_build_names)


def fetch_depositor_addresses(included_vault_addresses: List[str]) -> Set[str]:
    return {
        address
        for address
        in select(
            tx.to_address
            for tx
            in UserTx
            if tx.type == 'deposit'
            and tx.vault.address.address in included_vault_addresses
        ).distinct()
    }


@memory.cache()
def contract_is_verified_and_has_no_errors(address: str) -> bool:
    try:
        contract(address)
    except CompilerError:
        return False
    except ValueError as e:
        if "Contract source code not verified" in str(e):
            return False
        else:
            raise
    return True


def print_popular_non_migrator_contract_depositors(build_names: Dict[str,str]) -> None:
    print('Common build names for contract depositors by number of instances:')
    stats = defaultdict(int)
    for build_name in build_names.values():
        stats[build_name] += 1
        
    popular_non_migrator_contract_depositors = defaultdict(set)
    for build_name, ct in stats.items():
        popular_non_migrator_contract_depositors[ct].add(build_name)
    
    popular_non_migrator_contract_depositors = {
        ct: popular_non_migrator_contract_depositors[ct]
        for ct in sorted(stats.values(), reverse=True)
    }

    pprint(popular_non_migrator_contract_depositors, sort_dicts=False)


def fetch_transfers(included_vault_addresses: List[str], included_addresses: List[str]) -> QueryResult:
    """ Fetch relevant transfers from db """
    return select(
        tx
        for tx
        in UserTx
        if tx.type == 'transfer'
        and tx.vault.address.address in included_vault_addresses
        and tx.to_address in included_addresses
        and tx.from_address in included_addresses
    ).fetch()


def calc_stats(depositors: List[str], included_addresses: List[str], transfers: QueryResult) -> Dict[str,int]:
    ''' Return a dict with all stats included in this analysis '''

    transferoors = {tx.from_address for tx in transfers}
    transfer_recipients = {tx.to_address for tx in transfers}

    return {
        'data current thru block': last_recorded_block(UserTx),
        'depositors': len(depositors),
        'excluded depositors': len(depositors) - len(included_addresses),
        'included depositors': len(included_addresses),
        'included contract depositoors': len([address for address in included_addresses if is_contract(address)]),
        'included non contract depositors': len([address for address in included_addresses if not is_contract(address)]),
        'transferoors': len(transferoors),
        'contract transferoors': len([address for address in transferoors if is_contract(address)]),
        'non contract transferoors': len([address for address in transferoors if not is_contract(address)]),
        'transferoor destinations': len(transfer_recipients),
        'transferoor destinations contracts': len([address for address in transfer_recipients if is_contract(address)]),
        'transferoor destinations non contracts': len([address for address in transfer_recipients if not is_contract(address)]),
    }


def create_dataframe_from_transfers(transfers: QueryResult) -> DataFrame:
    ''' Create a DataFrame with all transfers included in this analysis '''

    # Turn transfers into list of dicts that can be used by pandas
    return DataFrame(
        [
            {
                'chainid': tx.vault.address.chainid,
                'timestamp': tx.timestamp,
                'block': tx.block,
                'hash': tx.hash,
                'log_index': tx.log_index,
                'vault': tx.vault.symbol,
                'vault_address': tx.vault.address.address,
                'type': tx.type,
                'from': tx.from_address,
                'to': tx.to_address,
                'amount': tx.amount,
                'price': tx.price,
                'value_usd': tx.value_usd,
                'gas_used': tx.gas_used,
                'gas_price': tx.gas_price,
            } for tx in tqdm(transfers)
        ]
    )
