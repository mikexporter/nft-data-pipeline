from web3 import Web3
import os
import requests
import json
import polars as pl
import numpy as np
from hexbytes import HexBytes

infura_api_key = os.getenv("INFURA_API_KEY")
infura_url = f"https://mainnet.infura.io/v3/{infura_api_key}"
etherscan_api_key = os.getenv("ETHERSCAN_API_KEY")

class HexJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, HexBytes):
            return obj.hex()
        return super().default(obj)

def get_parquet(file_path, schema):
    if os.path.isfile(file_path):
        df = pl.read_parquet(file_path)
    else:
        df = pl.DataFrame({}, schema=schema)
        df.write_parquet(file_path)
    return df
    
def get_abi(address, etherscan_api_key):
    response = requests.get(f"https://api.etherscan.io/api?module=contract&action=getabi&address={address}&apikey={etherscan_api_key}")
    response_json = json.loads(response.text)
    result = response_json['result']
    if result == 'Contract source code not verified':
        return []
    else:
        abi = json.loads(result)
        return abi

def is_erc721_compliant(abi):
    required_functions = {"balanceOf", "ownerOf", "approve", "getApproved", "setApprovalForAll", "isApprovedForAll", "transferFrom", "safeTransferFrom"}
    required_events = {"Transfer", "Approval"}

    # Check if all required functions exist in the ABI
    if all(any(func["name"] == required_func for func in abi if func["type"] == "function") for required_func in required_functions):
        # Check if all required events exist in the ABI
        if all(any(event["name"] == required_event for event in abi if event["type"] == "event") for required_event in required_events):
            return True

    return False

def ingest_erc721_from_block(block_number, rpc_endpoint, etherscan_api_key):

    blocks_ingested_path = "../data/raw/blocks_ingested.parquet"
    blocks_ingested_schema = [
        ("Block Number", pl.Int64)
    ]

    blocks_ingested_df = get_parquet(blocks_ingested_path, blocks_ingested_schema)

    # Retrieve the block information
    if not blocks_ingested_df.shape[0] == 0 and blocks_ingested_df['Block Number'].eq(block_number).any():
        pass
    else:
        # Create a Web3 instance
        web3 = Web3(Web3.HTTPProvider(rpc_endpoint))
        block = web3.eth.get_block(block_number,full_transactions=True)

        if block is not None:
            block_timestamp = block['timestamp']

            # Loop through each transaction in the block
            for tx in block['transactions']:

                # Check if the transaction involves a contract
                if tx['to'] is not None:
                    tx_hash = tx['hash'].hex()
                    contract_address = tx['to']

                    ignore_address_path = "../data/raw/ignore_address.parquet"
                    ignore_address_schema = [
                        ("Address", pl.Utf8)
                    ]
                    ignore_address_df = get_parquet(ignore_address_path, ignore_address_schema)

                    transactions_path = "../data/raw/transactions.parquet"
                    transactions_schema = [
                            ("Block Number", pl.Int64),
                            ("Block Timestamp", pl.Int64),
                            ("Transaction Hash", pl.Utf8),
                            ("Transaction Data", pl.Utf8)
                    ]
                    transactions_df = get_parquet(transactions_path, transactions_schema)
                    
                    if not ignore_address_df.shape[0] == 0 and ignore_address_df['Address'].eq(contract_address).any():
                        # Contract address is a private address
                        pass
                    elif not transactions_df.shape[0] == 0 and transactions_df['Transaction Hash'].eq(tx_hash).any():
                        # Contract address has already been checked
                        pass
                    elif web3.eth.get_code(contract_address) == '0x':
                        # Contract address is not a contract
                        ignore_address_df = pl.concat([ignore_address_df, pl.DataFrame({"Address": [contract_address]})])
                        ignore_address_df.write_parquet(ignore_address_path)
                    else:
                        # Contract address is an ERC721 contract
                        contracts_path = "../data/raw/contracts.parquet"
                        contracts_schema = [
                            ("Address", pl.Utf8),
                            ("ABI", pl.Utf8)
                        ]
                        contracts_df = get_parquet(contracts_path, contracts_schema)

                        # Attempt to check if it is an ERC721 contract
                        abi = get_abi(contract_address, etherscan_api_key)

                        if not is_erc721_compliant(abi):
                            # Contract is not ERC721 compliant
                            ignore_address_df = pl.concat([ignore_address_df, pl.DataFrame({"Address": [contract_address]})])
                            ignore_address_df.write_parquet(ignore_address_path)
                        else:
                            # Contract is ERC721 compliant
                            contracts_df = pl.concat([
                                contracts_df, 
                                pl.DataFrame({
                                    "Address": [contract_address], 
                                    "ABI": [json.dumps(abi)]
                                    })
                                ])
                            
                            contracts_df.write_parquet(contracts_path)
                            
                            tx_dict = dict(tx)
                            tx_json = json.dumps(tx_dict, cls=HexJsonEncoder)

                            transactions_df = pl.concat([
                                transactions_df, 
                                pl.DataFrame({
                                    "Block Number": [block_number], 
                                    "Block Timestamp": [block_timestamp], 
                                    "Transaction Hash": [tx_hash], 
                                    "Transaction Data": [tx_json]
                                    })
                                ])
                            
                            transactions_df.write_parquet(transactions_path)
                                
        blocks_ingested_df = pl.concat([blocks_ingested_df, pl.DataFrame({"Block Number": [block_number]})])
        blocks_ingested_df.write_parquet(blocks_ingested_path)

def ingest_erc721_from_blocks(start_block_number, end_block_number, rpc_endpoint, etherscan_api_key):
    for block_number in range(start_block_number, end_block_number + 1):
        ingest_erc721_from_block(block_number, rpc_endpoint, etherscan_api_key)

# Test the function with a block number
start_block_number = 17497810
end_block_number = 17497810
ingest_erc721_from_blocks(start_block_number, end_block_number, infura_url, etherscan_api_key)