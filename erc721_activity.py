from web3 import Web3
import os
import requests
import json
import polars as pl

infura_api_key = os.getenv("INFURA_API_KEY")
infura_url = f"https://mainnet.infura.io/v3/{infura_api_key}"
etherscan_api_key = os.getenv("ETHERSCAN_API_KEY")

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

def get_erc721_transactions(block_number, rpc_endpoint, etherscan_api_key):
    # Create a Web3 instance
    web3 = Web3(Web3.HTTPProvider(rpc_endpoint))

    # Create an empty DataFrame to store the transaction information
    # Create an empty DataFrame to store the transaction information
    schema = [
        ("Block Number", pl.UInt64),
        ("Block Hash", pl.Utf8),
        ("Block Timestamp", pl.UInt64),
        ("Transaction Hash", pl.Utf8),
        ("Contract Address", pl.Utf8),
        ("Sender", pl.Utf8),
        ("Value", pl.UInt64),
        ("Function Name", pl.Utf8),
        ("Function Arguments", pl.Utf8),
        ("Gas Limit", pl.UInt64),
        ("Gas Price", pl.UInt64),
    ]

    block_df = pl.DataFrame({}, schema=schema)

    # Retrieve the block information
    block = web3.eth.get_block(block_number,full_transactions=True)

    if block is not None:
        block_number = block['number']
        block_hash = block['hash']
        block_timestamp = block['timestamp']

        # Loop through each transaction in the block
        for tx in block['transactions']:

            # Check if the transaction involves a contract
            if tx['to'] is not None:
                contract_address = tx['to']
                if web3.eth.get_code(contract_address) != '0x':
                    # Contract exists at the address

                    # Attempt to check if it is an ERC721 contract
                    abi = get_abi(contract_address, etherscan_api_key)

                    contract = web3.eth.contract(address=contract_address)
                    is_erc721 = is_erc721_compliant(abi)

                    if is_erc721:

                        function_inputs_decoded = web3.eth.contract(abi=abi).decode_function_input(data=tx['input'])
                        function_name = str(function_inputs_decoded[0])[10:-1]
                        function_arguments = str(function_inputs_decoded[1])

                        transaction_dict = {
                            "Block Number": block_number,
                            "Block Hash": block_hash.hex(),
                            "Block Timestamp": block_timestamp,
                            "Transaction Hash": tx["hash"].hex(),
                            "Contract Address": contract_address,
                            "Sender": tx['from'],
                            "Value": tx['value'],
                            "Function Name": function_name,
                            "Function Arguments": function_arguments,
                            "Gas Limit": tx['gas'],
                            "Gas Price": tx['gasPrice']
                        }

                        transaction_df = pl.DataFrame(transaction_dict, schema=schema)

                        # Append the transaction information as a row to the DataFrame
                        block_df = block_df.extend(
                            transaction_df
                        )
    
    # Print the DataFrame
    print(block_df)
    return(block_df)

# Test the function with a block number
block_number = 17497810
get_erc721_transactions(block_number, infura_url, etherscan_api_key)
