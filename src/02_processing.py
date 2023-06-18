from web3 import Web3
import polars as pl
import os

infura_api_key = os.getenv("INFURA_API_KEY")
infura_url = f"https://mainnet.infura.io/v3/{infura_api_key}"

web3 = Web3(Web3.HTTPProvider(infura_url))

transactions_df = pl.read_parquet("../data/raw/transactions.parquet")
transactions_df = transactions_df.with_columns(pl.col("Transaction Data").apply(lambda x: json.loads(x)['to']).alias("Contract Address"))
transactions_df = transactions_df.with_columns(pl.col("Transaction Data").apply(lambda x: json.loads(x)['from']).alias("Sender"))
transactions_df = transactions_df.with_columns(pl.col("Transaction Data").apply(lambda x: json.loads(x)['value']).alias("Value"))
transactions_df = transactions_df.with_columns(pl.col("Transaction Data").apply(lambda x: json.loads(x)['input']).alias("Input"))
transactions_df = transactions_df.with_columns(pl.col("Transaction Data").apply(lambda x: json.loads(x)['gas']).alias("Gas Limit"))
transactions_df = transactions_df.with_columns(pl.col("Transaction Data").apply(lambda x: json.loads(x)['gasPrice']).alias("Gas Price"))