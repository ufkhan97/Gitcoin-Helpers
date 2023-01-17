# Gitcoin-Helpers

Starting to collect little scripts which may be helpful when working with Gitcoin Data and Protocol. Contributions welcome!

## GetGrantsData.py
This script takes a round ID and API_KEY as input and returns data on the projects in the round, primarily the projects title and payout_address. Data is gathered from the mainnet subgraph and IPFS. The output comes in two forms: a csv file and a SQL file. The SQL file can be used to start a Dune Query
