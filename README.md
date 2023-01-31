# Gitcoin-Helpers

Starting to collect little scripts which may be helpful when working with Gitcoin Data and Protocol. Contributions welcome!

## GetGrantsData.py
This script takes a round ID and API_KEY (your API_KEY for querying TheGraph protocol) as input and returns data on the projects in the round, primarily the projects title and payout_address. Data is gathered from the mainnet subgraph and IPFS. The output comes in two forms: a csv file and a SQL file. The SQL file can be used to start a Dune Query.

## create_hypercerts_metadata.py
This script takes as arguments:
1. The path of a csv file created by `GetGrantsData.py` 
2. A directory for exporting hypercerts metadata (as a json), eg, `../metadata/myround`
It tranforms the data in the csv file into a metadata file (ready for uploading on IPFS) that stores all required hypercerts properties

## canonical_project_list.json
This file has data on the projects in all three of the Gitcoin Alpha Rounds. For each round, there is a list of grantee titles and wallet addresses 

## get_sql_from_canonical_list.py
This script uses the canonical list and a provided list of round names and round addresses to build and ouput a SQL file which has every alpha grantee's title, address, round_name, and round_address. This script takes no arguments
