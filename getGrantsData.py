import json
import requests
import pandas as pd
from datetime import datetime

# Get the Round Data by Querying TheGraph 
def get_round_data(round_id, API_KEY):
    # URL with the endpoint of the round manager subgraph for mainnet 
    url = "https://gateway.thegraph.com/api/" + API_KEY + "/subgraphs/id/BQXTJRLZi7NWGq5AXzQQxvYNa5i1HmqALEJwy3gGJHCr"

    # Construct the GraphQL query
    query = '''
        {
    rounds(where:{
        id: "''' + round_id + '''"
    }) {
        id
        projects {
        id
        project
        status
        payoutAddress
        metaPtr {
            protocol
            pointer
        }
        }
    }
    }
    '''
    #Query TheGraph API for the round's data by POST request
    response = requests.post(url, json={'query': query})
    data = response.json()

    # Initialize an empty list to store the fields
    fields = []

    # Iterate through the JSON object and add data to our list
    for round in data['data']['rounds']:
        for project in round['projects']:
            fields.append({
                'round_id': round['id'],
                'project_id': project['project'],
                'status': project['status'],
                'payoutAddress': project['payoutAddress'],
                'pointer': project['metaPtr']['pointer']
            })

    df = pd.DataFrame(fields)
    return df

def retrieve_ipfs_file(cid):
    try:
        # Build the URL to the file on the Cloudflare IPFS gateway
        url = f"https://cloudflare-ipfs.com/ipfs/{cid}"
        # Send a GET request to the URL
        response = requests.get(url)
        # Parse the JSON data
        data = json.loads(response.content)
        # Extract the recipient and title fields
        recipient = data["application"]["recipient"]
        title = data["application"]["project"]["title"]
        # Return the recipient and title
        return recipient, title
    except:
        return None, None

def dataframe_to_sql(df, file_path):
    # Create the template for the SQL query
    template = """WITH grantees AS (
    SELECT title, address, status FROM (VALUES {}) x (title, address, status)
    )
    SELECT * FROM grantees;
    """
    # Create a list to store the values
    values_list = []
    # Iterate through the DataFrame
    for _, row in df.iterrows():
        # Check for missing values in the specified columns
        if not row[['title', 'recipient', 'status']].isnull().any():
            # If there are no missing values, add the row to the values list
            # replace single quotes with double quotes in title column
            title = row["title"].replace("'", "''")
            values_list.append("('{}', '{}', '{}')".format(title, row["recipient"], row["status"]))

    # Create the values string
    values = ", ".join(values_list)
    # Write the query to the file
    with open(file_path, "w") as f:
        f.write(template.format(values))


def main(id, API_KEY):
    #Get the current time
    current_time = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")

    #Pull the Data from TheGraph and save it to a dataframe
    df = get_round_data(id, API_KEY)

    # Add IPFS data with the grantees name and address 
    df[['recipient','title']] = df['pointer'].apply(lambda x: pd.Series(retrieve_ipfs_file(x)))
   
    #Construct names for files that will be saved
    csv_file_name = '{}_{}_data.csv'.format(current_time, id)
    sql_file_name = '{}_{}_data.sql'.format(current_time, id) 

    # Save The Data 
    df.to_csv(csv_file_name, index=False)
    dataframe_to_sql(df, sql_file_name)

# Replace the id with the right round id and your own API_KEY
id = "0xff519cf66c0ef800dcd32c92be89841859348fd6" #update this
API_KEY = "YOUR_KEY_HERE" #update this
main(id, API_KEY)
print("done")