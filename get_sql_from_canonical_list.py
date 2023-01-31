import json
import pandas as pd

def dataframe_to_sql(df, file_path):
    # Create the template for the SQL query
    template = """WITH grantees AS (
    SELECT title, address, round_name, round_address FROM (VALUES {}) x (title, address, round_name, round_address)
    )
    SELECT * FROM grantees;
    """
    # Create a list to store the values
    values_list = []
    # Iterate through the DataFrame
    for _, row in df.iterrows():
        # Check for missing values in the specified columns
        if not row[['grantee_title', 'grantee_address', 'round_name', 'round_address']].isnull().any():
            # If there are no missing values, add the row to the values list
            # replace single quotes with double quotes in title column
            title = row["grantee_title"].replace("'", "''")
            # Replace the first character of recipient with '\' and save it under the column named address
            address = '\\' + row["grantee_address"][1:]
            round_address = '\\' + row["round_address"][1:]
            values_list.append("('{}', '{}' ::bytea, '{}', '{}')".format(title, address, row["round_name"], round_address))

    # Create the values string
    values = ", ".join(values_list)
    # Write the query to the file
    with open(file_path, "w") as f:
        f.write(template.format(values))

def main(): 
    ROUNDS = {
        'Climate Solutions': '0x1b165fe4da6bc58ab8370ddc763d367d29f50ef0', 
        'Open Source Software': '0xd95a1969c41112cee9a2c931e849bcef36a16f4c', 
        'Ethereum Infrastructure': '0xe575282b376e3c9886779a841a2510f1dd8c2ce4'
    }

    with open('canonical_project_list.json') as json_file:
        data = json.load(json_file)

    df = pd.DataFrame(columns = ["round_name","round_address", "grantee_title", "grantee_address"])

    for round_name, projects in data.items():
        for project in projects:
            df = df.append({"grantee_title": project["title"], "grantee_address": project["address"], "round_name": round_name,"round_address": ROUNDS[round_name]}, ignore_index=True)

    dataframe_to_sql(df, "all_alpha_round_grantees.sql")

if __name__ == "__main__":
    main()
