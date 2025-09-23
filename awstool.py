# awstool.py
import io, os
import json
import http.client
import urllib.parse
import pandas as pd
from datetime import datetime, timedelta
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient
import traceback
import numpy as np
import pyodbc






#Billing_report = None
last_country = None
last_start_date = None
last_end_date = None

# Global CSV path
sap_consolidation_csv = "sap_consolidation.csv"

# -----------------------
# Azure Key Vault Setup
# -----------------------
VAULT_URL = "https://tds-bi-vault.vault.azure.net/"
credential = DefaultAzureCredential()
secret_client = SecretClient(vault_url=VAULT_URL, credential=credential)

# -----------------------
# Country Configuration
# -----------------------
country_cfg = {
    "BE": {"secret_id": "api-keys-BE", "AWS": 57272, "Account_ID": 301},
    "AT": {"secret_id": "api-keys-AT", "AWS": 57269, "Account_ID": 302},
    "ES": {"secret_id": "api-keys-ES", "AWS": 57271, "Account_ID": 394},
    "CH": {"secret_id": "api-keys-CH", "AWS": 57632, "Account_ID": 306},
    "CZ": {"secret_id": "api-keys-CZ", "AWS": 57633, "Account_ID": 2786},
    "DE": {"secret_id": "api-keys-DE", "AWS": 57634, "Account_ID": 309},
    "DK": {"secret_id": "api-keys-DK", "AWS": 57635, "Account_ID": 975},
    "FI": {"secret_id": "api-keys-FI", "AWS": 57691, "Account_ID": 979},
    "FR": {"secret_id": "api-keys-FR", "AWS": 57692, "Account_ID": 471},
    "HR": {"secret_id": "api-keys-HR", "AWS": 57693, "Account_ID": 2957},
    "HU": {"secret_id": "api-keys-HU", "AWS": 57694, "Account_ID": 305},
    "IT": {"secret_id": "api-keys-IT", "AWS": 57695, "Account_ID": 645},
    "NL": {"secret_id": "api-keys-NL", "AWS": 57696, "Account_ID": 303},
    "NO": {"secret_id": "api-keys-NO", "AWS": 57697, "Account_ID": 1013},
    "PL": {"secret_id": "api-keys-PL", "AWS": 57698, "Account_ID": 308},
    "PT": {"secret_id": "api-keys-PT", "AWS": 57708, "Account_ID": 950},
    "RO": {"secret_id": "api-keys-RO", "AWS": 57699, "Account_ID": 307},
    "RS": {"secret_id": "api-keys-RS", "AWS": 57702, "Account_ID": 9950},
    "SE": {"secret_id": "api-keys-SE", "AWS": 57703, "Account_ID": 808},
    "SI": {"secret_id": "api-keys-SI", "AWS": 57707, "Account_ID": 7484},
    "TR": {"secret_id": "api-keys-TR", "AWS": 57705, "Account_ID": 630},
    "UK": {"secret_id": "api-keys-UK", "AWS": 57704, "Account_ID": 304}
}

emea_cfg = {
    "EMEA": {"secret_id": "api-keys-EMEA", "AWS": 57273, "Account_ID": 240},
}

# -----------------------
# Helper: Refresh token
# -----------------------
def refresh_token(cfg):
    """
    Refresh token and update Azure Key Vault.
    Works locally using kvault_connections().
    """
    

    conn = http.client.HTTPSConnection("ion.tdsynnex.com")
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    # Get old secret
    secret_value = secret_client.get_secret(cfg["secret_id"]).value
    secret_json = json.loads(secret_value)
    old_refresh = secret_json["refresh_key"]

    # Call API for new token
    body = urllib.parse.urlencode({
        "grant_type": "refresh_token",
        "refresh_token": old_refresh
    })
    conn.request("POST", "/oauth/token", body, headers)
    resp = conn.getresponse()
    resp_json = json.loads(resp.read().decode("utf-8"))

    new_refresh = resp_json["refresh_token"]
    new_access = resp_json["access_token"]

    # Update secret in Key Vault
    secret_client.set_secret(
        cfg["secret_id"],
        json.dumps({"refresh_key": new_refresh, "access_key": new_access})
    )

    return new_access

# -----------------------
# Get database password
# -----------------------
def get_db_password():
    """
    Fetch database password from Azure Key Vault.
    """
    secret_value = secret_client.get_secret("database-password").value
    return secret_value

# Example usage
db_password = get_db_password()

# -----------------------
# Main Function
# -----------------------
def run_awstool(country: str, start_date: str, end_date: str):
    global  last_country, last_start_date, last_end_date
    """
    Run AWS Tool:
    1. Fetch country-level report (date range from HTML).
    2. Fetch rolling 1-year EMEA report.
    3. Merge/group both datasets into Billing_report.
    """
    try:
        # --- Step 1: Country-specific report ---
        if country not in country_cfg:
            return {"error": f"Country {country} not supported."}

        cfg_country = country_cfg[country]
        access_token_country = refresh_token(cfg_country)
        

        # Dates → ISO8601
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt   = datetime.strptime(end_date, "%Y-%m-%d")
        start_iso = start_dt.strftime('%Y-%m-%dT00:00:00Z')
        end_iso   = end_dt.strftime('%Y-%m-%dT23:59:59Z')

        # Request payload
        payload_country = {
            "report_id": cfg_country["AWS"],
            "report_module": "REPORTS_REPORTS_MODULE",
            "category": "BILLING_REPORTS",
            "specs": {
                "date_range_option": {
                    "selected_range": {
                        "fixed_date_range": {"start_date": start_iso, "end_date": end_iso}
                    }
                }
            }
        }

        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {access_token_country}"}
        conn = http.client.HTTPSConnection("ion.tdsynnex.com")
        conn.request(
            "POST",
            f"/api/v3/accounts/{cfg_country['Account_ID']}/reports/{cfg_country['AWS']}/reportDataCsv",
            json.dumps(payload_country),
            headers
        )
        res = conn.getresponse()
        data = res.read().decode("utf-8")

        if res.status != 200:
            return {"error": f"Country {country} failed: HTTP {res.status} - {data}"}

        report_json = json.loads(data)
        df_country = pd.read_csv(io.StringIO(report_json["results"]))

        # Normalize country df
        df_country.columns = df_country.columns.str.replace('SAP_ID', 'SAP ID')
        df_country['Country'] = country

        # Standardize cost/margin columns
        df_country.columns = df_country.columns.str.replace(
            r'Seller Cost \((EUR|GBP|NOK|SEK|CHF|DKK|USD|AUD|CAD|HKD|INR)\)', 'Seller Cost', regex=True)
        df_country.columns = df_country.columns.str.replace(
            r'Customer Cost \((EUR|GBP|NOK|SEK|CHF|DKK|USD|AUD|CAD|HKD|INR)\)', 'Customer Cost', regex=True)
        df_country.columns = df_country.columns.str.replace(
            r'Margin \((EUR|GBP|NOK|SEK|CHF|DKK|USD|AUD|CAD|HKD|INR)\)', 'Margin', regex=True)
        df_country.columns = df_country.columns.str.replace(
            r'Sales Price Of Unit \((EUR|GBP|NOK|SEK|CHF|DKK|USD|AUD|CAD|HKD|INR)\)', 'Sales Price Of Unit', regex=True)

        # --- Step 2: EMEA rolling report ---

        cfg_emea = emea_cfg["EMEA"]
        access_token_emea = refresh_token(cfg_emea)

        end_dt = datetime.utcnow()
        start_dt = end_dt - timedelta(days=365)
        start_iso = start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_iso   = end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        payload_emea = {
            "report_id": cfg_emea["AWS"],
            "report_module": "REPORTS_REPORTS_MODULE",
            "category": "BILLING_REPORTS",
            "specs": {
                "date_range_option": {
                    "selected_range": {
                        "fixed_date_range": {"start_date": start_iso, "end_date": end_iso}
                    }
                }
            }
        }

        headers_emea = {"Content-Type": "application/json", "Authorization": f"Bearer {access_token_emea}"}
        conn.request(
            "POST",
            f"/api/v3/accounts/{cfg_emea['Account_ID']}/reports/{cfg_emea['AWS']}/reportDataCsv",
            json.dumps(payload_emea),
            headers_emea
        )
        res = conn.getresponse()
        data = res.read().decode("utf-8")

        if res.status != 200:
            return {"error": f"EMEA failed: HTTP {res.status} - {data}"}

        report_json = json.loads(data)
        final_df = pd.read_csv(io.StringIO(report_json["results"]))

        df_country['SAP ID (customer)'] = pd.to_numeric(df_country['SAP ID (customer)'], errors="coerce")  # convert invalid to NaN
        df_country['SAP ID (customer)'] = df_country['SAP ID (customer)'].fillna(999999).astype(int)


        df_country['SAP ID (customer)'] = df_country['SAP ID (customer)'].astype('Int32')
        df_country['Cloud Account Number'] = df_country['Cloud Account Number'].astype(str).str.zfill(12)


        if final_df is not None:
            final_df['Account Number'] = final_df['Account Number'].astype(str)
            final_df = final_df[final_df['Account Number'].notna() & (final_df['Account Number'] != "")]
            global Billing_report, last_country, last_start_date, last_end_date
            Billing_report = pd.merge(
                df_country,
                final_df,
                left_on=['Cloud Account Number'],
                right_on=['Account Number'],
                how="left"
            )
        else:
            Billing_report = df_country.copy()


            


        last_country = country
        last_start_date = start_date
        last_end_date = end_date    

        # Rename for clarity
        rename_mapping = {
            "Cloud Account Number": "Account",
            "SAP ID (customer)": "SAP_ID",
            "Product Name": "Materials",
            "Assigned Customer Company": "End_Customer",
        }
        Billing_report.rename(columns=rename_mapping, inplace=True)

        Billing_report['Account'] = Billing_report['Account'].astype(str).str.zfill(12)

        #Billing_report['Account'] = pd.to_numeric(Billing_report['Account'], errors='coerce').astype('Int64')

        Billing_report["Seller Cost"] = pd.to_numeric(Billing_report["Seller Cost"], errors='coerce').round(2)
        Billing_report["Customer Cost"] = pd.to_numeric(Billing_report["Customer Cost"], errors='coerce').round(2)

        Billing_report["SAP_ID"] = pd.to_numeric(Billing_report["SAP_ID"], errors="coerce")  # convert invalid to NaN
        Billing_report["SAP_ID"] = Billing_report["SAP_ID"].fillna(999999).astype(int)


        Billing_report.to_csv("latest_report.csv", index=False)

        # save metadata in parallel
        metadata = {
        "country": country,
        "start_date": start_date,
        "end_date": end_date
        }
        
        with open("metadata.json", "w") as f:
            json.dump(metadata, f)

        # Calculate sums
        seller_sum = Billing_report["Seller Cost"].sum()
        customer_sum = Billing_report["Customer Cost"].sum()

        return {
            "final_df_message": f"from {start_date} to {end_date}",
            "country": country,
            "seller_sum": Billing_report["Seller Cost"].sum(),
            "customer_sum": Billing_report["Customer Cost"].sum()
        }




    

    except Exception as e:
        return {"error": str(e)}


# -----------------------------
# New: function to handle exception adjustments
# -----------------------------



def apply_exception(uploaded_file):
    global  last_country, last_start_date, last_end_date

    expected_headers = ["SAP ID", "Account"]  

    try:

        # Reload metadata
        with open("metadata.json") as f:
            metadata = json.load(f)

        country = metadata["country"]
        start_date = metadata["start_date"]
        end_date = metadata["end_date"]



        Billing_report = pd.read_csv("latest_report.csv", dtype={"Account": str})
        Billing_report["Account"] = Billing_report["Account"].str.zfill(12)
        # If SAP_ID must also be integer:
        Billing_report["SAP_ID"] = Billing_report["SAP_ID"].astype("Int64")
        # Load file (CSV or XLSX)
        if uploaded_file.filename.endswith(".csv"):
            exceptions = pd.read_csv(uploaded_file)
        elif uploaded_file.filename.endswith(".xlsx"):
            exceptions = pd.read_excel(uploaded_file)
        else:
            return {"error": "Unsupported file type. Please upload credit with proper format."}

        # Validate headers
        if list(exceptions.columns) != expected_headers:
            return {"error": f"Header is not correct. Expected: {', '.join(expected_headers)}"}
        

        exceptions['Account'] = exceptions['Account'].astype(str).str.zfill(12)

        exceptions['SAP ID'] = exceptions['SAP ID'].astype('Int64')

        # Create a mapping from Account → SAP ID from exceptions

        account_to_sap = exceptions.set_index("Account")["SAP ID"].to_dict()
        
        # Update SAP_ID in Billing_report wherever Account matches

        Billing_report["SAP_ID"] = Billing_report["Account"].map(account_to_sap).combine_first(Billing_report["SAP_ID"])


        Billing_report.to_csv("latest_report.csv", index=False)

        # Update summary after adjustments
        seller_sum = Billing_report["Seller Cost"].sum()
        customer_sum = Billing_report["Customer Cost"].sum()

        return {
            "final_df_message": f"from {start_date} to {end_date} (Exception Applied)",
            "country": country,
            "seller_sum": Billing_report["Seller Cost"].sum(),
            "customer_sum": Billing_report["Customer Cost"].sum()
        }


    



    except Exception as e:
        print(traceback.format_exc())
        return {"error": str(e)}
    



# -----------------------------
# New: function to handle file upload + credit adjustment
# -----------------------------



def apply_credit_adjustments(uploaded_file):
    global  last_country, last_start_date, last_end_date

    expected_headers = ["Account", "Credit"]


    try:

        # Reload metadata
        with open("metadata.json") as f:
            metadata = json.load(f)

        country = metadata["country"]
        start_date = metadata["start_date"]
        end_date = metadata["end_date"]




        Billing_report = pd.read_csv("latest_report.csv", dtype={"Account": str})
        Billing_report["Account"] = Billing_report["Account"].str.zfill(12)
        # If SAP_ID must also be integer:
        Billing_report["SAP_ID"] = Billing_report["SAP_ID"].astype("Int64")
        # Load file (CSV or XLSX)
        if uploaded_file.filename.endswith(".csv"):
            credit_df = pd.read_csv(uploaded_file)
        elif uploaded_file.filename.endswith(".xlsx"):
            credit_df = pd.read_excel(uploaded_file)
        else:
            return {"error": "Unsupported file type. Please upload credit with proper format."}

        # Validate headers
        if list(credit_df.columns) != expected_headers:
            return {"error": f"Header is not correct. Expected: {', '.join(expected_headers)}"}
        

        credit_df['Account'] = credit_df['Account'].astype(str).str.zfill(12)


        # Apply credits to Billing_report
        for _, credit_row in credit_df.iterrows():
            account_id = credit_row['Account']
            credit_amount_seller = credit_row['Credit']  # apply to Seller Cost
            credit_amount_customer = credit_row['Credit']  # apply to Customer Cost

            # Select rows in Billing_report for this account
            account_rows = Billing_report.index[Billing_report['Account'] == account_id]

            for idx in account_rows:
                # Seller Cost adjustment
                if credit_amount_seller > 0:
                    deduction = min(Billing_report.at[idx, 'Seller Cost'], credit_amount_seller)
                    Billing_report.at[idx, 'Seller Cost'] -= deduction
                    credit_amount_seller -= deduction

                # Customer Cost adjustment
                if credit_amount_customer > 0:
                    deduction = min(Billing_report.at[idx, 'Customer Cost'], credit_amount_customer)
                    Billing_report.at[idx, 'Customer Cost'] -= deduction
                    credit_amount_customer -= deduction

                # Stop early if both credits exhausted
                if credit_amount_seller <= 0 and credit_amount_customer <= 0:
                    break

        Billing_report.to_csv("latest_report.csv", index=False)

        # Update summary after adjustments
        seller_sum = Billing_report["Seller Cost"].sum()
        customer_sum = Billing_report["Customer Cost"].sum()

        return {
            "final_df_message": f"from {start_date} to {end_date} (Adjusted with credit)",
            "country": country,
            "seller_sum": Billing_report["Seller Cost"].sum(),
            "customer_sum": Billing_report["Customer Cost"].sum()
        }

    

    except Exception as e:
        print(traceback.format_exc())
        return {"error": str(e)}
    

    # -----------------------------
# New: function to add PO number to Billing_report
# -----------------------------



def apply_po_adjustments(uploaded_file):
    global  last_country, last_start_date, last_end_date

    expected_headers = ["Reseller SAP ID", "End Customer", "PO", "PO Condition"]


    try:

        # Reload metadata
        with open("metadata.json") as f:
            metadata = json.load(f)

        country = metadata["country"]
        start_date = metadata["start_date"]
        end_date = metadata["end_date"]



        Billing_report = pd.read_csv("latest_report.csv", dtype={"Account": str})
        Billing_report["Account"] = Billing_report["Account"].str.zfill(12)
        # If SAP_ID must also be integer:
        Billing_report["SAP_ID"] = Billing_report["SAP_ID"].astype("Int64")
        # Load file (CSV or XLSX)
        if uploaded_file.filename.endswith(".csv"):
            custom_po_df = pd.read_csv(uploaded_file, encoding="latin1")
        elif uploaded_file.filename.endswith(".xlsx"):
            custom_po_df = pd.read_excel(uploaded_file)
        else:
            return {"error": "Unsupported file type. Please upload credit with proper format."}

        # Validate headers
        if list(custom_po_df.columns) != expected_headers:
            return {"error": f"Header is not correct. Expected: {', '.join(expected_headers)}"}
        

        custom_po_df['End Customer'] = pd.to_numeric(custom_po_df['End Customer'], errors='coerce')

        custom_po_df['End Customer'] = custom_po_df['End Customer'].astype('Int64')

        custom_po_df['End Customer'] = custom_po_df['End Customer'].astype(str).str.zfill(12)

        custom_po_df['Reseller SAP ID'] = custom_po_df['Reseller SAP ID'].astype('Int64')



    
        

        custom_po_df_unique = custom_po_df[['Reseller SAP ID', 'End Customer','PO','PO Condition'
                        ]].drop_duplicates()

        

        Billing_report = pd.merge(Billing_report, 
                             custom_po_df_unique,  
                             left_on=['SAP_ID','Account'
                                      ],  # Columns in `sc_df`
                            right_on=['Reseller SAP ID', 'End Customer'
                                      ],        # Columns in `cee_df`
                                      how="left"  # Perform a left join
                                      )
        

        Billing_report = Billing_report.drop(['Reseller SAP ID',
                                        'End Customer', 'PO Condition'], axis=1)
        
        Billing_report.to_csv("latest_report.csv", index=False)

        # Update summary after adjustments
        seller_sum = Billing_report["Seller Cost"].sum()
        customer_sum = Billing_report["Customer Cost"].sum()


        return {
            "final_df_message": f"from {start_date} to {end_date} (Adjusted with PO)",
            "country": country,
            "seller_sum": Billing_report["Seller Cost"].sum(),
            "customer_sum": Billing_report["Customer Cost"].sum()
        }

    

    except Exception as e:
        print(traceback.format_exc())
        return {"error": str(e)}
    


        # -----------------------------
# New: function to call SAP id with end customer consolidation 

def get_sap_ids():
    """
    Query SAP IDs from SQL and save them as a local CSV for later download.
    """
    try:
        # --- SQL connection ---
        server = 'bicompute-dwh.database.windows.net'
        database = 'db-cloudbi'
        username = 'tdadmin'
        driver = '{ODBC Driver 18 for SQL Server}'
        password= db_password

        conn = pyodbc.connect(
            f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        )

        query = "SELECT * FROM aws.end_customer"
        sap_ids_df = pd.read_sql(query, conn)
        conn.close()

        # Format as consolidation DataFrame
        consolidation_df = pd.DataFrame({"SAP ID": sap_ids_df['SAP_ID'].tolist()})

        # Save to global CSV path
        consolidation_df.to_csv(sap_consolidation_csv, index=False)


    except Exception as e:
        print(f"Error while building SAP consolidation report: {e}")



# -----------------------------
    

            # -----------------------------
# New: function to add final consolidation  
# -----------------------------

def consolidation():
    global  last_country, last_start_date, last_end_date

    try:

        # Reload metadata
        with open("metadata.json") as f:
            metadata = json.load(f)

        country = metadata["country"]
        start_date = metadata["start_date"]
        end_date = metadata["end_date"]


        Billing_report = pd.read_csv("latest_report.csv", dtype={"Account": str})


        # Connection parameters
        server = 'bicompute-dwh.database.windows.net'  # or your server name
        database = 'db-cloudbi'
        username = 'tdadmin'
        password = db_password
        driver = '{ODBC Driver 18 for SQL Server}'

        conn = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}')

        # Query only the SAP_ID column
        query = "SELECT SAP_ID FROM aws.end_customer"

        # Load into a DataFrame
        sap_ids_df = pd.read_sql(query, conn)

        # Optional: if you want a single column as a list for your consolidation
        sap_ids_list = sap_ids_df['SAP_ID'].tolist()

        Billing_report["SAP_ID"] = Billing_report["SAP_ID"].replace("", pd.NA)  # turn empty strings into NaN
        Billing_report["SAP_ID"] = Billing_report["SAP_ID"].fillna(000000)

        Billing_report = Billing_report[Billing_report['SAP_ID'].notna()]

        # Build your consolidation DataFrame
        consolidation_df = pd.DataFrame({"SAP ID": sap_ids_list})

        query = "SELECT SAP_ID FROM aws.end_customer"
        sap_ids_df = pd.read_sql(query, conn)
        consolidation_df = pd.DataFrame({"SAP ID": sap_ids_df['SAP_ID'].tolist()})

        consolidation_df["Condition Creation/ Country"]="Creation By End Customer"

        consolidation_unique = consolidation_df[["SAP ID","Condition Creation/ Country"
                        ]].drop_duplicates()
        
        consolidation_unique['SAP ID'] = consolidation_unique['SAP ID'].astype('Int64')


        consolidation_unique['Condition Creation/ Country'] = (
            consolidation_unique['Condition Creation/ Country'].str.strip()
            )
        
        
        Billing_report = pd.merge(
            Billing_report,
            consolidation_unique,
            left_on=['SAP_ID'],
            right_on=['SAP ID'],
            how='left'
            )
        
        Billing_report['Condition Creation/ Country'] = Billing_report['Condition Creation/ Country'].fillna('Creation by Reseller')


        Billing_report["Account"] = Billing_report["Account"].str.zfill(12)
        # If SAP_ID must also be integer:
        Billing_report["SAP_ID"] = Billing_report["SAP_ID"].astype("Int64")

        if "PO" not in Billing_report.columns:
            Billing_report["PO"] = np.nan 
            
        Billing_report['PO'] = Billing_report['PO'].fillna('NaN')
        Billing_report['End_Customer'] = Billing_report['End_Customer'].fillna('unknown')

        if "Condition Creation/ Country" not in Billing_report.columns:
            Billing_report["Condition Creation/ Country"] = "creation by reseller"

        if "SAP ID" in Billing_report.columns:  # drop only if column exists
            Billing_report = Billing_report.drop('SAP ID', axis=1)
            
            
        selected_cols = ['Reseller Name', 'Account', 'SAP_ID', 'Seller Cost', "Materials",'Customer Cost',
                         'End_Customer', 'PO', 'Condition Creation/ Country']
        
        Billing_report = Billing_report[selected_cols]

        Billing_report['Material_id'] = np.where(
            Billing_report['Materials'].str.contains('TechCARE', case=False, na=False),
            11532184,
            6688949)

        # Convert to datetime and format
        start_fmt = pd.to_datetime(start_date).strftime("%m/%d/%y")
        end_fmt = pd.to_datetime(end_date).strftime("%m/%d/%y")
        billing_period_str = f"{start_fmt} to {end_fmt}"

        Billing_report["Billing period"] = billing_period_str
        Billing_report['Condition Creation/ Country'] = Billing_report['Condition Creation/ Country'].str.lower()

        

        # Split DataFrames
        reseller_df = Billing_report[Billing_report['Condition Creation/ Country'] == 'creation by reseller'].drop(columns=["End_Customer"])
        end_customer_df = Billing_report[Billing_report['Condition Creation/ Country'] == 'creation by end customer']

        # Group reseller
        grouped_reseller = (
            reseller_df.groupby(['SAP_ID', "Billing period", "Material_id", "PO", 'Condition Creation/ Country'], as_index=False)
                       .agg({"Seller Cost": "sum", "Customer Cost": "sum"})
        )

        # Group end customer
        grouped_end_customer = (
            end_customer_df.groupby(['SAP_ID', 'Condition Creation/ Country', 'PO',
                                     'Material_id', "Billing period", "End_Customer"], as_index=False)
                           .agg({"Seller Cost": "sum", "Customer Cost": "sum"})
        )

        # Merge
        Billing_report = pd.concat([grouped_end_customer, grouped_reseller], ignore_index=True)
        Billing_report['PO'] = Billing_report['PO'].replace('NaN', '')
        Billing_report['PO Condition'] = np.where(Billing_report['PO'] != '', 'PO header', '')

        # Add empty columns
        Billing_report['Account'] = ''
        Billing_report['Usage'] = ''
        Billing_report['Material Not Created'] = ''
        Billing_report['Sales Order Number'] = ''
        Billing_report['Billing Block'] = ''

        # Margin
        Billing_report['Margin'] = Billing_report['Customer Cost'] - Billing_report['Seller Cost']

        # Rename columns
        Billing_report.rename(columns={
            'SAP_ID': 'Reseller Name',
            'Condition Creation/ Country': 'Creation Condition',
            'Material_id': 'Materials',
            'End_Customer': 'End Customer'
        }, inplace=True)

        # Calculate sums for summary
        seller_sum = Billing_report["Seller Cost"].sum()
        customer_sum = Billing_report["Customer Cost"].sum()

        Billing_report["Seller Cost"] = pd.to_numeric(Billing_report["Seller Cost"], errors='coerce').round(2)
        Billing_report["Customer Cost"] = pd.to_numeric(Billing_report["Customer Cost"], errors='coerce').round(2)
        Billing_report["Margin"] = pd.to_numeric(Billing_report["Margin"], errors='coerce').round(2)

        # Reorder columns
        Billing_report = Billing_report[[
            'Reseller Name',
            'Account',
            'End Customer',
            'Materials',
            'Seller Cost',
            'Customer Cost',
            'Margin',
            'Usage',
            'Billing period',
            'Creation Condition',
            'Material Not Created',
            'PO',
            'PO Condition',
            'Sales Order Number',
            'Billing Block'
        ]]

        # Save latest version
        Billing_report.to_csv("latest_report.csv", index=False)

        

        return {
            "final_df_message": f"from {start_date} to {end_date} [Consolidated]",
            "country": country,
            "seller_sum": Billing_report["Seller Cost"].sum(),
            "customer_sum": Billing_report["Customer Cost"].sum()
        }

    except Exception as e:
        print(traceback.format_exc())
        return {"error": str(e)}
    

# New: function to get BlobServiceClient

def get_blob_service_client():
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if conn_str:
        return BlobServiceClient.from_connection_string(conn_str)
    else:
        # Use managed identity when deployed in Azure
        credential = DefaultAzureCredential()
        return BlobServiceClient(
            account_url="https://awstoolstorage.blob.core.windows.net",
            credential=credential
        )
    
def amend_sap_consolidation(uploaded_file):
    """
    Replace aws.end_customer table content with values from uploaded CSV.
    """
    try:
        # --- SQL connection ---
        server = 'bicompute-dwh.database.windows.net'
        database = 'db-cloudbi'
        username = 'tdadmin'
        driver = '{ODBC Driver 18 for SQL Server}'
        password= db_password

        conn = pyodbc.connect(
            f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        )
        cursor = conn.cursor()

        # 1. Drop & recreate the table
        cursor.execute("""
            IF OBJECT_ID('aws.end_customer', 'U') IS NOT NULL
                DROP TABLE aws.end_customer;
            CREATE TABLE aws.end_customer (
                SAP_ID NVARCHAR(50)
            );
        """)
        conn.commit()

        # 2. Load uploaded CSV into DataFrame
        if uploaded_file.filename.endswith(".csv"):
            sap_ids_df = pd.read_csv(uploaded_file)
        elif uploaded_file.filename.endswith(".xlsx"):
            sap_ids_df = pd.read_excel(uploaded_file)
        else:
            return {"error": "Unsupported file type. Please upload a CSV or XLSX."}

        # Ensure column consistency
        if "SAP ID" not in sap_ids_df.columns:
            return {"error": "File must contain a column named 'SAP ID'."}

        # 3. Insert new data
        for sap_id in sap_ids_df["SAP ID"].dropna().astype(str).tolist():
            cursor.execute("INSERT INTO aws.end_customer (SAP_ID) VALUES (?)", sap_id)

        conn.commit()
        conn.close()

        return {"message": f"Table aws.end_customer refreshed with {len(sap_ids_df)} rows."}

    except Exception as e:
        return {"error": str(e)}



    


    


    



    













