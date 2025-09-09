# awstool.py
import io
import json
import http.client
import urllib.parse
import pandas as pd
from datetime import datetime, timedelta
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import traceback
import numpy as np


Billing_report = None
last_country = None
last_start_date = None
last_end_date = None



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
# Main Function
# -----------------------
def run_awstool(country: str, start_date: str, end_date: str):
    global Billing_report, last_country, last_start_date, last_end_date
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


        df_country['SAP ID (customer)'] = df_country['SAP ID (customer)'].astype('Int32')
        df_country['Cloud Account Number'] = df_country['Cloud Account Number'].astype(str).str.zfill(12)


        if final_df is not None:
            final_df['Account Number'] = final_df['Account Number'].astype(str)
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


        Billing_report.to_csv("latest_report.csv", index=False)

        # Calculate sums
        seller_sum = Billing_report["Seller Cost"].sum()
        customer_sum = Billing_report["Customer Cost"].sum()

        return {
        "final_df_message": f"from {start_date} to {end_date}",
        "country": country,
        "seller_sum": seller_sum,
        "customer_sum": customer_sum
    }

    except Exception as e:
        return {"error": str(e)}


# -----------------------------
# New: function to handle exception adjustments
# -----------------------------



def apply_exception(uploaded_file):
    global Billing_report, last_country, last_start_date, last_end_date

    expected_headers = ["SAP ID", "Account"]  

    try:
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
            "final_df_message": f"from {last_start_date} to {last_end_date} (Exception Applied)",
            "country": last_country,
            "seller_sum": seller_sum,
            "customer_sum": customer_sum
            }

    except Exception as e:
        print(traceback.format_exc())
        return {"error": str(e)}
    

    # -----------------------------
# New: function to add PO number to Billing_report



# -----------------------------
# New: function to handle file upload + credit adjustment
# -----------------------------



def apply_credit_adjustments(uploaded_file):
    global Billing_report, last_country, last_start_date, last_end_date

    expected_headers = ["Account", "Credit", "Credit Remained", "Reseller ID To Delete"]


    try:
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
            "final_df_message": f"from {last_start_date} to {last_end_date} (Adjusted with credit)",
            "country": last_country,
            "seller_sum": seller_sum,
            "customer_sum": customer_sum
            }

    except Exception as e:
        print(traceback.format_exc())
        return {"error": str(e)}
    

    # -----------------------------
# New: function to add PO number to Billing_report
# -----------------------------



def apply_po_adjustments(uploaded_file):
    global Billing_report, last_country, last_start_date, last_end_date

    expected_headers = ["Reseller SAP ID", "End Customer", "PO", "PO Condition"]


    try:
        # Load file (CSV or XLSX)
        if uploaded_file.filename.endswith(".csv"):
            custom_po_df = pd.read_csv(uploaded_file)
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
            "final_df_message": f"from {last_start_date} to {last_end_date} (Adjusted with credit and PO)",
            "country": last_country,
            "seller_sum": seller_sum,
            "customer_sum": customer_sum
            }

    except Exception as e:
        print(traceback.format_exc())
        return {"error": str(e)}
    


        # -----------------------------
# New: function to add consolidation  to Billing_report
# -----------------------------



def apply_consolidation_adjustments(uploaded_file):
    global Billing_report, last_country, last_start_date, last_end_date

    expected_headers = ["SAP ID", "Condition Creation/ Country"]


    try:
        # Load file (CSV or XLSX)
        if uploaded_file.filename.endswith(".csv"):
            consolidation_df = pd.read_csv(uploaded_file)
        elif uploaded_file.filename.endswith(".xlsx"):
            consolidation_df = pd.read_excel(uploaded_file)
        else:
            return {"error": "Unsupported file type. Please upload credit with proper format."}

        # Validate headers
        if list(consolidation_df.columns) != expected_headers:
            return {"error": f"Header is not correct. Expected: {', '.join(expected_headers)}"}

        consolidation_unique = consolidation_df[["SAP ID","Condition Creation/ Country"
                        ]].drop_duplicates()
        
        consolidation_df['SAP ID'] = consolidation_df['SAP ID'].astype('Int64')


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

        #Billing_report = Billing_report.drop('SAP ID', axis=1)



        #Billing_report['Material_id'] = np.where(Billing_report['Materials'].str.contains('TechCARE', case=False, na=False),11532184,6688949)


        # Convert to datetime and format
        #start_fmt = pd.to_datetime(last_start_date).strftime("%m/%d/%y")
        #end_fmt = pd.to_datetime(last_end_date).strftime("%m/%d/%y")
        # Create billing period string
        #billing_period_str = f"{start_fmt} to {end_fmt}"
        # Add to DataFrame
        #Billing_report["Billing period"] = billing_period_str

        #Billing_report.rename(columns={"Condition Creation/ Country":'Creation Condition'}, inplace=True)
 
        Billing_report.to_csv("latest_report.csv", index=False)

        # Update summary after adjustments
        seller_sum = Billing_report["Seller Cost"].sum()
        customer_sum = Billing_report["Customer Cost"].sum()

        return {
            "final_df_message": f"from {last_start_date} to {last_end_date} (Adjusted with credit and PO and Consolidation)",
            "country": last_country,
            "seller_sum": seller_sum,
            "customer_sum": customer_sum
            }

    except Exception as e:
        print(traceback.format_exc())
        return {"error": str(e)}
    

            # -----------------------------
# New: function to add final consolidation  
# -----------------------------

def consolidation():
    global Billing_report, last_country, last_start_date, last_end_date

    try:

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
        start_fmt = pd.to_datetime(last_start_date).strftime("%m/%d/%y")
        end_fmt = pd.to_datetime(last_end_date).strftime("%m/%d/%y")
        billing_period_str = f"{start_fmt} to {end_fmt}"

        Billing_report["Billing period"] = billing_period_str
        Billing_report['Condition Creation/ Country'] = Billing_report['Condition Creation/ Country'].str.lower()
        Billing_report = Billing_report[Billing_report['SAP_ID'].notna()]

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

        # Calculate sums for summary
        seller_sum = Billing_report["Seller Cost"].sum()
        customer_sum = Billing_report["Customer Cost"].sum()

        return {
            "final_df_message": f"from {last_start_date} to {last_end_date} [Consolidated]",
            "country": last_country,
            "seller_sum": seller_sum,
            "customer_sum": customer_sum
        }

    except Exception as e:
        print(traceback.format_exc())
        return {"error": str(e)}



















