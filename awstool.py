# awstool.py
import io
import json
import http.client
import urllib.parse
import pandas as pd
from datetime import datetime, timedelta
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

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

emea_cfg = {"secret_id": "api-keys-EMEA", "AWS": 57273, "Account_ID": 240}

# -----------------------
# Helper: Refresh token
# -----------------------
def refresh_token(cfg):
    """Refresh token and update Azure Key Vault."""
    conn = http.client.HTTPSConnection("ion.tdsynnex.com")
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    secret_value = secret_client.get_secret(cfg["secret_id"]).value
    secret_json = json.loads(secret_value)
    old_refresh = secret_json["refresh_key"]

    body = urllib.parse.urlencode({
        "grant_type": "refresh_token",
        "refresh_token": old_refresh
    })
    conn.request("POST", "/oauth/token", body, headers)
    resp = conn.getresponse()
    resp_json = json.loads(resp.read().decode("utf-8"))

    new_refresh = resp_json["refresh_token"]
    new_access = resp_json["access_token"]

    # Update secret
    secret_client.set_secret(
        cfg["secret_id"],
        json.dumps({"refresh_key": new_refresh, "access_key": new_access})
    )

    return new_access

# -----------------------
# Main Function
# -----------------------
def run_awstool(country: str, start_date: str, end_date: str):
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
        access_token_emea = refresh_token(emea_cfg)

        end_dt = datetime.utcnow()
        start_dt = end_dt - timedelta(days=365)
        start_iso = start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_iso   = end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        payload_emea = {
            "report_id": emea_cfg["AWS"],
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
            f"/api/v3/accounts/{emea_cfg['Account_ID']}/reports/{emea_cfg['AWS']}/reportDataCsv",
            json.dumps(payload_emea),
            headers_emea
        )
        res = conn.getresponse()
        data = res.read().decode("utf-8")

        if res.status != 200:
            return {"error": f"EMEA failed: HTTP {res.status} - {data}"}

        report_json = json.loads(data)
        final_df = pd.read_csv(io.StringIO(report_json["results"]))

        # --- Step 3: Merge ---
        grouped = df_country.groupby(
            ['Reseller Name','Cloud Account Number', 'Product Name','SAP ID (customer)']
        )[['Seller Cost', 'Customer Cost']].sum().reset_index()

        grouped['SAP ID (customer)'] = grouped['SAP ID (customer)'].astype('Int32')
        grouped['Cloud Account Number'] = grouped['Cloud Account Number'].astype(str)

        if final_df is not None:
            final_df['Account Number'] = final_df['Account Number'].astype(str)
            Billing_report = pd.merge(
                grouped,
                final_df,
                left_on=['Cloud Account Number'],
                right_on=['Account Number'],
                how="left"
            )
        else:
            Billing_report = grouped.copy()

        # Rename for clarity
        rename_mapping = {
            "Cloud Account Number": "Account",
            "SAP ID (customer)": "SAP_ID",
            "Product Name": "Materials",
            "Assigned Customer Company": "End_Customer",
        }
        Billing_report.rename(columns=rename_mapping, inplace=True)

        # Prepare output 
        final_df_count = len(final_df) if final_df is not None else 0 
        final_df_message = f"EMEA rolling report contains {final_df_count} rows." if final_df_count > 0 else "EMEA rolling report is empty or API failed."

        return {
            "final_df_message": final_df_message,
            "country": country,
            "rows": Billing_report.head(10).to_dict(orient="records")
        }

    except Exception as e:
        return {"error": str(e)}
