# awstool.py
import io
import json
import http.client
import urllib.parse
import pandas as pd
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# Key Vault setup
VAULT_URL = "https://tds-bi-vault.vault.azure.net/"
credential = DefaultAzureCredential()
secret_client = SecretClient(vault_url=VAULT_URL, credential=credential)

# Country configuration
country_cfg = {
    "BE": {"secret_id": "api-keys-BE", "GCP_Pricebook": 49709, "Account_ID": 301},
    "CH": {"secret_id": "api-keys-CH", "GCP_Pricebook": 49725, "Account_ID": 306},
    # Add more countries as needed
}

def run_awstool(country: str, start_date: str, end_date: str):
    """
    Run AWS Tool: rotate token for selected country, fetch report, return preview.
    Inputs:
        - country: str, country code (e.g., "BE")
        - start_date: str, YYYY-MM-DD
        - end_date: str, YYYY-MM-DD
    Returns:
        dict with rotation_status and report preview or error.
    """
    if country not in country_cfg:
        return {"error": f"Country {country} not supported."}

    cfg = country_cfg[country]

    try:
        # 🔑 1. Get secret from Key Vault
        secret_value = secret_client.get_secret(cfg["secret_id"]).value
        secret_json = json.loads(secret_value)
        old_refresh = secret_json["refresh_key"]

        # 🔄 2. Refresh token
        conn = http.client.HTTPSConnection("ion.tdsynnex.com")
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
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

        # 🔹 3. Convert input dates to RFC3339 timestamp
        start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_dt   = datetime.strptime(end_date, "%Y-%m-%d")
        start_date_rfc = start_date_dt.strftime("%Y-%m-%dT00:00:00Z")
        end_date_rfc   = end_date_dt.strftime("%Y-%m-%dT23:59:59Z")

        # 🔹 4. Fetch report
        payload = {
            "report_id": cfg["GCP_Pricebook"],
            "report_module": "REPORTS_REPORTS_MODULE",
            "category": "BILLING_REPORTS",
            "specs": {
                "date_range_option": {
                    "selected_range": {
                        "fixed_date_range": {
                            "start_date": start_date_rfc,
                            "end_date": end_date_rfc
                        }
                    }
                }
            }
        }

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {new_access}"
        }

        conn.request(
            "POST",
            f"/api/v3/accounts/{cfg['Account_ID']}/reports/{cfg['GCP_Pricebook']}/reportDataCsv",
            json.dumps(payload),
            headers
        )
        res = conn.getresponse()
        data = res.read().decode("utf-8")

        if res.status != 200:
            return {"error": f"HTTP {res.status} - {data}"}

        report_json = json.loads(data)
        csv_data = report_json["results"]

        df = pd.read_csv(io.StringIO(csv_data))
        df["Country"] = country

        # Return preview (first 5 rows)
        return {
            "rotation_status": "success",
            "country": country,
            "rows": df.head(5).to_dict(orient="records")
        }

    except Exception as e:
        return {"error": str(e)}
