# awstool.py
import json
import http.client
import urllib.parse
import pandas as pd
import io
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

KEYVAULT_URL = "https://gorkavault.vault.azure.net/"

def run_awstool(start_date, end_date):
    """
    Refresh tokens from Azure Key Vault, fetch report data, return DataFrame.
    start_date and end_date must be strings in format 'YYYY-MM-DD'.
    """
    credential = DefaultAzureCredential()
    secret_client = SecretClient(vault_url=KEYVAULT_URL, credential=credential)

    country_cfg = {
        "AT": {"secret_id": "AT", "ReportId": 49708, "Account_ID": 302}
    }

    token_rows = []
    all_report_data = []

    for country, cfg in country_cfg.items():
        try:
            # Get current refresh key from Key Vault
            secret_bundle = secret_client.get_secret(cfg["secret_id"])
            old_refresh = secret_bundle.value

            # Refresh call
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
            access_token = resp_json["access_token"]

            # Save ONLY the new refresh key back
            secret_client.set_secret(cfg["secret_id"], new_refresh)

            token_rows.append({
                "Country": country,
                "ReportId": cfg["ReportId"],
                "Account_ID": cfg["Account_ID"],
                "Access_Token": access_token,
            })

        except Exception as e:
            return pd.DataFrame([{"Error": f"Token refresh failed for {country}: {e}"}])

    def get_report_data_csv(account_id, report_id, bearer_token, start_date, end_date):
        conn = http.client.HTTPSConnection("ion.tdsynnex.com")
        payload = {
            "report_id": report_id,
            "report_module": "REPORTS_REPORTS_MODULE",
            "category": "BILLING_REPORTS",
            "specs": {
                "date_range_option": {
                    "selected_range": {
                        "fixed_date_range": {
                            "start_date": start_date + "T00:00:00Z",
                            "end_date": end_date + "T23:59:59Z"
                        }
                    }
                }
            }
        }
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {bearer_token}"
        }

        conn.request(
            "POST",
            f"/api/v3/accounts/{account_id}/reports/{report_id}/reportDataCsv",
            json.dumps(payload),
            headers
        )
        res = conn.getresponse()
        data = res.read().decode("utf-8")

        if res.status != 200:
            raise RuntimeError(f"HTTP {res.status} - {data}")

        report_json = json.loads(data)
        csv_data = report_json["results"]

        df = pd.read_csv(io.StringIO(csv_data))
        df["Country"] = country
        return df

    for _, row in pd.DataFrame(token_rows).iterrows():
        try:
            df = get_report_data_csv(
                row["Account_ID"],
                row["ReportId"],
                row["Access_Token"],
                start_date,
                end_date
            )
            all_report_data.append(df)
        except Exception as e:
            return pd.DataFrame([{"Error": f"Report fetch failed for {row['Country']}: {e}"}])

    return pd.concat(all_report_data, ignore_index=True) if all_report_data else pd.DataFrame()
