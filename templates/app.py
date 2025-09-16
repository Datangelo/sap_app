import json, os, io, traceback
from io import BytesIO
from flask import Flask, request, send_file, jsonify, render_template, send_from_directory
import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from awstool import run_awstool, apply_credit_adjustments,apply_po_adjustments,apply_exception,consolidation
from awstool import last_country, last_start_date, last_end_date  # import globals
import csv

load_dotenv() 

app = Flask(__name__)

# Configuration via environment variables
STORAGE_ACCOUNT_URL = os.environ.get("STORAGE_ACCOUNT_URL")
CONTAINER_NAME = os.environ.get("CONTAINER_NAME")


#STORAGE_ACCOUNT_URL = f"https://awstoolstorage.blob.core.windows.net"
CONTAINER_NAME = "billing-report-uploaded"

blob_service_client = BlobServiceClient(account_url=STORAGE_ACCOUNT_URL, credential=DefaultAzureCredential())



# ---------- STEP 1 ----------
@app.route("/awstool", methods=["GET", "POST"])
def awstool():
    result = None
    if request.method == "POST":
        country = request.form.get("country")
        start_date = request.form.get("start_date")
        end_date = request.form.get("end_date")

        result = run_awstool(country, start_date, end_date)
        if "error" not in result:
            # Save metadata for later use in download
            with open("metadata.json", "w") as f:
                json.dump({
                    "country": country,
                    "start_date": start_date,
                    "end_date": end_date
                }, f)

    return render_template("awstool.html", result=result)


# ---------- STEP 2b ----------
@app.route("/upload_credits", methods=["POST"])
def upload_credits():
    if "file" not in request.files:
        return render_template("awstool.html", result={"error": "No file uploaded"})

    file = request.files["file"]
    if file.filename == "":
        return render_template("awstool.html", result={"error": "No file selected"})

    result = apply_credit_adjustments(file)
    return render_template("awstool.html", result=result)


# ---------- STEP 2a ----------
@app.route("/upload_exception", methods=["POST"])
def upload_exception():
    if "file" not in request.files:
        return render_template("awstool.html", result={"error": "No file uploaded"})

    file = request.files["file"]
    if file.filename == "":
        return render_template("awstool.html", result={"error": "No file selected"})

    result = apply_exception(file)
    return render_template("awstool.html", result=result)


# ---------- STEP 3 ----------
@app.route("/consolidation", methods=["GET", "POST"])
def run_consolidation():
    result = consolidation()
    return render_template("awstool.html", result=result)


# ---------- STEP 2c ----------
@app.route("/upload_po", methods=["POST"])
def upload_po():
    if "file" not in request.files:
        return render_template("awstool.html", result={"error": "No file uploaded"})

    file = request.files["file"]
    if file.filename == "":
        return render_template("awstool.html", result={"error": "No file selected"})

    result = apply_po_adjustments(file)
    return render_template("awstool.html", result=result)


# ---------- STEP 4 ----------
@app.route("/download_csv")
def download_csv():
    try:
        # --- Load metadata ---
        if os.path.exists("metadata.json"):
            with open("metadata.json", "r") as f:
                metadata = json.load(f)
        else:
            metadata = {}

        country = metadata.get("country", "unknown")
        start_fmt = metadata.get("start_date", "unknown").replace("-", "")
        end_fmt = metadata.get("end_date", "unknown").replace("-", "")

        filename = f"AWS_Billing_Report_{country}_from_{start_fmt}_to_{end_fmt}.csv"

        # --- Load CSV into memory ---
        with open("latest_report.csv", "rb") as f:
            file_bytes = io.BytesIO(f.read())

        # --- Upload to Azure Blob ---
        file_bytes.seek(0)  # reset pointer before upload
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME,
            blob=filename
        )
        blob_client.upload_blob(file_bytes, overwrite=True)

        # --- Return file to user ---
        file_bytes.seek(0)  # reset pointer again for download
        return send_file(
            file_bytes,
            mimetype="text/csv",
            as_attachment=True,
            download_name=filename
        )

    except Exception as e:
        return f"Error: {str(e)}", 500

    
    #----------- Templates ----------

@app.route("/download_template/<template>")
def download_template(template):
    import io
    import csv

    templates = {
        "exceptions": ["SAP ID", "Account"],
        "credits": ["Account", "Credit"],
        "po": ["Reseller SAP ID", "End Customer", "PO", "PO Condition"],
        "consolidation": ["SAP ID", "Condition Creation/ Country"]
    }

    if template not in templates:
        return "Invalid template requested", 400

    # Create CSV in memory
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(templates[template])
    output.seek(0)

    return send_file(
        io.BytesIO(output.getvalue().encode("utf-8")),
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"{template}_template.csv"
    )
    
##----------- SAP to FTP ----------
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/x2cf')
def x2cf():
    return render_template('x2cf.html')

@app.route('/consolidate')
def consolidate():
    return render_template('consolidated.html')



@app.route('/favicon.ico')
def favicon():
    return send_from_directory(
        os.path.join(app.root_path, 'static'),
        'favicon.ico',
        mimetype='image/x-icon'
    )

@app.route('/upload', methods=['POST'])
def upload_file():
    uploaded = request.files.get('file')
    if not uploaded:
        return "No file uploaded.", 400

    raw_bytes = uploaded.read()
    df = pd.read_excel(BytesIO(raw_bytes),
                       engine='openpyxl',
                       dtype=str)

    transformed_df = transform_sap(df)
    
    #csv_bytes = transformed_df.to_csv(index=False,header=False)

    # Use an in-memory buffer instead of writing to disk
    buffer = BytesIO()
    for line in transformed_df["merged"]:
        buffer.write((line + "\n").encode("utf-8"))

    csv_bytes = buffer.getvalue()


    base = uploaded.filename.rsplit('.', 1)[0]
    transformed_name = f"{base}_FTP.csv"

    blob = blob_service_client.get_blob_client(
        container=CONTAINER_NAME,
        blob=transformed_name
    )
    blob.upload_blob(csv_bytes, overwrite=True)

    return jsonify({'download_url': f'/download/{transformed_name}'}), 200

@app.route('/download/<filename>')
def download_file(filename):
    blob_client = blob_service_client.get_blob_client(
        container=CONTAINER_NAME,
        blob=filename
    )
    data = blob_client.download_blob().readall()
    return send_file(
        BytesIO(data),
        as_attachment=True,
        download_name=filename
    )


    
def transform_sap(df: pd.DataFrame) -> pd.DataFrame:

    def smart_quote(val):
        val_str = str(val)
        return f'"{val_str}"' if ',' in val_str else val_str
    
    
    df[["Sale Price","Cost Price"]] = df[["Sale Price","Cost Price"]].apply(pd.to_numeric, errors="coerce").round(2)

 
    header_df = df.iloc[:, :10].drop_duplicates().reset_index(drop=True)

    dup_ids = header_df["Header ID"][header_df["Header ID"].duplicated()].unique()
    if len(dup_ids) > 0:
        raise ValueError(f"Invalid file. Different header with same ID: {list(dup_ids)}")

    line_df   = df.iloc[:, 10:].copy()

    header_df.rename(columns={"Header ID": "ID"}, inplace=True)
    line_df  .rename(columns={"Line ID":   "ID"}, inplace=True)

    header_df.insert(0, "Type", "H")
    line_df  .insert(0, "Type", "L")

    header_df["merged"] = (
    header_df
    .drop(columns="ID")
    .fillna("")
    .applymap(smart_quote)
    .agg(";".join, axis=1)
    )
    
    line_df["merged"] = (
    line_df
    .drop(columns="ID")
    .fillna("")
    .applymap(smart_quote)
    .agg(";".join, axis=1)
    )

    header_out = header_df[["ID", "merged"]]
    line_out   = line_df[["ID", "merged"]]

    rows = []
    for _, hdr in header_out.iterrows():
        rows.append(hdr.to_dict())
        matching = line_out[line_out["ID"] == hdr["ID"]]
        for _, ln in matching.iterrows():
            rows.append(ln.to_dict())

    combined = pd.DataFrame(rows, columns=["merged"])
    return combined

# upload endpoint
@app.route('/x2cf_upload_file', methods=['POST'])
def x2cf_upload_file():
    global dfs
    dfs = []

    files = request.files.getlist('file')
    if not files:
        return jsonify({'error': 'No files uploaded'}), 400

    columns = set()

    try:
        for file in files:
            if file.filename.endswith('.csv'):
                df = pd.read_csv(file, dtype={"Payer Account ID": 'string',
                                              "Cloud Account Number": 'string'})
            elif file.filename.endswith('.xlsx'):
                df = pd.read_excel(file, dtype={"Payer Account ID": 'string',
                                                 "Cloud Account Number": 'string'})
            else:
                return jsonify({'error': f'Invalid file format: {file.filename}'}), 400

            country = file.filename[:2].upper()
            df['Created Country'] = country

            dfs.append(df)
            columns.update(df.columns.tolist())

        return jsonify(sorted(columns))
    except Exception as e:
        app.logger.error("Error during file upload: %s", e)
        return jsonify({'error': 'Failed to process files'}), 500

@app.route('/process', methods=['POST'])
def process_file():
    global dfs
    try:
        group_by_columns = request.form.getlist('group_by')
        aggregations     = request.form.getlist('aggregations')
        order_by_column  = request.form.get('order_by')
        column_order     = request.form.getlist('column_order')

        # build the aggregation dict
        agg_dict = {}
        for item in aggregations:
            if ':' in item:
                col, agg = item.split(':', 1)
                if agg == 'sum':
                    agg_dict[col] = 'sum'

        combined = pd.concat(dfs, ignore_index=True)
        grouped  = combined.groupby(group_by_columns).agg(agg_dict).reset_index()

        # reorder columns if requested
        if column_order:
            grouped = grouped[column_order]

        # sort if requested
        if order_by_column:
            grouped = grouped.sort_values(by=order_by_column)

        # write to excel in-memory
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            grouped.to_excel(writer, index=False)
        output.seek(0)

        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='grouped_data.xlsx'
        )
    except Exception as e:
        app.logger.error("Error during processing: %s", e)
        return jsonify({'error': 'Failed to process data'}), 500


if __name__ == '__main__':
    # Use PORT environment variable if set (Azure App Service assigns it)
    port = int(os.environ.get("PORT", 8000))  # fallback to 8000 for local testing
    app.run(host='0.0.0.0', port=port)
    









