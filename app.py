import os
from io import BytesIO
from flask import Flask, request, send_file, jsonify, render_template, send_from_directory
import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from awstool import run_awstool, apply_credit_adjustments,apply_po_adjustments,apply_consolidation_adjustments,apply_exception,consolidation
from awstool import last_country, last_start_date, last_end_date  # import globals
import csv

load_dotenv() 

app = Flask(__name__)

# Configuration via environment variables
STORAGE_ACCOUNT_URL = os.environ.get("STORAGE_ACCOUNT_URL")
CONTAINER_NAME      = os.environ.get("CONTAINER_NAME")

# Initialize blob service client
blob_service_client = BlobServiceClient(
    account_url=STORAGE_ACCOUNT_URL,
    credential=DefaultAzureCredential()
)


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/x2cf')
def x2cf():
    return render_template('x2cf.html')

# Track progress across steps
progress_flags = {
    "step1_done": False,
    "step2a_done": False,
    "step2b_done": False,
    "step2c_done": False,
    "step2d_done": False,
    "step3_done": False,
    "step4_done": False,
}


# ---------- STEP 1 ----------
@app.route("/awstool", methods=["GET", "POST"])
def awstool():
    result = None

    # Reset flags on GET
    if request.method == "GET":
        for key in progress_flags:
            progress_flags[key] = False

            
    if request.method == "POST":
        country = request.form.get("country")
        start_date = request.form.get("start_date")
        end_date = request.form.get("end_date")

        result = run_awstool(country, start_date, end_date)
        if "error" not in result:
            progress_flags["step1_done"] = True

    return render_template("awstool.html", result=result, **progress_flags)

# ---------- STEP 2b ----------
@app.route("/upload_credits", methods=["POST"])
def upload_credits():
    if "file" not in request.files:
        return render_template("awstool.html", result={"error": "No file uploaded"}, **progress_flags)

    file = request.files["file"]
    if file.filename == "":
        return render_template("awstool.html", result={"error": "No file selected"}, **progress_flags)

    result = apply_credit_adjustments(file)
    if "error" not in result:
        progress_flags["step2b_done"] = True

    return render_template("awstool.html", result=result, **progress_flags)


# ---------- STEP 2a ----------
@app.route("/upload_exception", methods=["POST"])
def upload_exception():
    if "file" not in request.files:
        return render_template("awstool.html", result={"error": "No file uploaded"}, **progress_flags)

    file = request.files["file"]
    if file.filename == "":
        return render_template("awstool.html", result={"error": "No file selected"}, **progress_flags)

    result = apply_exception(file)
    if "error" not in result:
        progress_flags["step2a_done"] = True

    return render_template("awstool.html", result=result, **progress_flags)



# ---------- STEP 3 ----------
@app.route("/consolidation", methods=["GET", "POST"])
def run_consolidation():
    result = consolidation()
    if "error" not in result:
        progress_flags["step3_done"] = True

    return render_template("awstool.html", result=result, **progress_flags)


# ---------- STEP 2c ----------
@app.route("/upload_po", methods=["POST"])
def upload_po():
    if "file" not in request.files:
        return render_template("awstool.html", result={"error": "No file uploaded"}, **progress_flags)

    file = request.files["file"]
    if file.filename == "":
        return render_template("awstool.html", result={"error": "No file selected"}, **progress_flags)

    result = apply_po_adjustments(file)
    if "error" not in result:
        progress_flags["step2c_done"] = True

    return render_template("awstool.html", result=result, **progress_flags)



# ---------- STEP 2d ----------
@app.route("/upload_consolidation", methods=["POST"])
def upload_consolidation():
    if "file" not in request.files:
        return render_template("awstool.html", result={"error": "No file uploaded"}, **progress_flags)

    file = request.files["file"]
    if file.filename == "":
        return render_template("awstool.html", result={"error": "No file selected"}, **progress_flags)

    result = apply_consolidation_adjustments(file)
    if "error" not in result:
        progress_flags["step2d_done"] = True

    return render_template("awstool.html", result=result, **progress_flags)


# ---------- STEP 4 ----------
@app.route("/download_csv")
def download_csv():
    from awstool import last_country, last_start_date, last_end_date  

    try:
        # Format dates safely
        start_fmt = last_start_date.replace("-", "") if last_start_date else "unknown"
        end_fmt   = last_end_date.replace("-", "") if last_end_date else "unknown"
        country   = last_country if last_country else "unknown"

        filename = f"AWS_Billing_Report_{country}_from_{start_fmt}_to_{end_fmt}.csv"

        return send_file(
            "latest_report.csv",
            mimetype="text/csv",
            as_attachment=True,
            download_name=filename
        )
    except FileNotFoundError:
        return "No report available to download", 400

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
    








