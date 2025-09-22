# Use a slim Python base image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install OS-level deps (if you need any, e.g. for pandas—often none required on slim)
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     build-essential \
# && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all application code (including app.py, templates/, etc.)
COPY . .

# Expose the port Gunicorn will serve on
EXPOSE 8000

# Run the app via Gunicorn with 4 worker processes
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:8000", "app:app"]
