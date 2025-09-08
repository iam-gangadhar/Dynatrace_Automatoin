import pandas as pd
import requests

# ==============================
# CONFIGURATION
# ==============================
DYNATRACE_URL = "https://jjd00414.live.dynatrace.com"  # your tenant
API_TOKEN = "dt0c01.7G5NB4FS6RXZA23L2O3EM4KW.O4V7UZML2WJFITHAYFGCD7HUR5FVSU5ZY4J3TQDDJMH7AIAY4GDKZCGKF6NZ7WUS"
CSV_FILE = "queries.csv"  # input CSV with cluster_name, pod_name
OUTPUT_FILE = "comparison_results.csv"

# ==============================
# STEP 1: Read CSV
# ==============================
df = pd.read_csv(CSV_FILE)
print("Input Data:")
print(df.head())

# ==============================
# STEP 2: Fetch metrics with dimensions
# ==============================
headers = {
    "Authorization": f"Api-Token {API_TOKEN}"
}

# We'll get the last 1 hour of pod metrics
params = {
    "metricSelector": "builtin:kubernetes.pods.running:splitBy(k8s.cluster.name,k8s.pod.name)",
    "resolution": "Inf",
    "from": "now-1h",
    "to": "now"
}

response = requests.get(f"{DYNATRACE_URL}/api/v2/metrics/query", headers=headers, params=params)

if response.status_code != 200:
    print("❌ Error fetching Dynatrace metrics:", response.text)
    exit(1)

results = response.json()

# ==============================
# STEP 3: Extract cluster and pod names
# ==============================
records = []
for series in results.get("result", []):
    dimensions = series.get("dimensions", [])
    if len(dimensions) == 2:
        cluster, pod = dimensions
        records.append({"cluster_name": cluster, "pod_name": pod})

dyn_df = pd.DataFrame(records)
print("Dynatrace Metrics Data:")
print(dyn_df.head())

# ==============================
# STEP 4: Compare with CSV
# ==============================
df["cluster_exists"] = df["cluster_name"].isin(dyn_df["cluster_name"])
df["pod_exists"] = df["pod_name"].isin(dyn_df["pod_name"])

