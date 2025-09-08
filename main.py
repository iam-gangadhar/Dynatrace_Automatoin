import pandas as pd
import requests

# ==============================
# CONFIGURATION
# ==============================
DYNATRACE_URL = "https://jjd00414.apps.dynatrace.com/"  # replace with your Dynatrace tenant
API_TOKEN = "dt0c01.7G5NB4FS6RXZA23L2O3EM4KW.O4V7UZML2WJFITHAYFGCD7HUR5FVSU5ZY4J3TQDDJMH7AIAY4GDKZCGKF6NZ7WUS"  # replace with your Dynatrace API token
CSV_FILE = "queries.csv"  # your input CSV with index_name, deployment_name
OUTPUT_FILE = "comparison_results.csv"

# ==============================
# STEP 1: Read CSV
# ==============================
df = pd.read_csv(CSV_FILE)
print("Input Data:")
print(df.head())

# ==============================
# STEP 2: Run Dynatrace DQL query
# ==============================
headers = {
    "Authorization": f"Api-Token {API_TOKEN}",
    "Content-Type": "application/json"
}

# Example query to fetch clusters and pods from logs
dql_query = {
    "query": "fetch logs | summarize count() by k8s.cluster.name, k8s.pod.name"
}

response = requests.post(
    f"{DYNATRACE_URL}/api/v2/logs/query",
    headers=headers,
    json=dql_query
)

if response.status_code != 200:
    print("Error fetching Dynatrace data:", response.text)
    exit(1)

results = response.json()

# ==============================
# STEP 3: Convert API response to DataFrame
# ==============================
try:
    dyn_df = pd.json_normalize(results["result"][0]["records"])
    dyn_df = dyn_df.rename(columns={
        "k8s.cluster.name": "cluster_name",
        "k8s.pod.name": "pod_name"
    })
    print("Dynatrace Data:")
    print(dyn_df.head())
except Exception as e:
    print("Error parsing Dynatrace response:", e)
    exit(1)

# ==============================
# STEP 4: Compare with CSV
# ==============================
df["cluster_exists"] = df["index_name"].isin(dyn_df["cluster_name"])
df["deployment_exists"] = df["deployment_name"].isin(dyn_df["pod_name"])

# ==============================
# STEP 5: Save results
# ==============================
df.to_csv(OUTPUT_FILE, index=False)
print(f"\nComparison completed. Results saved to {OUTPUT_FILE}")
