import pandas as pd
import requests

# ==============================
# CONFIGURATION
# ==============================
DYNATRACE_URL = "https://jjd00414.live.dynatrace.com"  # Grail API uses .live.dynatrace.com
API_TOKEN = "dt0c01.7G5NB4FS6RXZA23L2O3EM4KW.O4V7UZML2WJFITHAYFGCD7HUR5FVSU5ZY4J3TQDDJMH7AIAY4GDKZCGKF6NZ7WUS"  # replace with your Dynatrace API token
CSV_FILE = "queries.csv"      # your input CSV with index_name, deployment_name
OUTPUT_FILE = "comparison_results.csv"

# ==============================
# STEP 1: Read CSV
# ==============================
df = pd.read_csv(CSV_FILE)
print("Input Data:")
print(df.head())

# ==============================
# STEP 2: Run Grail Logs API query
# ==============================
headers = {
    "Authorization": f"Api-Token {API_TOKEN}",
    "Content-Type": "application/json"
}

# Grail Logs API endpoint
api_endpoint = f"{DYNATRACE_URL}/platform/logs/v1/query"

# DQL query example
payload = {
    "query": "fetch logs | summarize count() by k8s.cluster.name, k8s.pod.name"
}

response = requests.post(api_endpoint, headers=headers, json=payload)

# Handle response
if response.status_code != 200:
    print("❌ Error fetching Dynatrace data:", response.text)
    dyn_df = pd.DataFrame(columns=["cluster_name", "pod_name"])
else:
    results = response.json()
    try:
        records = results.get("result", [])[0].get("records", [])
        if records:
            dyn_df = pd.json_normalize(records)
            dyn_df = dyn_df.rename(columns={
                "k8s.cluster.name": "cluster_name",
                "k8s.pod.name": "pod_name"
            })
        else:
            print("⚠️ No records found in Dynatrace logs.")
            dyn_df = pd.DataFrame(columns=["cluster_name", "pod_name"])
    except Exception as e:
        print("⚠️ Error parsing Dynatrace response:", e)
        dyn_df = pd.DataFrame(columns=["cluster_name", "pod_name"])

print("Dynatrace Data:")
print(dyn_df.head())

# ==============================
# STEP 3: Compare with CSV
# ==============================
df["cluster_exists"] = df["index_name"].isin(dyn_df["cluster_name"])
df["deployment_exists"] = df["deployment_name"].isin(dyn_df["pod_name"])

# ==============================
# STEP 4: Save results
# ==============================
df.to_csv(OUTPUT_FILE, index=False)
print(f"\n✅ Comparison completed. Results saved to {OUTPUT_FILE}")
