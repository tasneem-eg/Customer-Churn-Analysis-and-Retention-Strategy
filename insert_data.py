import duckdb
import pandas as pd
from datetime import datetime

def insert_csv_data(csv_path, db_path="flights.duckdb"):
    con = duckdb.connect(db_path)

    df = pd.read_csv(csv_path, dtype=str)

   
    numeric_cols = [
        "longitude", "latitude", "baro_altitude", "velocity",
        "true_track", "vertical_rate", "geo_altitude", "position_source"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    
    for tcol in ["time_position", "last_contact"]:
        if tcol in df.columns:
            df[tcol] = pd.to_datetime(pd.to_numeric(df[tcol], errors="coerce"), unit="s", errors="coerce")

    
    df["ingestion_time"] = datetime.now()

    con.register("df", df)
    con.execute("INSERT INTO flights SELECT * FROM df")

    con.close()
    print(f"✅ Inserted {len(df)} records from {csv_path} successfully.")

if __name__ == "__main__":
    csv_path = "Flight_Pipeline_Data.csv"
    try:
        insert_csv_data(csv_path)
    except FileNotFoundError:
        print("CSV file not found. Please make sure the file exists.")

