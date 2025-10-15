import duckdb
import pandas as pd

DB_PATH = "flights.duckdb"

def get_total_flights():
    con = duckdb.connect(DB_PATH)
    result = con.execute("SELECT COUNT(*) AS total_flights FROM flights").fetchdf()
    con.close()
    return result

def get_top_countries(limit=5):
    con = duckdb.connect(DB_PATH)
    query = f"""
        SELECT origin_country, COUNT(*) AS flight_count
        FROM flights
        GROUP BY origin_country
        ORDER BY flight_count DESC
        LIMIT {limit}
    """
    result = con.execute(query).fetchdf()
    con.close()
    return result

def get_avg_velocity():
    con = duckdb.connect(DB_PATH)
    result = con.execute("""
        SELECT AVG(velocity) AS avg_velocity
        FROM flights
        WHERE velocity IS NOT NULL
    """).fetchdf()
    con.close()
    return result

def get_on_ground_ratio():
    con = duckdb.connect(DB_PATH)
    result = con.execute("""
        SELECT
            SUM(CASE WHEN on_ground THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS on_ground_percent
        FROM flights
    """).fetchdf()
    con.close()
    return result


if __name__ == "__main__":
    print("Total Flights:")
    print(get_total_flights(), "\n")

    print("Top Origin Countries:")
    print(get_top_countries(), "\n")

    print("Average Velocity:")
    print(get_avg_velocity(), "\n")

    print("Flights On Ground (%):")
    print(get_on_ground_ratio(), "\n")
