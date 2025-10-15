import duckdb

con = duckdb.connect("flights.duckdb")


con.execute("""
CREATE TABLE IF NOT EXISTS flights (
    icao24 VARCHAR,
    callsign VARCHAR,
    origin_country VARCHAR,
    time_position TIMESTAMP,
    last_contact TIMESTAMP,
    longitude DOUBLE,
    latitude DOUBLE,
    baro_altitude DOUBLE,
    on_ground BOOLEAN,
    velocity DOUBLE,
    true_track DOUBLE,
    vertical_rate DOUBLE,
    geo_altitude DOUBLE,
    squawk VARCHAR,
    spi BOOLEAN,
    position_source INTEGER,
    ingestion_time TIMESTAMP
);
""")

con.close()
print("✅ Database and 'flights' table created successfully.")
