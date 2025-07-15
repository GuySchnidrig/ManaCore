import duckdb

# Connect to a DuckDB file (creates the file if it doesn't exist)
con = duckdb.connect('G:/My Drive/MTG/ManaCore/data/database/vintage-cube.duckdb')



con.close()