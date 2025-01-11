import pyarrow.parquet as pq

table = pq.read_table('gs://edit-data-eng-project-group1/LandingZone_converted/carris_lines_data.parquet')
print(f"Number of columns: {len(table.column_names)}")
print(f"Column names: {table.column_names}")
