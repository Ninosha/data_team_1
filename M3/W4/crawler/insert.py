# table = client.get_table(table_id)
 # schema = table.schema
#
# job_config = bigquery.LoadJobConfig(
#     source_format=bigquery.SourceFormat.CSV,
#     skip_leading_rows=1,
#     schema=schema,
# )
#
# with open(file_url, "rb") as source_file:
#     job = client.load_table_from_file(source_file, table_id,
#                                       job_config=job_config)
#
# job.result()  # Waits for the job to complete.
#
