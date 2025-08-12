from src.starburst import run_query

cols, rows = run_query("SELECT current_catalog, current_schema")
print(cols, rows)
