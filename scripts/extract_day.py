# import helpers
from src.starburst import run_query
from src.cache import cache_name, save_parquet
from src.queries import day_snapshot_sql
import pandas as pd

# define the date we want to pull data for
YEAR, MONTH, DAY = 2025, 7, 30

# build the SQL query
# day_snapshot_sql uses the above parameters to query
# - selects only rows for the given date
# - limits to RHEL major version 10
# - pulls only the latest snapshot per system
SQL = day_snapshot_sql(YEAR, MONTH, DAY, rhel_major=10)

# create a cache file path for the query
out = cache_name(SQL, prefix=f"rhel10_{YEAR:04d}{MONTH:02d}{DAY:02d}")

# defining a filter function to remove systems without a kernel package
def kernel_filter(df: pd.DataFrame) -> pd.DataFrame:
    # create a new column `is_kernel` = true if the package name starts with "kernel"
    has_kernel = (
        df.assign(is_kernel=df["name"].str.match(r"(?i)^kernel"))
        # group by each system (inventory_id) and find if ANY package is a kernel
          .groupby("inventory_id", as_index=False)["is_kernel"].max()
        # rename the True/False column to `has_kernel`
          .rename(columns={"is_kernel": "has_kernel"})
    )
    # merge back into the original dataframe so each row knows if its system has a kernel 
    # keep only rows where `has_kernel` == true and drop the helper column
    return (df.merge(has_kernel, on="inventory_id")
              .query("has_kernel == True")
              .drop(columns=["has_kernel"]))

def main():
    # if out.exists():
    #     print(f"Parquet already exists: {out}")
    #     return
    # run query against Starburst and get the results
    cols, rows = run_query(SQL)

    # load into Pandas DataFram
    df = pd.DataFrame(rows, columns=cols)

    # apply kernel filter
    df = kernel_filter(df)

    # See how many rows match
    kernel_rows = df[df["name"].str.match(r"(?i)^kernel")]
    print(f"Rows where name starts with 'kernel': {len(kernel_rows)}")

    # Look at a sample
    print(kernel_rows[["inventory_id", "name"]].head(20))

    # save to parquet for future use
    # save_parquet(df, out)

    # confirmation log
    print(f"Saved {len(df):,} rows → {out}")

if __name__ == "__main__":
    main()
