import sys
import pandas as pd
print("arguments", sys.argv)

month = int(sys.argv[1])
print(f"Running pipeline for month {month}")

df = pd.DataFrame({"day": [1, 2], "no_of_passsengers": [3, 4]})
df['month']=month
print(df.head())

df.to_parquet(f"output_day_{sys.argv[1]}.parquet")