import pandas as pd

# Sample data
data = {
    "Date": ["2023-11-15", "2023-11-15", "2023-11-16", "2023-11-16"],
    "Hour": [1, 2, 1, 2],
    "Value": [100, 100, 300, 400]
}

df = pd.DataFrame(data)
print("Original DataFrame:")
print(df.index)

# Pivoting the data
pivoted_df = df.pivot(index="Date", columns="Hour", values="Value")
print("\nPivoted DataFrame:")
print(pivoted_df)