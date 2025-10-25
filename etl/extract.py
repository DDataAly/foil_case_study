import os
import pandas as pd
from ucimlrepo import fetch_ucirepo

# Creates data folder for the original dataset
os.makedirs("data", exist_ok=True)

# The dataset is a custom Python object defined in ucimlrepo package
# It has data attribute with data which contains dataset matrices as pandas dataframes
# data.original is a dataframe consisting of all IDs, features, and targets
online_retail = fetch_ucirepo(id=352)
df = online_retail.data.original

# Saves a local copy in the data folder
save_path = "data/online_retail_raw.csv"
df.to_csv(save_path, index=False)

print(f"Dataset saved to: {save_path}")
print(f"Shape: {df.shape}") # Returns (541909,8)
