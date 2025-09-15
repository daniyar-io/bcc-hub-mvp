import pandas as pd
from sklearn.model_selection import train_test_split

df = pd.read_csv("data/processed/clients_full.csv")

open_set, hidden_set = train_test_split(df, test_size=0.2, random_state=42)

open_set.to_csv("data/processed/open_test.csv", index=False)
hidden_set.to_csv("data/processed/hidden_test.csv", index=False)

print("✅ open_test.csv и hidden_test.csv созданы")