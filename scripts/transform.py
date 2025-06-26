# %%
import pandas as pd

# Read Data
df = pd.read_csv("../data/raw/deaths.csv", sep=";", )

df = df.drop(columns=["Total"])    

id_vars = ["Categoria"]

df_melted = pd.melt(df,
                    id_vars=id_vars, 
                    var_name="Year", 
                    value_name="Deaths"
                )

# Convert 'Year' to numeric
df_melted["Year"] = df_melted["Year"].astype(int)

print(df_melted)

df_melted.to_csv("../data/processed/deaths_transformed.csv", index=False)

# %%
