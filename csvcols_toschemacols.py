# %% [markdown]
# #### Imports

# %%
from snowpark import *
from snowpark.functions import *

# %% [markdown]
# #### Snowpark session
# 

# %%
session = Session()

# %% [markdown]
# #### Load CSV to df

# %%

df = session.read.format('csv').load('my_file.csv')

# %% [markdown]
# #### Get column orders od csv and table

# %%
csv_column_order = df.columns
target_table_column_order = session.table('my_target_table').columns

# %% [markdown]
# #### Compare column orders and rearrange if needed

# %%
if csv_column_order != target_table_column_order:
    # Rearrange columns in DataFrame
    df = df.select([col(c) for c in target_table_column_order])

# %% [markdown]
# #### Append DataFrame to target table

# %%
df.write.mode('append').saveAsTable('my_target_table')


