import pandas as pd
orig_df=pd.read_csv("train_data.csv")
new_df=orig_df['industry']
new_df.to_csv("CondemnJava.csv",index=False,header=False)