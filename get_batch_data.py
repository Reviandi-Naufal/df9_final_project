import pandas as pd

def batch_data(df):
    max_value = df['step'].max()
    min_value = df['step'].min()
    for counter in range(min_value, max_value+1):
        df_save = df[df['step'] == counter]
        df_save.to_parquet(f'Dataset/cleaned_dataset/online_payment_{counter}.parquet')
        print(f'data {counter} successfully saved')