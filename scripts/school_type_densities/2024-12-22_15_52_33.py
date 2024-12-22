import pandas as pd

# Read the CSV file
file_path = r'C:/Users/ncbmk/P3Project/Q2/scha/data/sk_sec_2019.csv'
data = pd.read_csv(file_path)

# Group by 'LEA' and sum 'TOTPUPS'
total_pups_by_lea = data.groupby('LEA', as_index=False)['TOTPUPS'].sum()

# Rename the aggregated column
total_pups_by_lea.rename(columns={'TOTPUPS': 'total_pups'}, inplace=True)

# Save the result to a new CSV
output_path = r'gen/task/2024-12-22_15_52_20/data/total_pups_by_lea.csv'
total_pups_by_lea.to_csv(output_path, index=False)