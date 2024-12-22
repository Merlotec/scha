import pandas as pd

# Read the CSV file
file_path = r'C:/Users/ncbmk/P3Project/Q2/scha/data/sk_sec_2019.csv'
data = pd.read_csv(file_path)

# Define the list of NFTYPEs to filter
nftype_list = ["AC", "ACC", "AC1619", "ACC1619", "CY", "F1619", "FSS", "F", "FD", "VA", "VC"]

# Filter schools based on criteria
filtered_data = data[(data['NFTYPE'].isin(nftype_list)) & (data['ADMPOL'] != 'SEL')]

# Group by 'LEA' and sum 'TOTPUPS'
target_pups_by_lea = filtered_data.groupby('LEA', as_index=False)['TOTPUPS'].sum()

# Rename the aggregated column
target_pups_by_lea.rename(columns={'TOTPUPS': 'target_pups'}, inplace=True)

# Save the result to a new CSV
output_path = r'gen/task/2024-12-22_15_52_20/data/target_pups_by_lea.csv'
target_pups_by_lea.to_csv(output_path, index=False)