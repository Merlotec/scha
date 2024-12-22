import pandas as pd

# Load the datasets
total_pups_path = 'gen/task/2024-12-22_15_52_20/data/total_pups_by_lea.csv'
target_pups_path = 'gen/task/2024-12-22_15_52_20/data/target_pups_by_lea.csv'

total_pups_df = pd.read_csv(total_pups_path)
target_pups_df = pd.read_csv(target_pups_path)

# Merge the datasets on the 'LEA' column
merged_df = pd.merge(total_pups_df, target_pups_df, on='LEA', how='left')

# Calculate 'target_prop' as the ratio of 'target_pups' to 'total_pups'
merged_df['target_prop'] = merged_df['target_pups'] / merged_df['total_pups']

# Save the final DataFrame to a new CSV file
output_path = 'gen/task/2024-12-22_15_52_20/data/final_lea_summary.csv'
merged_df.to_csv(output_path, index=False)