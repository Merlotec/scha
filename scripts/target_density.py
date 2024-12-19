import pandas as pd

# Load the dataset
file_path = '/home/ncbmk/dev/gsp/scha/radius/msoa_popn_age.csv'
data = pd.read_csv(file_path)

# Ensure population columns are read as integers
data.iloc[:, 6:] = data.iloc[:, 6:].replace(',', '', regex=True).astype(int)

# Sum the population for ages 11 to 18
school_age_population = data.loc[:, '11':'18'].sum(axis=1)

# Calculate the proportion of school-aged children
all_ages_population = data['All Ages'].replace(',', '', regex=True).astype(int)
data['msoa_target_proportion'] = school_age_population / all_ages_population

# Define the output path
output_path = 'msoa_popn_with_target_proportion.csv'

# Save the modified dataset
data.to_csv(output_path, index=False)

msoa_data_path = 'msoa_popn_with_target_proportion.csv'
school_data_path = 'sk_adm_log_cl.csv'
msoa_data = pd.read_csv(msoa_data_path)
school_data = pd.read_csv(school_data_path)

# Merge the datasets on 'MSOA Code' and 'msoa21cd'
merged_data = school_data.merge(msoa_data[['MSOA Code', 'msoa_target_proportion']], how='left', left_on='msoa21cd', right_on='MSOA Code')

# Calculate the target density
merged_data['target_density'] = merged_data['density'] * merged_data['msoa_target_proportion']

# Output the modified school dataset
output_path = 'sk_adm_with_target_density.csv'
merged_data.to_csv(output_path, index=False)
