import pandas as pd
from get_gic_email_practices import load_practices_data

df = load_practices_data()

# Create the output DataFrame
result_rows = []

# Process 'Individual' groupings
individual_df = df[df['grouping'] == 'Individual']
for _, row in individual_df.iterrows():
    for col in ['primary_email', 'secondary_email']:
        email = row[col]
        if pd.notna(email) and email.strip():
            result_rows.append({
                'recipient': email.strip(),
                'grouping': row['grouping'],
                'tins_received': row['tin'],
                'practice_names': row['practice_name'],
                'bcc': row['bcc'],
            })

# Process 'Combined TIN' groupings
combined_df = df[df['grouping'] == 'Combined TIN']
grouped_combined = combined_df.groupby('primary_email')
for email, group in grouped_combined:
    combined_tins = ', '.join(group['tin'].unique())
    combined_practices = ', '.join(group['practice_name'].unique())
    combined_bcc = ', '.join(group['bcc'].dropna().unique())

    if pd.notna(email) and email.strip():  # type: ignore
        result_rows.append({
            'recipient': email.strip(),  # type: ignore
            'grouping': 'Combined TIN',
            'tins_received': combined_tins,
            'practice_names': combined_practices,
            'bcc': combined_bcc,
        })

    for _, row in group.iterrows():
        for col in ['secondary_email']:
            secondary = row[col]
            if pd.notna(secondary) and secondary.strip():
                result_rows.append({
                    'recipient': secondary.strip(),
                    'grouping': 'Combined TIN',
                    'tins_received': combined_tins,
                    'practice_names': combined_practices,
                    'bcc': combined_bcc,
                })

# Process 'Practice Group' groupings
practice_group_df = df[df['grouping'] == 'Practice Group']
grouped_practice = practice_group_df.groupby('primary_email')
for email, group in grouped_practice:
    combined_tins = ', '.join(group['tin'].unique())
    combined_practices = ', '.join(group['practice_name'].unique())
    combined_bcc = ', '.join(group['bcc'].dropna().unique())

    if pd.notna(email) and email.strip():  # type: ignore
        result_rows.append({
            'recipient': email.strip(),  # type: ignore
            'grouping': 'Practice Group',
            'tins_received': combined_tins,
            'practice_names': combined_practices,
            'bcc': combined_bcc,
        })

    for _, row in group.iterrows():
        for col in ['secondary_email']:
            secondary = row[col]
            if pd.notna(secondary) and secondary.strip():
                result_rows.append({
                    'recipient': secondary.strip(),
                    'grouping': 'Practice Group',
                    'tins_received': row['tin'],
                    'practice_names': row['practice_name'],
                    'bcc': row['bcc'],
                })

# Convert to DataFrame
output_df = pd.DataFrame(result_rows)
output_df.sort_values(by='recipient', inplace=True)
output_df.reset_index(drop=True, inplace=True)
output_df.drop_duplicates(inplace=True)
output_df.to_csv(
    "/home/etl/etl_home/downloads/transformed_quality_emails.csv", index=False)
