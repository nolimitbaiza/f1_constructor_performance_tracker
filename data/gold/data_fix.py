import pandas as pd

# Load your file
df = pd.read_csv('constructor_monthly.csv')

# Rename columns
df = df.rename(columns={
    'constructor_name': 'ConstructorName',
    'points_m': 'Points',
    'm': 'Date'
})

# Create the Season column from the Date
df['Date'] = pd.to_datetime(df['Date'])
df['Season'] = df['Date'].dt.year

# Format date cleanly
df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

# Save the new file ready for your dashboard
df[['Season', 'Date', 'ConstructorName', 'Points']].to_csv('f1_data.csv', index=False)
