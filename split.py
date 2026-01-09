import pandas as pd
from sklearn.model_selection import train_test_split

# Load the preprocessed dataset (adjust path if needed)
df = pd.read_csv('Preprocessed_DATASET.csv')

# Display basic info (optional, for verification)
print(df.head())
print(df.info())
print(df['Is_Fraud'].value_counts())  # Check class balance

# Separate features (X) and target (y)
# Drop 'Transaction_Time' if not using it (it's a string; could extract features like hour/weekday)
X = df.drop(['Is_Fraud', 'Transaction_Time'], axis=1)  # Features (exclude target and timestamp)
y = df['Is_Fraud']  # Target

# Split into train (80%) and test (20%) sets
# Use stratify=y to maintain class balance in splits (important for imbalanced data)
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# Verify shapes
print(f"Train set: {X_train.shape} features, {y_train.shape} labels")
print(f"Test set: {X_test.shape} features, {y_test.shape} labels")

# Optional: Save splits to CSV
X_train.to_csv('train_features.csv', index=False)
y_train.to_csv('train_labels.csv', index=False)
X_test.to_csv('test_features.csv', index=False)
y_test.to_csv('test_labels.csv', index=False)