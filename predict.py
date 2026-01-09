import pandas as pd
from sklearn.preprocessing import LabelEncoder, MinMaxScaler
import pickle
import numpy as np

# Load the raw dataset to fit the preprocessors
df_raw = pd.read_csv('DATASET.csv')

# Categorical columns to encode
categorical_cols = [
    'Transaction_ID', 'User_ID', 'Device_Type', 'Location', 
    'Merchant_Category', 'IP_Address', 'Card_Type', 'Authentication_Method'
]

# Fit LabelEncoders
label_encoders = {}
for col in categorical_cols:
    le = LabelEncoder()
    df_raw[col] = le.fit_transform(df_raw[col].astype(str))
    label_encoders[col] = le

# Numerical columns to scale
numerical_cols = [
    'Transaction_Amount', 'Account_Balance', 'Previous_Transaction_Amount', 
    'Daily_transaction_count', 'Avg_Transaction_Amount_Per_Day', 
    'Avg_Transactions_amount_7Day', 'Failed_Transaction_Count_7d', 
    'Card_Age_Months', 'Transaction_Distance_KM'
]

# Fit MinMaxScalers
scalers = {}
for col in numerical_cols:
    scaler = MinMaxScaler()
    df_raw[[col]] = scaler.fit_transform(df_raw[[col]])
    scalers[col] = scaler

# === SAVE ENCODERS AND SCALERS ===
with open('label_encoders.pkl', 'wb') as f:
    pickle.dump(label_encoders, f)

with open('scalers.pkl', 'wb') as f:
    pickle.dump(scalers, f)

print("Saved label_encoders.pkl and scalers.pkl")

# Binary columns (no processing needed, but ensure they are 0/1)
binary_cols = ['IP_Address_Flagged', 'Is_Weekend']

# Load the trained model
with open('random_forest_model.pkl', 'rb') as f:
    model = pickle.load(f)

# Load one of the feature files to get the exact column order for prediction
X_train = pd.read_csv('train_features.csv')
feature_columns = X_train.columns.tolist()

# Function to preprocess new input
def preprocess_new_data(new_data_dict):
    df_new = pd.DataFrame([new_data_dict])
    
    # Encode categorical columns, handle unknown with -1
    for col in categorical_cols:
        try:
            df_new[col] = label_encoders[col].transform(df_new[col].astype(str))
        except ValueError:
            df_new[col] = -1  # Unknown category
    
    # Scale numerical columns
    for col in numerical_cols:
        if col in df_new.columns:
            df_new[[col]] = scalers[col].transform(df_new[[col]])
    
    # Ensure binary columns are 0 or 1
    for col in binary_cols:
        if col in df_new.columns:
            df_new[col] = df_new[col].clip(0, 1)
    
    # Drop Transaction_Time if present
    if 'Transaction_Time' in df_new.columns:
        df_new = df_new.drop(['Transaction_Time'], axis=1)
    
    # Reorder columns to match training features
    df_new = df_new.reindex(columns=feature_columns, fill_value=0)
    
    return df_new

# === Manual Input Prediction ===
print("\nEnter the transaction details manually (raw values):")

transaction_id = input("Transaction_ID (e.g., T00001): ")
user_id = input("User_ID (e.g., U695651): ")
transaction_amount = float(input("Transaction_Amount (e.g., 29445.32): "))
transaction_time = input("Transaction_Time (e.g., 26-05-2025 02:24): ")  # Will be dropped
account_balance = float(input("Account_Balance (e.g., 194165.05): "))
device_type = input("Device_Type (e.g., Tablet/Mobile/Desktop): ")
location = input("Location (e.g., Pimpri-Chinchwad): ")
merchant_category = input("Merchant_Category (e.g., Jewellery): ")
ip_address = input("IP_Address (e.g., 117.108.194.20): ")
ip_address_flagged = int(input("IP_Address_Flagged (0 or 1): "))
previous_transaction_amount = float(input("Previous_Transaction_Amount (e.g., 13852.48): "))
daily_transaction_count = int(input("Daily_transaction_count (e.g., 3): "))
avg_transaction_amount_per_day = float(input("Avg_Transaction_Amount_Per_Day (e.g., 43505.82): "))
avg_transactions_amount_7day = float(input("Avg_Transactions_amount_7Day (e.g., 56390.51): "))
failed_transaction_count_7d = int(input("Failed_Transaction_Count_7d (e.g., 1): "))
card_type = input("Card_Type (e.g., Credit/Debit): ")
card_age_months = int(input("Card_Age_Months (e.g., 8): "))
transaction_distance_km = float(input("Transaction_Distance_KM (e.g., 25.12): "))
authentication_method = input("Authentication_Method (e.g., PIN/OTP): ")
is_weekend = int(input("Is_Weekend (0 or 1): "))

# Create dict with inputs
new_data_dict = {
    'Transaction_ID': transaction_id,
    'User_ID': user_id,
    'Transaction_Amount': transaction_amount,
    'Transaction_Time': transaction_time,
    'Account_Balance': account_balance,
    'Device_Type': device_type,
    'Location': location,
    'Merchant_Category': merchant_category,
    'IP_Address': ip_address,
    'IP_Address_Flagged': ip_address_flagged,
    'Previous_Transaction_Amount': previous_transaction_amount,
    'Daily_transaction_count': daily_transaction_count,
    'Avg_Transaction_Amount_Per_Day': avg_transaction_amount_per_day,
    'Avg_Transactions_amount_7Day': avg_transactions_amount_7day,
    'Failed_Transaction_Count_7d': failed_transaction_count_7d,
    'Card_Type': card_type,
    'Card_Age_Months': card_age_months,
    'Transaction_Distance_KM': transaction_distance_km,
    'Authentication_Method': authentication_method,
    'Is_Weekend': is_weekend
}

# Preprocess the new data
df_new_preprocessed = preprocess_new_data(new_data_dict)

# Make prediction
prediction = model.predict(df_new_preprocessed)[0]
probability = model.predict_proba(df_new_preprocessed)[0][1]  # Probability of fraud (class 1)

# Output the result
print("\n================ Prediction Result ================")
if prediction == 1:
    print(f"FRAUD DETECTED! (Risk Score: {probability*100:.2f}%)")
else:
    print(f"NORMAL TRANSACTION (Fraud Probability: {probability*100:.2f}%)")

# Optional: Test accuracy on test set
"""
X_test = pd.read_csv('test_features.csv')
y_test = pd.read_csv('test_labels.csv')
y_pred_test = model.predict(X_test)
from sklearn.metrics import accuracy_score
print("Model accuracy on test set:", accuracy_score(y_test, y_pred_test))
"""