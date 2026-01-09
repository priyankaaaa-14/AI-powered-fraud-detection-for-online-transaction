#!/usr/bin/env python3
"""
generate_user_to_mongo.py (robust bulk-upsert version)

- Reads DATASET.csv, groups by User_ID and upserts one document per user into MongoDB.
- Uses bulk_write in batches to avoid network timeouts / operation cancelled errors.
- Ensures all values are native Python types (no numpy / pandas types).
- Retries bulk_write on transient errors and logs problematic user_ids.
- Excludes per-transaction Is_Weekend / Is_Fraud as requested; keeps per-user Is_Fraud aggregate.

Usage:
    python generate_user_to_mongo.py

Make sure MongoDB is running and DATASET.csv path is correct.
"""
import os
import sys
import time
import random
import string
import hashlib
import pandas as pd
from pymongo import MongoClient, UpdateOne, errors
from tqdm import tqdm

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATASET_PATH = os.path.join(BASE_DIR, "DATASET.csv")  # adjust if needed

# Mongo settings - increase timeouts for long bulk operations
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "fraud_detection_db"
USERS_COLLECTION = "users"

# Bulk parameters
BATCH_SIZE = 1000
MAX_RETRIES = 3
RETRY_BACKOFF = 3  # seconds

# Helpers
def sha256_hash(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()

def rand_phone():
    return "+91" + "".join(random.choices("0123456789", k=10))

def rand_card():
    num = "".join(random.choices("0123456789", k=16))
    masked = "************" + num[-4:]
    return num, masked

def default_secret_key(length=8):
    chars = string.ascii_letters + string.digits
    return "".join(random.choices(chars, k=length))

first_names = ["Aarav","Vivaan","Aditya","Vihaan","Krishna","Ishaan","Rohan","Aryan","Dhruv","Kartik"]
last_names = ["Patel","Sharma","Gupta","Reddy","Nair","Singh","Kumar","Das","Mehta","Rao"]
def random_name():
    return f"{random.choice(first_names)} {random.choice(last_names)}"

# Safe type conversions to native Python
def to_str(x, default=""):
    if pd.isna(x):
        return default
    return str(x)

def to_int(x, default=0):
    try:
        if pd.isna(x):
            return default
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return default

def to_float(x, default=0.0):
    try:
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default

def ensure_txn_dict(row):
    """
    Build a transaction dict with safe typing.
    Excludes per-transaction Is_Weekend and Is_Fraud (as requested).
    """
    txn = {
        "Transaction_ID": to_str(row.get("Transaction_ID", "")),
        "User_ID": to_str(row.get("User_ID", "")),
        "Transaction_Amount": to_float(row.get("Transaction_Amount", 0.0)),
        "Transaction_Time": to_str(row.get("Transaction_Time", "")),
        "Account_Balance": to_float(row.get("Account_Balance", 0.0)),
        "Device_Type": to_str(row.get("Device_Type", "")),
        "Location": to_str(row.get("Location", "")),
        "Merchant_Category": to_str(row.get("Merchant_Category", "")),
        "IP_Address": to_str(row.get("IP_Address", "")),
        "IP_Address_Flagged": to_int(row.get("IP_Address_Flagged", 0)),
        "Previous_Transaction_Amount": to_float(row.get("Previous_Transaction_Amount", 0.0)),
        "Daily_transaction_count": to_int(row.get("Daily_transaction_count", 0)),
        "Avg_Transaction_Amount_Per_Day": to_float(row.get("Avg_Transaction_Amount_Per_Day", 0.0)),
        "Avg_Transactions_amount_7Day": to_float(row.get("Avg_Transactions_amount_7Day", 0.0)),
        "Failed_Transaction_Count_7d": to_int(row.get("Failed_Transaction_Count_7d", 0)),
        "Card_Type": to_str(row.get("Card_Type", "")),
        "Card_Age_Months": to_int(row.get("Card_Age_Months", 0)),
        "Transaction_Distance_KM": to_float(row.get("Transaction_Distance_KM", 0.0)),
        "Authentication_Method": to_str(row.get("Authentication_Method", "")),
    }
    return txn

def build_user_doc(user_id, g_sorted):
    """
    Given a DataFrame (sorted descending by time) for one user, build the user document.
    """
    latest = g_sorted.iloc[0]
    card_num, masked_card = rand_card()
    secret_key_plain = default_secret_key(8)
    password_plain = "Password123"  # default demo password

    # recent transactions (up to 10)
    recent_txns = []
    for _, r in g_sorted.head(10).iterrows():
        recent_txns.append(ensure_txn_dict(r))

    user_doc = {
        "User_ID": to_str(user_id),
        "user_id": to_str(user_id),
        "name": random_name(),
        "phone_number": rand_phone(),
        "location": to_str(latest.get("Location", "")),
        "password_hash": sha256_hash(password_plain),
        "secret_key_hash": sha256_hash(secret_key_plain),
        "account_summary": {
            "Total_Balance": to_float(latest.get("Account_Balance", 0.0)),
            "Spend_Analysis": {
                "Inflow": to_float(latest.get("Avg_Transaction_Amount_Per_Day", 0.0)),
                "Outflow": to_float(latest.get("Avg_Transactions_amount_7Day", 0.0))
            },
            "Card_Age_Months": to_int(latest.get("Card_Age_Months", 0) or 0),
            "Card_Number": masked_card
        },
        "recent_transactions": recent_txns,
        # user-level Is_Fraud aggregate: 1 if any transaction has Is_Fraud == 1
        "Is_Fraud": 1 if to_int(g_sorted["Is_Fraud"].astype(int).sum(), 0) > 0 else 0,
        "demo_plain_password": password_plain,
        "demo_plain_secret": secret_key_plain
    }
    return user_doc

def main():
    if not os.path.exists(DATASET_PATH):
        print(f"[ERROR] DATASET.csv not found at {DATASET_PATH}")
        sys.exit(1)

    print("[INFO] Loading dataset... (this may take some time for large files)")
    # Use low_memory=False to avoid dtype warning; strip column names
    df = pd.read_csv(DATASET_PATH, low_memory=False)
    df.columns = df.columns.str.strip()

    # required columns (Is_Weekend intentionally excluded)
    required_cols = [
        "Transaction_ID","User_ID","Transaction_Amount","Transaction_Time","Account_Balance",
        "Device_Type","Location","Merchant_Category","IP_Address","IP_Address_Flagged",
        "Previous_Transaction_Amount","Daily_transaction_count","Avg_Transaction_Amount_Per_Day",
        "Avg_Transactions_amount_7Day","Failed_Transaction_Count_7d","Card_Type","Card_Age_Months",
        "Transaction_Distance_KM","Authentication_Method","Is_Fraud"
    ]
    for c in required_cols:
        if c not in df.columns:
            print(f"[ERROR] Missing required column in dataset: {c}")
            sys.exit(1)

    # parse transaction time to enable sorting. We won't fail on parse errors.
    # Use errors='coerce' so unparsable values become NaT
    try:
        df["__t"] = pd.to_datetime(df["Transaction_Time"], dayfirst=True, errors="coerce")
    except Exception:
        # fallback: let pandas try with default parsing per-row
        df["__t"] = pd.to_datetime(df["Transaction_Time"], errors="coerce")

    # sort within each group when we build g_sorted

    # Create MongoClient with elevated timeouts for large bulk ops
    client = MongoClient(MONGO_URI,
                         serverSelectionTimeoutMS=20000,
                         socketTimeoutMS=120000,
                         connectTimeoutMS=20000)
    db = client[DB_NAME]
    col = db[USERS_COLLECTION]

    bulk_ops = []
    total_users = 0
    failed_users = []
    op_count = 0
    start_time = time.time()

    # iterate groups and prepare UpdateOne ops
    grouped = df.groupby("User_ID")
    user_iter = grouped

    print("[INFO] Preparing bulk operations...")
    for user_id, g in tqdm(user_iter, unit="users"):
        try:
            # sort copy by parsed time (newest first), fallback to original order if __t is NaT
            g_sorted = g.copy()
            if "__t" in g_sorted.columns:
                g_sorted = g_sorted.sort_values("__t", ascending=False, na_position='last')
            else:
                g_sorted = g_sorted

            user_doc = build_user_doc(user_id, g_sorted)

            # Create an UpdateOne operation to upsert the user document
            op = UpdateOne({"User_ID": user_doc["User_ID"]}, {"$set": user_doc}, upsert=True)
            bulk_ops.append(op)
            op_count += 1
            total_users += 1

            # execute batch when reached BATCH_SIZE
            if len(bulk_ops) >= BATCH_SIZE:
                success = False
                attempt = 0
                while not success and attempt < MAX_RETRIES:
                    try:
                        result = col.bulk_write(bulk_ops, ordered=False)
                        success = True
                        # reset
                        bulk_ops = []
                    except errors.BulkWriteError as bwe:
                        attempt += 1
                        print(f"[WARN] BulkWriteError on attempt {attempt}: {bwe.details}")
                        time.sleep(RETRY_BACKOFF)
                    except (errors.AutoReconnect, errors.NetworkTimeout, errors.PyMongoError) as e:
                        attempt += 1
                        print(f"[WARN] Transient mongo error on bulk_write attempt {attempt}: {e}")
                        time.sleep(RETRY_BACKOFF)
                if not success:
                    print("[ERROR] Bulk write failed after retries. Recording failed batch and continuing.")
                    # log and continue - try to upsert individually to identify bad users
                    for single_op in bulk_ops:
                        uid = single_op._filter.get("User_ID", "unknown_user")
                        try:
                            col.update_one(single_op._filter, single_op._doc, upsert=True)
                        except Exception as ex:
                            print(f"[ERROR] Failed upsert for user {uid}: {ex}")
                            failed_users.append(uid)
                    bulk_ops = []

        except Exception as e:
            uid_str = str(user_id)
            print(f"[ERROR] Exception while preparing user {uid_str}: {e}")
            failed_users.append(uid_str)
            continue

    # flush any remaining operations
    if bulk_ops:
        success = False
        attempt = 0
        while not success and attempt < MAX_RETRIES:
            try:
                result = col.bulk_write(bulk_ops, ordered=False)
                success = True
            except errors.BulkWriteError as bwe:
                attempt += 1
                print(f"[WARN] BulkWriteError on final batch attempt {attempt}: {bwe.details}")
                time.sleep(RETRY_BACKOFF)
            except (errors.AutoReconnect, errors.NetworkTimeout, errors.PyMongoError) as e:
                attempt += 1
                print(f"[WARN] Transient mongo error on final bulk_write attempt {attempt}: {e}")
                time.sleep(RETRY_BACKOFF)
        if not success:
            print("[ERROR] Final bulk write failed after retries; attempting per-item writes.")
            for single_op in bulk_ops:
                uid = single_op._filter.get("User_ID", "unknown_user")
                try:
                    col.update_one(single_op._filter, single_op._doc, upsert=True)
                except Exception as ex:
                    print(f"[ERROR] Failed final upsert for user {uid}: {ex}")
                    failed_users.append(uid)
            bulk_ops = []

    elapsed = time.time() - start_time
    print(f"[OK] Completed upsert of {total_users} users in {elapsed:.1f}s. Failed users: {len(failed_users)}")
    if failed_users:
        print("[WARN] Failed user list (first 20):", failed_users[:20])

if __name__ == "__main__":
    main()
