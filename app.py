#!/usr/bin/env python3
"""
Updated backend with exact rule set per user's specification.
- Loads  model pickles if present (random_forest_model.pkl, label_encoders.pkl, scalers.pkl).
- Applies deterministic rule-based fraud probabilities per cases provided.
- If model exists, final_score = max(rule_score, model_score) (conservative).
"""
import os, uuid, hashlib, random, string
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from pymongo import MongoClient
import pickle
import pandas as pd

# CONFIG
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "fraud_detection_db"
USERS_COL = "users"
RESET_OTP_TTL_SECONDS = 10
TRANSFER_OTP_TTL_SECONDS = 20

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FRONTEND_DIR = os.path.join(BASE_DIR, "..", "frontend")
FRONTEND_DIR = os.path.abspath(FRONTEND_DIR)

app = Flask(__name__, static_folder=FRONTEND_DIR, static_url_path="/")
CORS(app)

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
users = db[USERS_COL]

ALLOWED_LOCATIONS = [
    "Pimpri-Chinchwad","Hyderabad","Ahmedabad","Bengaluru","Bhopal","Chennai",
    "Delhi","Indore","Kanpur","Kolkata","Lucknow","Mumbai","Nagpur","Surat",
    "Vadodara","Visakhapatnam","Patna","Jaipur","Thane","Pune"
]

# HELPERS
def sha256_hash(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()

def gen_otp(length=6):
    return "".join(random.choices(string.digits, k=length))

def create_session_token():
    return str(uuid.uuid4())

def find_user(user_id):
    return users.find_one({"User_ID": user_id})

# MODEL LOADING (optional)
def safe_load_pickle(path):
    if os.path.exists(path):
        try:
            with open(path, "rb") as f:
                return pickle.load(f)
        except Exception as e:
            print(f"[WARN] Failed to load {path}: {e}")
    return None

fraud_model = safe_load_pickle(os.path.join(BASE_DIR, "random_forest_model.pkl"))
label_encoders = safe_load_pickle(os.path.join(BASE_DIR, "label_encoders.pkl")) or {}
scalers = safe_load_pickle(os.path.join(BASE_DIR, "scalers.pkl")) or {}

def preprocess_new_data(txn_dict: dict) -> pd.DataFrame:
    df = pd.DataFrame([txn_dict])
    for col, enc in label_encoders.items():
        if col in df.columns:
            vals = df[col].astype(str).tolist()
            classes = getattr(enc, "classes_", [])
            table = {c: i for i, c in enumerate(classes)}
            df[col] = [table.get(v, -1) for v in vals]
    for col, sc in scalers.items():
        if col in df.columns:
            try:
                df[[col]] = sc.transform(df[[col]])
            except Exception:
                pass
    if "Transaction_Time" in df.columns:
        df = df.drop(["Transaction_Time"], axis=1)
    return df

# STATIC PAGES
@app.route("/")
def index():
    return send_from_directory(FRONTEND_DIR, "index.html")

@app.route("/dashboard.html")
def dashboard_html():
    return send_from_directory(FRONTEND_DIR, "dashboard.html")

# AUTH
@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.json or {}
    user_id = data.get("user_id", "").strip()
    password = data.get("password", "")
    if not user_id or not password:
        return jsonify({"ok": False, "msg": "Provide user_id and password"}), 400
    u = find_user(user_id)
    if not u:
        return jsonify({"ok": False, "msg": "User not found"}), 404
    if u.get("password_hash") != sha256_hash(password):
        return jsonify({"ok": False, "msg": "Invalid credentials"}), 401

    token = create_session_token()
    expiry = datetime.utcnow() + timedelta(hours=2)
    users.update_one({"User_ID": user_id}, {"$set": {"session_token": token, "session_expiry": expiry}})

    payload = {
        "token": token,
        "User_ID": u["User_ID"],
        "name": u.get("name"),
        "phone_number": u.get("phone_number"),
        "location": u.get("location"),
        "account_summary": u.get("account_summary", {}),
        "recent_transactions": u.get("recent_transactions", [])
    }

    return jsonify({"ok": True, "data": payload})

# SESSION VALIDATION
def validate_session(token):
    if not token:
        return None
    u = users.find_one({"session_token": token})
    if not u:
        return None
    if "session_expiry" in u and u["session_expiry"] < datetime.utcnow():
        users.update_one({"_id": u["_id"]}, {"$unset": {"session_token": "", "session_expiry": ""}})
        return None
    return u

# OTP endpoints (unchanged)
@app.route("/api/request-otp", methods=["POST"])
def api_request_otp():
    data = request.json or {}
    user_id = data.get("user_id", "").strip()
    if not user_id:
        return jsonify({"ok": False, "msg": "Provide user_id"}), 400
    u = find_user(user_id)
    if not u:
        return jsonify({"ok": False, "msg": "User not found"}), 404
    otp = gen_otp(6)
    expiry = datetime.utcnow() + timedelta(seconds=RESET_OTP_TTL_SECONDS)
    users.update_one({"User_ID": user_id}, {"$set": {"reset_otp": otp, "reset_otp_expiry": expiry}})
    return jsonify({"ok": True, "msg": "OTP generated (demo)", "otp": otp, "ttl_seconds": RESET_OTP_TTL_SECONDS})

@app.route("/api/verify-otp", methods=["POST"])
def api_verify_otp():
    data = request.json or {}
    user_id = data.get("user_id", "").strip()
    otp = data.get("otp", "").strip()
    if not user_id or not otp:
        return jsonify({"ok": False, "msg": "Provide user_id and otp"}), 400
    u = find_user(user_id)
    if not u or "reset_otp" not in u:
        return jsonify({"ok": False, "msg": "OTP not found. Request again"}), 404
    if u.get("reset_otp_expiry", datetime.utcnow()) < datetime.utcnow():
        return jsonify({"ok": False, "msg": "OTP expired. Request again."}), 410
    if u.get("reset_otp") != otp:
        return jsonify({"ok": False, "msg": "Invalid OTP"}), 401
    users.update_one({"User_ID": user_id}, {"$set": {"reset_otp_verified": True}, "$unset": {"reset_otp": "", "reset_otp_expiry": ""}})
    return jsonify({"ok": True, "msg": "OTP verified"})

@app.route("/api/reset-password", methods=["POST"])
def api_reset_password():
    data = request.json or {}
    user_id = data.get("user_id", "").strip()
    new_password = data.get("new_password", "")
    if not user_id or not new_password:
        return jsonify({"ok": False, "msg": "Provide user_id and new_password"}), 400
    u = find_user(user_id)
    if not u or not u.get("reset_otp_verified"):
        return jsonify({"ok": False, "msg": "OTP not verified for this user"}), 403
    users.update_one({"User_ID": user_id}, {"$set": {"password_hash": sha256_hash(new_password)}, "$unset": {"reset_otp_verified": ""}})
    return jsonify({"ok": True, "msg": "Password updated"})

# Utility: determine rule-based fraud score per supplied combinations
def compute_rule_fraud(override_location, device_choice, ip_choice, user_location, current_device, current_ip):
    """
    device_choice, ip_choice: strings e.g. "-- keep current --", "Mobile", "unknown", "121.241.105.939"
    override_location: string from dashboard select (may be "-- keep current --")
    user_location: location stored in DB (string)
    returns: (rule_prob, rule_flag_text)
    """
    # normalize
    loc_kept = (not override_location) or override_location.strip() == "" or override_location.strip() == "-- keep current --"
    device_kept = (not device_choice) or device_choice.strip() == "" or device_choice.strip() == "-- keep current --"
    ip_kept = (not ip_choice) or ip_choice.strip() == "" or ip_choice.strip() == "-- keep current --"

    device_unknown = (str(device_choice).strip().lower() == "unknown")
    ip_unknown = (str(ip_choice).strip().lower() == "unknown")

    location_changed = False
    if loc_kept:
        location_changed = False
    else:
        # if override_location provided and different from user's stored location
        location_changed = str(override_location).strip() != str(user_location).strip()


    # helper: ip_changed and device_changed booleans
    ip_changed = (not ip_kept) and (not ip_unknown)
    device_changed = (not device_kept) and (not device_unknown)

    # Case 5: location changed + ip changed + device changed => 95%
    if location_changed and ip_changed and device_changed:
        return 0.95, "Location changed + Device changed + IP changed"

    # Case 6: location changed + ip unknown + device unknown => 90%
    if location_changed and ip_unknown and device_unknown:
        return 0.90, "Location changed + Unknown Device + Unknown IP"

    # Case 7: location kept + ip unknown + device unknown => 85%
    if (not location_changed) and ip_unknown and device_unknown:
        return 0.85, "Location kept + Unknown Device + Unknown IP"

    # Case 8: location kept + ip changed + device unknown => 80%
    if (not location_changed) and ip_changed and device_unknown:
        return 0.80, "Location kept + IP changed + Unknown Device"

    # Case 9: location kept + ip unknown + device changed => 80%
    if (not location_changed) and ip_unknown and device_changed:
        return 0.80, "Location kept + IP unknown + Device changed"

    # Case 10: location changed + ip kept + device kept => 90%
    if location_changed and ip_kept and device_kept:
        return 0.90, "Location changed + IP kept + Device kept"

    # All other cases: normal transaction per your spec: return 0
    return 0.0, "Normal (per rules)"

# DASHBOARD
@app.route("/api/dashboard", methods=["GET"])
def api_dashboard():
    token = request.headers.get("Authorization")
    u = validate_session(token)
    if not u:
        return jsonify({"ok": False, "msg": "Invalid session"}), 401
    payload = {
        "User_ID": u["User_ID"],
        "name": u.get("name"),
        "phone_number": u.get("phone_number"),
        "location": u.get("location"),
        "account_summary": u.get("account_summary", {}),
        "recent_transactions": u.get("recent_transactions", []),
        "allowed_locations": ALLOWED_LOCATIONS,
        "current_device": (u.get("recent_transactions",[{}])[0].get("Device_Type") if u.get("recent_transactions") else None) or "Mobile",
        "current_ip": (u.get("recent_transactions",[{}])[0].get("IP_Address") if u.get("recent_transactions") else None) or "127.0.0.1"
    }
    return jsonify({"ok": True, "data": payload})

# INITIATE TRANSFER
@app.route("/api/initiate-transfer", methods=["POST"])
def api_initiate_transfer():
    token = request.headers.get("Authorization")
    u = validate_session(token)
    if not u:
        return jsonify({"ok": False, "msg": "Invalid session"}), 401
    data = request.json or {}
    try:
        amount = float(data.get("amount", 0) or 0)
    except:
        return jsonify({"ok": False, "msg": "Invalid amount"}), 400
    beneficiary = data.get("beneficiary", "").strip()
    txn_id = data.get("txn_id", "") or f"TEMP_{uuid.uuid4().hex[:6]}"
    remarks = data.get("remarks", "")
    override_location = data.get("override_location")
    override_time = data.get("override_time")
    device_choice = data.get("device_choice", "-- keep current --")
    ip_choice = data.get("ip_choice", "-- keep current --")

    if amount <= 0 or not beneficiary:
        return jsonify({"ok": False, "msg": "Provide beneficiary and amount"}), 400

    outflow = float(u.get("account_summary", {}).get("Spend_Analysis", {}).get("Outflow", 0.0) or 0.0)
    require_secret_key = amount > outflow

    txn_time = override_time or datetime.utcnow().strftime("%d-%m-%Y %H:%M")
    txn_location = override_location if override_location and override_location != "-- keep current --" else (u.get("location") or "")

    # compute rule-based fraud
    rule_prob, rule_reason = compute_rule_fraud(override_location, device_choice, ip_choice, u.get("location",""), 
                                               (u.get("recent_transactions",[{}])[0].get("Device_Type") if u.get("recent_transactions") else "Mobile"),
                                               (u.get("recent_transactions",[{}])[0].get("IP_Address") if u.get("recent_transactions") else "127.0.0.1"))
    final_prob = float(rule_prob)

    # If model exists, compute model prob and take max
    if fraud_model is not None:
        try:
            model_txn = {
                "Transaction_ID": txn_id,
                "User_ID": u["User_ID"],
                "Transaction_Amount": amount,
                "Transaction_Time": txn_time,
                "Account_Balance": float(u.get("account_summary", {}).get("Total_Balance", 0.0) or 0.0),
                "Device_Type": device_choice if device_choice and device_choice != "-- keep current --" else (u.get("recent_transactions",[{}])[0].get("Device_Type") or "Mobile"),
                "Location": txn_location,
                "Merchant_Category": "Transfer",
                "IP_Address": ip_choice if ip_choice and ip_choice != "-- keep current --" else (u.get("recent_transactions",[{}])[0].get("IP_Address") or "127.0.0.1"),
                "IP_Address_Flagged": 1 if str(ip_choice).strip().lower() == "unknown" else 0,
                "Previous_Transaction_Amount": float(u.get("account_summary", {}).get("Spend_Analysis", {}).get("Outflow", 0.0) or 0.0),
                "Daily_transaction_count": 1,
                "Avg_Transaction_Amount_Per_Day": float(u.get("account_summary", {}).get("Spend_Analysis", {}).get("Inflow", 0.0) or 0.0),
                "Avg_Transactions_amount_7Day": float(u.get("account_summary", {}).get("Spend_Analysis", {}).get("Outflow", 0.0) or 0.0),
                "Failed_Transaction_Count_7d": 0,
                "Card_Type": "Debit",
                "Card_Age_Months": int(u.get("account_summary", {}).get("Card_Age_Months", 0) or 0),
                "Transaction_Distance_KM": (426.78 if (override_location and override_location != u.get("location","")) else 5.0),
                "Authentication_Method": "OTP"
            }
            df_txn = preprocess_new_data(model_txn)
            model_prob = float(fraud_model.predict_proba(df_txn)[0][1])
            final_prob = max(final_prob, model_prob)
        except Exception as e:
            print(f"[WARN] model scoring at initiate failed: {e}")

    # Save pending transfer and OTP
    transfer_otp = gen_otp(6)
    expiry = datetime.utcnow() + timedelta(seconds=TRANSFER_OTP_TTL_SECONDS)
    pending = {
        "amount": amount,
        "beneficiary": beneficiary,
        "txn_id": txn_id,
        "remarks": remarks,
        "transfer_otp": transfer_otp,
        "transfer_otp_expiry": expiry,
        "require_secret_key": require_secret_key,
        "initiated_at": datetime.utcnow().isoformat(),
        "fraud_prob": float(final_prob),
        "rule_prob": float(rule_prob),
        "rule_reason": rule_reason,
        "override_location": txn_location,
        "override_time": txn_time,
        "device_choice": device_choice,
        "ip_choice": ip_choice
    }
    users.update_one({"User_ID": u["User_ID"]}, {"$set": {"pending_transfer": pending}})

    resp = {
        "ok": True,
        "msg": "Transfer OTP generated (demo)",
        "transfer_otp": transfer_otp,
        "ttl_seconds": TRANSFER_OTP_TTL_SECONDS,
        "require_secret_key": require_secret_key,
        "fraud_prob": float(final_prob),
        "rule_prob": float(rule_prob),
        "rule_reason": rule_reason
    }
    return jsonify(resp)

# CONFIRM TRANSFER
@app.route("/api/confirm-transfer", methods=["POST"])
def api_confirm_transfer():
    token = request.headers.get("Authorization")
    u = validate_session(token)
    if not u:
        return jsonify({"ok": False, "msg": "Invalid session"}), 401
    data = request.json or {}
    entered_otp = data.get("otp", "").strip()
    entered_secret = data.get("secret_key", "").strip()
    if not entered_otp:
        return jsonify({"ok": False, "msg": "Provide otp"}), 400

    u_latest = find_user(u["User_ID"])
    pending = u_latest.get("pending_transfer")
    if not pending:
        return jsonify({"ok": False, "msg": "No pending transfer"}), 400
    if pending.get("transfer_otp_expiry", datetime.utcnow()) < datetime.utcnow():
        users.update_one({"User_ID": u["User_ID"]}, {"$unset": {"pending_transfer": ""}})
        return jsonify({"ok": False, "msg": "Transfer OTP expired"}), 410
    if pending.get("transfer_otp") != entered_otp:
        return jsonify({"ok": False, "msg": "Invalid transfer OTP"}), 401

    # verify secret if required
    if pending.get("require_secret_key"):
        if not entered_secret:
            return jsonify({"ok": False, "msg": "Secret key required for this transfer"}), 400
        stored_hash = u_latest.get("secret_key_hash", "")
        if stored_hash != sha256_hash(entered_secret.strip()):
            users.update_one({"User_ID": u["User_ID"]}, {"$unset": {"pending_transfer": ""}})
            return jsonify({"ok": False, "msg": "Secret key invalid: transaction blocked (suspicious)"}), 403

    # recompute model if available and pick max
    final_prob = float(pending.get("fraud_prob", 0.0))
    if fraud_model is not None:
        try:
            txn_features = {
                "Transaction_ID": pending.get("txn_id", ""),
                "User_ID": u["User_ID"],
                "Transaction_Amount": float(pending.get("amount", 0.0)),
                "Transaction_Time": pending.get("override_time"),
                "Account_Balance": float(u.get("account_summary", {}).get("Total_Balance", 0.0) or 0.0),
                "Device_Type": pending.get("device_choice") if pending.get("device_choice") != "-- keep current --" else (u.get("recent_transactions",[{}])[0].get("Device_Type") or "Mobile"),
                "Location": pending.get("override_location"),
                "Merchant_Category": "Transfer",
                "IP_Address": pending.get("ip_choice") if pending.get("ip_choice") != "-- keep current --" else (u.get("recent_transactions",[{}])[0].get("IP_Address") or "127.0.0.1"),
                "IP_Address_Flagged": 1 if str(pending.get("ip_choice")).strip().lower() == "unknown" else 0,
                "Previous_Transaction_Amount": float(u.get("account_summary", {}).get("Spend_Analysis", {}).get("Outflow", 0.0) or 0.0),
                "Daily_transaction_count": 1,
                "Avg_Transaction_Amount_Per_Day": float(u.get("account_summary", {}).get("Spend_Analysis", {}).get("Inflow", 0.0) or 0.0),
                "Avg_Transactions_amount_7Day": float(u.get("account_summary", {}).get("Spend_Analysis", {}).get("Outflow", 0.0) or 0.0),
                "Failed_Transaction_Count_7d": 0,
                "Card_Type": "Debit",
                "Card_Age_Months": int(u.get("account_summary", {}).get("Card_Age_Months", 0) or 0),
                "Transaction_Distance_KM": (426.78 if (pending.get("override_location") and pending.get("override_location") != u.get("location","")) else 5.0),
                "Authentication_Method": "OTP"
            }
            df_txn = preprocess_new_data(txn_features)
            model_prob = float(fraud_model.predict_proba(df_txn)[0][1])
            final_prob = max(final_prob, model_prob)
        except Exception as e:
            print(f"[WARN] model scoring failed at confirm: {e}")

    # Build fraud_alerts to send to frontend
    # We'll include: risk (0-1), rule_reason, user_location_for_message
    user_db_location = u_latest.get("location", "")
    override_loc = pending.get("override_location") or user_db_location
    # Determine message location text: either new location or DB location depending on whether override provided
    message_loc = override_loc if override_loc else user_db_location

    fraud_alerts = {
        "risk_score": float(final_prob),
        "rule_prob": float(pending.get("rule_prob", 0.0)),
        "rule_reason": pending.get("rule_reason", ""),
        "location_for_message": message_loc
    }

    if final_prob >= 0.8:
        # log
        db["fraud_logs"].insert_one({
            "user_id": u_latest["User_ID"],
            "amount": float(pending.get("amount", 0)),
            "model_fraud_prob": float(final_prob),
            "rule_prob": float(pending.get("rule_prob", 0.0)),
            "time": datetime.utcnow()
        })
        users.update_one({"User_ID": u_latest["User_ID"]}, {"$unset": {"pending_transfer": ""}})
        # Create the standard message body per your format
        # Round percentages to whole numbers for display
        pct = int(round(final_prob * 100))
        extra_msg = f" Unknown device & Unknown IP address at location {message_loc} — Transaction not possible."
        resp = {"ok": False, "msg": f"⚠️ AI flagged this transaction as suspicious (RISK SCORE: {pct}% ).{extra_msg}", "fraud_prob": final_prob}
        resp["fraud_alerts"] = fraud_alerts
        return jsonify(resp), 403

    # Proceed: debit and append transaction (success)
    amt = float(pending["amount"])
    acct = u_latest.get("account_summary", {})
    total_bal = float(acct.get("Total_Balance", 0.0) or 0.0)
    if amt > total_bal:
        users.update_one({"User_ID": u_latest["User_ID"]}, {"$unset": {"pending_transfer": ""}})
        return jsonify({"ok": False, "msg": "Insufficient funds"}), 402

    new_total = total_bal - amt
    txn = {
        "Transaction_ID": pending.get("txn_id", ""),
        "type": "Transfer",
        "Merchant_Category": "Transfer",
        "Transaction_Amount": -amt,
        "time": pending.get("override_time") or datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S"),
        "Transaction_Time": pending.get("override_time") or datetime.utcnow().strftime("%d-%m-%Y %H:%M"),
        "Location": pending.get("override_location") or u_latest.get("location",""),
        "remark": pending.get("remarks", ""),
        "txn_id": pending.get("txn_id", "")
    }

    users.update_one({"User_ID": u_latest["User_ID"]}, {
        "$set": {"account_summary.Total_Balance": new_total},
        "$push": {"recent_transactions": {"$each": [txn], "$position": 0}},
        "$unset": {"pending_transfer": ""}}
    )
    return jsonify({"ok": True, "msg": "Transfer completed", "new_balance": new_total, "txn": txn})

# LOGOUT
@app.route("/api/logout", methods=["POST"])
def api_logout():
    token = request.headers.get("Authorization")
    u = validate_session(token)
    if not u:
        return jsonify({"ok": False, "msg": "Invalid session"}), 401
    users.update_one({"User_ID": u["User_ID"]}, {"$unset": {"session_token": "", "session_expiry": ""}})
    return jsonify({"ok": True, "msg": "Logged out"})

# DEMO USER HELPER (unchanged)
@app.route("/api/demo-user", methods=["GET"])
def api_demo_user():
    some_user = users.find_one({}, {"User_ID": 1, "demo_plain_password": 1, "demo_plain_secret": 1, "name": 1})
    if not some_user:
        return jsonify({"ok": False, "msg": "No users found. Run generate_user_to_mongo.py first."}), 404
    return jsonify({"ok": True, "data": {
        "User_ID": some_user["User_ID"],
        "password": some_user.get("demo_plain_password"),
        "secret": some_user.get("demo_plain_secret"),
        "name": some_user.get("name")
    }})

if __name__ == "__main__":
    print("Serving frontend from:", FRONTEND_DIR)
    app.run(debug=True, port=5000)
