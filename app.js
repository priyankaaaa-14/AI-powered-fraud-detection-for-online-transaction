// app.js - frontend logic (Dashboard selects -> Transfer)
const API_ROOT = "http://127.0.0.1:5000/api";
function id(n){return document.getElementById(n);}
const toastEl = id("toast");
function showToast(msg, ttl=4000, cls=""){ toastEl.textContent = msg; toastEl.className="toast"; if(cls) toastEl.classList.add(cls); toastEl.classList.remove("hidden"); if(ttl>0) setTimeout(()=>toastEl.classList.add("hidden"), ttl); }
function hideToast(){ toastEl.classList.add("hidden"); }

// LOGIN page handlers (index.html expected)
if (id("btn_login")){
  id("btn_login").onclick = async ()=>{
    const user_id = id("login_user_id").value.trim();
    const password = id("login_password").value;
    if(!user_id||!password){ showToast("Enter credentials",2000); return; }
    const res = await fetch(API_ROOT + "/login", {
      method:"POST", headers:{"Content-Type":"application/json"},
      body: JSON.stringify({user_id, password})
    });
    const j = await res.json();
    if(!j.ok){ showToast(j.msg,3000); return; }
    localStorage.setItem("session_token", j.data.token);
    window.location.href = "/dashboard.html";
  };
  // forgot password UI (unchanged)
  id("forgot_link").onclick = (e)=>{ e.preventDefault(); id("overlay").classList.remove("hidden"); showStep(1); };
  function showStep(n){
    id("forgot_step1").classList.toggle("hidden", n!==1);
    id("forgot_step2").classList.toggle("hidden", n!==2);
    id("forgot_step3").classList.toggle("hidden", n!==3);
  }
  ["back_to_login","back_to_login2","back_to_login3"].forEach(x=>{ if(id(x)) id(x).onclick=()=>{ id("overlay").classList.add("hidden"); hideToast(); } });
  if(id("btn_request_otp")) id("btn_request_otp").onclick = async ()=>{
    const u = id("fp_user_id").value.trim(); if(!u){ showToast("Enter User ID",2000); return; }
    const res = await fetch(API_ROOT + "/request-otp", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({user_id:u})});
    const j = await res.json();
    if(!j.ok){ showToast(j.msg,3000); return; }
    showStep(2); showToast("OTP (demo): "+j.otp, 0, "otp"); localStorage.setItem("reset_user", u);
  };
  if(id("btn_verify_otp")) id("btn_verify_otp").onclick = async ()=>{
    const u = localStorage.getItem("reset_user"), otp = id("input_otp").value.trim(); if(!u||!otp){ showToast("Enter OTP",2000); return; }
    const res = await fetch(API_ROOT + "/verify-otp", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({user_id:u, otp})});
    const j = await res.json(); if(!j.ok){ showToast(j.msg,3000); return; } hideToast(); showStep(3);
  };
  if(id("btn_reset_password")) id("btn_reset_password").onclick = async ()=>{
    const u = localStorage.getItem("reset_user"), p1=id("new_password").value, p2=id("confirm_password").value;
    if(!p1||p1!==p2){ showToast("Passwords must match",2000); return; }
    const res = await fetch(API_ROOT + "/reset-password", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({user_id:u, new_password:p1})});
    const j = await res.json(); if(!j.ok){ showToast(j.msg,3000); return; }
    showToast("Password updated. Login with new password.",3000); id("overlay").classList.add("hidden");
  };
}

// DASHBOARD pages
if (window.location.pathname.endsWith("dashboard.html")){
  const token = localStorage.getItem("session_token");
  if(!token){ window.location.href="/"; }

  // nav switching
  document.querySelectorAll(".sidebar .nav").forEach(el=>{
    el.addEventListener("click", (e)=>{
      document.querySelectorAll(".sidebar .nav").forEach(x=>x.classList.remove("active"));
      e.currentTarget.classList.add("active");
      const p = e.currentTarget.getAttribute("data-page");
      document.querySelectorAll(".page").forEach(x=>x.classList.add("hidden"));
      document.getElementById("page_"+p).classList.remove("hidden");
      if(p==="overview") loadOverview();
      if(p==="accounts") loadAccounts();
      if(p==="transfer") loadTransfer();
    });
  });

  id("btn_logout").onclick = async ()=>{
    await fetch(API_ROOT + "/logout", {method:"POST", headers:{"Authorization": token}});
    localStorage.removeItem("session_token"); window.location.href="/";
  };

  id("btn_view_accounts")?.addEventListener("click", ()=>document.querySelector('[data-page="accounts"]').click());
  id("go_transfer")?.addEventListener("click", ()=>document.querySelector('[data-page="transfer"]').click());

  loadOverview(); refreshTransactions(); loadAccounts();

  async function loadOverview(){
    const res = await fetch(API_ROOT + "/dashboard", {headers:{"Authorization": token}});
    const j = await res.json(); if(!j.ok){ showToast("Session expired",2000); localStorage.removeItem("session_token"); window.location.href="/"; return; }
    const d = j.data;
    id("welcome_line").textContent = `Welcome Back, ${d.name || d.User_ID}!`;
    id("user_location").textContent = "Location: " + (d.location||"-");
    id("user_phone").textContent = "Phone: " + (d.phone_number||"-");
    id("total_balance").textContent = "₹" + ((d.account_summary.Total_Balance||0).toFixed(2));
    id("inflow").textContent = "₹" + ((d.account_summary.Spend_Analysis||{}).Inflow||0).toFixed(2);
    id("outflow").textContent = "₹" + ((d.account_summary.Spend_Analysis||{}).Outflow||0).toFixed(2);

    // populate location select with allowed locations and set default to keep current
    const sel = id("select_location"); sel.innerHTML = "<option>-- keep current --</option>";
    (d.allowed_locations||[]).forEach(loc=>{ const opt=document.createElement("option"); opt.value=loc; opt.textContent=loc; sel.appendChild(opt);});

    // ensure current device/ip are visible in selects
    if(d.current_device){
      const sdev = id("select_device");
      if([...sdev.options].every(o=>o.value !== d.current_device)){
        const opt = document.createElement("option"); opt.value = d.current_device; opt.textContent = d.current_device;
        sdev.appendChild(opt);
      }
    }
    if(d.current_ip){
      const sip = id("select_ip");
      if([...sip.options].every(o=>o.value !== d.current_ip)){
        const opt = document.createElement("option"); opt.value = d.current_ip; opt.textContent = d.current_ip;
        sip.appendChild(opt);
      }
    }

    // recent transactions (overview)
    id("recent_transactions").innerHTML = "";
    (d.recent_transactions||[]).slice(0,6).forEach(tx=>{
      const li = document.createElement("li");
      const time = tx.Transaction_Time || tx.time || "";
      const amt = tx.Transaction_Amount || tx.amount || 0;
      const remark = tx.remark || tx.Merchant_Category || tx.type || "";
      li.innerHTML = `<div class="recent-item"><div><strong>${remark}</strong><div class="recent-meta">${tx.Location||''} • ${time}</div></div><div style="text-align:right"><div style="color:${amt<0?'#e65555':'#0a8e42'}">${amt<0?('₹'+Math.abs(amt).toFixed(2)):('₹'+(amt).toFixed(2))}</div></div></div>`;
      id("recent_transactions").appendChild(li);
    });

    hideFraudBanner(); id("top-user").textContent = `Call Us: +91 98765 | Welcome, ${d.name || d.User_ID}`;
  }

  async function loadAccounts(){
    const res = await fetch(API_ROOT + "/dashboard", {headers:{"Authorization": token}});
    const j = await res.json(); if(!j.ok){ showToast("Session expired",2000); localStorage.removeItem("session_token"); window.location.href="/"; return; }
    const d = j.data;
    id("user_short").textContent = (" " + d.User_ID).slice(0,10);
    id("savings_balance").textContent = "₹" + (d.account_summary.Total_Balance||0).toFixed(2);
    id("balance_text").textContent = "₹" + (d.account_summary.Total_Balance||0).toFixed(2);
    id("available_spend").textContent = "₹" + (d.account_summary.Total_Balance||0).toFixed(2);
    id("card_last4").textContent = (d.account_summary.Card_Number || "************5651").slice(-4);
    id("card_age").textContent = d.account_summary.Card_Age_Months || 0;
    const list = id("recent_transactions_accounts"); if(list) list.innerHTML = "";
    (d.recent_transactions||[]).slice(0,6).forEach(tx=>{
      if(!list) return;
      const li = document.createElement("li");
      const amt = tx.Transaction_Amount || tx.amount || 0;
      li.innerHTML = `<div style="display:flex;justify-content:space-between"><div><strong>${tx.Merchant_Category||tx.type}</strong><div style="font-size:12px;color:#666">${tx.Transaction_Time||tx.time}</div></div><div style="color:${amt<0?'#e65555':'#0a8e42'}">${amt<0?('₹'+Math.abs(amt).toFixed(2)):(amt>0?('₹'+amt.toFixed(2)):'')}</div></div>`;
      list.appendChild(li);
    });
  }

  async function loadTransfer(){
    id("beneficiary").value = ""; id("txn_id").value = ""; id("amount").value = ""; id("remarks").value = "";
    id("transfer_otp_input").value=""; id("transfer_secret_input").value=""; id("transfer_secret_input").style.display="none";
    id("transfer_verify").classList.add("hidden"); id("transfer_result").textContent=""; hideFraudBanner();
  }

  function showFraudBanner(serverAlerts){
    const banner = id("fraud_banner"); if(!banner) return;
    banner.classList.remove("hidden");
    // serverAlerts expected: {risk_score:0.95, location_for_message: "..."}
    const pct = Math.round((serverAlerts && serverAlerts.risk_score || 0) * 100);
    id("fraud_msg").textContent = `⚠️ AI flagged this transaction as suspicious (RISK SCORE: ${pct}% ).`;
    const loc = serverAlerts && serverAlerts.location_for_message ? serverAlerts.location_for_message : "-";
    id("fraud_details").textContent = `Unknown device & Unknown IP address at location ${loc} — Transaction not possible.`;
  }
  function hideFraudBanner(){ const b=id("fraud_banner"); if(b) b.classList.add("hidden"); }

  // Initiate transfer: send selected dashboard values
  id("btn_initiate").onclick = async ()=>{
    const beneficiary = id("beneficiary").value.trim();
    const txn_id = id("txn_id").value.trim();
    const amount = id("amount").value.trim();
    const remarks = id("remarks").value.trim();
    if(!beneficiary || !amount){ showToast("Enter beneficiary and amount",3000); return; }

    // read dashboard selections
    const override_location = id("select_location") ? id("select_location").value : "-- keep current --";
    const override_time = id("manual_dt") ? id("manual_dt").value.trim() : "";
    const device_choice = id("select_device") ? id("select_device").value : "-- keep current --";
    const ip_choice = id("select_ip") ? id("select_ip").value : "-- keep current --";

    const res = await fetch(API_ROOT + "/initiate-transfer", {
      method:"POST",
      headers: {"Content-Type":"application/json", "Authorization": token},
      body: JSON.stringify({beneficiary, txn_id, amount, remarks, override_location, override_time, device_choice, ip_choice})
    });
    const j = await res.json();
    if(!j.ok){ showToast(j.msg || "Error",4000); return; }

    // show OTP toast and reveal verify area; DO NOT show fraud banner yet (we show banner after confirm only)
    showToast("Transfer OTP (demo): " + j.transfer_otp, j.ttl_seconds*1000, "otp");
    setTimeout(()=> hideToast(), j.ttl_seconds*1000);
    id("transfer_verify").classList.remove("hidden");
    id("transfer_result").textContent = "";
    id("transfer_secret_input").style.display = j.require_secret_key ? "block" : "none";
  };

  // Confirm transfer: show fraud banner if blocked
  id("btn_confirm_transfer").onclick = async ()=>{
    const otp = id("transfer_otp_input").value.trim();
    const secret = id("transfer_secret_input").value.trim();
    if(!otp){ showToast("Enter OTP",3000); return; }
    const res = await fetch(API_ROOT + "/confirm-transfer", {
      method:"POST", headers: {"Content-Type":"application/json","Authorization": token},
      body: JSON.stringify({otp, secret_key: secret})
    });
    const j = await res.json();
    if(!j.ok){
      // show fraud banner (backend sends 'fraud_alerts')
      if(j.fraud_alerts) showFraudBanner(j.fraud_alerts);
      id("transfer_result").textContent = j.msg || "Transfer blocked";
      id("transfer_result").classList.remove("transfer-success");
      id("transfer_result").classList.add("transfer-blocked");
      await refreshTransactions(); loadOverview(); loadAccounts();
      return;
    }
    // success
    hideFraudBanner();
    showToast("Transfer successful",3000);
    const remark = j.txn.remark || "Transfer";
    const amt = Math.abs(j.txn.Transaction_Amount || 0).toFixed(2);
    const loc = j.txn.Location || "";
    const timing = j.txn.Transaction_Time || j.txn.time || "";
    id("transfer_result").innerHTML = `<strong>${remark}</strong> — ₹${amt} SUCCESS<br/><span class="recent-meta">${loc} • ${timing}</span>`;
    id("transfer_result").classList.remove("transfer-blocked");
    id("transfer_result").classList.add("transfer-success");
    await refreshTransactions(); loadOverview(); loadAccounts();
  };

  // Recent transactions list (transfer page)
  async function refreshTransactions(){
    const res = await fetch(API_ROOT + "/dashboard", {headers:{"Authorization": token}});
    const j = await res.json(); if(!j.ok) return;
    const list = id("recent_transactions_transfer"); if(list) list.innerHTML = "";
    (j.data.recent_transactions || []).slice(0,8).forEach(tx=>{
      if(!list) return;
      const li = document.createElement("li");
      const amt = tx.Transaction_Amount || tx.amount || 0;
      const remark = tx.remark || tx.Merchant_Category || tx.type || 'Transfer';
      const time = tx.Transaction_Time || tx.time || '';
      const location = tx.Location || '';
      const rightText = `<div style="color:#0a8e42">₹${Math.abs(amt).toFixed(2)} SUCCESS</div>`;
      li.innerHTML = `<div style="display:flex;justify-content:space-between"><div><strong>${remark}</strong><div style="font-size:12px;color:#666">${location} • ${time}</div></div><div>${rightText}</div></div>`;
      list.appendChild(li);
    });
  }
}
