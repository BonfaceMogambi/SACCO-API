import streamlit as st
import requests

API_BASE = "http://127.0.0.1:8000/api/v1"

st.title("💳 SACCO API Interactive UI")

# Tabs for different endpoints
tabs = st.tabs(["Balance Enquiry", "Funds Transfer", "Mini-statement", "Loan Inquiry", "Loan FT"])

# ---------------- Balance Enquiry ----------------
with tabs[0]:
    st.subheader("Balance Enquiry")
    acc = st.text_input("Enter Account Number", "")
    msg_id = st.text_input("Message ID", "MSG001")
    if st.button("Check Balance"):
        payload = {
            "serviceName": "Balance",
            "messageID": msg_id,
            "Connection": {
                "connectionID": "test",
                "connectionPassword": "testpass"
            },
            "getBalanceRequest": {
                "OperationParameters": {"TransactionDate": "2025-09-08"},
                "Account": {"DebitAccount": acc},
                "Institution": {"InstitutionCode": "SACCO01"},
                "Posting": {
                    "ChargeAmount": "5",
                    "ChargeCurrency": "KES",
                    "FeeAmount": "0",
                    "FeeCurrency": "KES",
                    "Narrative": "Balance Enquiry"
                }
            }
        }
        res = requests.post(f"{API_BASE}/balance", json=payload)
        if res.status_code == 200:
            st.json(res.json())
        else:
            st.error(res.text)

# ---------------- Funds Transfer ----------------
with tabs[1]:
    st.subheader("Funds Transfer")
    debit = st.text_input("Debit Account")
    credit = st.text_input("Credit Account")
    amount = st.number_input("Amount", min_value=1.0, step=1.0)
    if st.button("Send Funds"):
        payload = {
            "serviceName": "FT",
            "messageID": "FT123",
            "Connection": {"connectionID": "test", "connectionPassword": "testpass"},
            "sendFundsTransferRequest": {
                "OperationParameters": {
                    "TransactionDate": "2025-09-08",
                    "TransactionType": "FT"
                },
                "Institution": {"InstitutionCode": "SACCO01"},
                "Posting": {
                    "DebitAccount": debit,
                    "Amount": str(amount),
                    "Currency": "KES",
                    "CreditAccount": credit,
                    "ChargeAmount": "0",
                    "ChargeCurrency": "KES",
                    "FeeAmount": "0",
                    "FeeCurrency": "KES",
                    "Narrative1": "Funds Transfer"
                }
            }
        }
        res = requests.post(f"{API_BASE}/funds-transfer", json=payload)
        st.json(res.json())
