from typing import List, Optional, Literal
from fastapi import FastAPI, Body, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime
import os
from dotenv import load_dotenv
import mysql.connector
from fastapi import Request

# ---------------------------
# Load .env credentials
# ---------------------------
load_dotenv()

EXPECTED_CONN_ID = os.getenv("CONNECTION_ID")
EXPECTED_CONN_PASS = os.getenv("CONNECTION_PASSWORD")

def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )

def find_member_by_account(account_number: str):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM members WHERE account_number = %s", (account_number,))
    member = cursor.fetchone()
    cursor.close()
    conn.close()
    return member

def find_transactions_by_account(account_number: str, limit: int = 5):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT * FROM transactions 
        WHERE account_number = %s 
        ORDER BY transaction_date DESC 
        LIMIT %s
    """, (account_number, limit))
    txns = cursor.fetchall()
    cursor.close()
    conn.close()
    return txns

def find_loans_by_account(account_number: str):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM loans WHERE linked_account_number = %s", (account_number,))
    loans = cursor.fetchall()
    cursor.close()
    conn.close()
    return loans

# ---------------------------
# FastAPI app with CORS
# ---------------------------
app = FastAPI(
    title="SACCO Standard API",
    version="1.2.3",
    description="Implements Balance Enquiry, Funds Transfer, Mini-statement, Loan Inquiry, and Loan Funds Transfer per SACCO Standard API Specification.",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For development - restrict in production
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods including OPTIONS
    allow_headers=["*"],  # Allows all headers
)

# ---------------------------
# Shared / Utility Structures
# ---------------------------
class KeyValue(BaseModel):
    key: str
    value: str

class Connection(BaseModel):
    connectionID: Optional[str] = EXPECTED_CONN_ID
    connectionPassword: Optional[str] = EXPECTED_CONN_PASS

def verify_connection(conn: Connection):
    if conn.connectionID != EXPECTED_CONN_ID or conn.connectionPassword != EXPECTED_CONN_PASS:
        raise HTTPException(status_code=401, detail="Invalid Connection credentials")

# ---------------------------
# Balance Enquiry
# ---------------------------
class BalanceAccount(BaseModel):
    CreditAccount: Optional[str] = None
    DebitAccount: str

class BalanceInstitution(BaseModel):
    InstitutionCode: str
    InstitutionName: Optional[str] = None

class BalancePosting(BaseModel):
    ChargeAmount: str
    ChargeCurrency: str
    FeeAmount: str
    FeeCurrency: str
    Narrative: str

class BalanceOperationParameters(BaseModel):
    TransactionDate: str
    TerminalID: Optional[str] = None
    Channel: Optional[str] = None

class GetBalanceRequest(BaseModel):
    OperationParameters: BalanceOperationParameters
    Account: BalanceAccount
    Institution: BalanceInstitution
    Posting: BalancePosting
    AdditionalInfo: Optional[List[KeyValue]] = None

class BalanceEnquiryIn(BaseModel):
    serviceName: str = "Balance"
    messageID: str
    Connection: Connection
    getBalanceRequest: GetBalanceRequest

class BalanceReplyOperationParameters(BaseModel):
    TransactionDate: str
    TransactionReference: str

class BalanceReplyInstitution(BaseModel):
    InstitutionCode: str
    InstitutionName: Optional[str] = None

class BalanceReplyAccount(BaseModel):
    DebitAccount: str
    BookBalance: str
    ClearedBalance: str
    Currency: str

class GetBalanceResponse(BaseModel):
    OperationParameters: BalanceReplyOperationParameters
    Institution: BalanceReplyInstitution
    Account: BalanceReplyAccount
    AdditionalInfo: Optional[List[KeyValue]] = None

class BalanceEnquiryOut(BaseModel):
    messageID: str
    statusCode: str
    statusDescription: str
    getBalanceResponse: GetBalanceResponse

@app.post("/api/v1/balance", response_model=BalanceEnquiryOut)
def get_balance(payload: BalanceEnquiryIn = Body(...)):
    charge_amount = 5.0  # Charge 5 bob for balance check
    account = payload.getBalanceRequest.Account.DebitAccount

    member = find_member_by_account(account)
    if not member:
        raise HTTPException(status_code=404, detail="Account not found")

    # Convert balances to float for calculation
    current_book_balance = float(member["book_balance"])
    current_cleared_balance = float(member["cleared_balance"])

    if current_book_balance < charge_amount or current_cleared_balance < charge_amount:
        raise HTTPException(status_code=400, detail="Insufficient balance to perform balance enquiry charge")

    # Calculate new balances after charge
    new_book_balance = current_book_balance - charge_amount
    new_cleared_balance = current_cleared_balance - charge_amount

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Update members table balances
        cursor.execute("""
            UPDATE members
            SET book_balance = %s, cleared_balance = %s
            WHERE account_number = %s
        """, (new_book_balance, new_cleared_balance, account))

        # Insert transaction record for the charge
        cursor.execute("""
            INSERT INTO transactions (account_number, transaction_date, transaction_reference, debit_credit, amount, narration, posting_date, book_balance, cleared_balance, channel_id)
            VALUES (%s, NOW(), %s, %s, %s, %s, NOW(), %s, %s, %s)
        """, (
            account,
            f"Ref_{payload.messageID}_BALCHARGE",
            "DR",
            charge_amount,
            "Balance enquiry fee",
            new_book_balance,
            new_cleared_balance,
            "BAL01"
        ))

        conn.commit()
    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    cursor.close()
    conn.close()

    # Return updated balance info
    return BalanceEnquiryOut(
        messageID=payload.messageID,
        statusCode="200",
        statusDescription=f"Successful",
        getBalanceResponse=GetBalanceResponse(
            OperationParameters=BalanceReplyOperationParameters(
                TransactionDate=datetime.now().isoformat(),
                TransactionReference=f"Ref_{payload.messageID}_BALCHARGE",
            ),
            Institution=BalanceReplyInstitution(
                InstitutionCode=payload.getBalanceRequest.Institution.InstitutionCode,
                InstitutionName=payload.getBalanceRequest.Institution.InstitutionName,
            ),
            Account=BalanceReplyAccount(
                DebitAccount=member["account_number"],
                BookBalance=str(new_book_balance),
                ClearedBalance=str(new_cleared_balance),
                Currency=member["currency"],
            ),
            AdditionalInfo=[
                KeyValue(key="CardNumber", value=member["card_number"]),
                KeyValue(key="MemberName", value=member["member_name"]),
            ],
        ),
    )



# ---------------------------
# Funds Transfer
# ---------------------------
class FT_OperationParameters(BaseModel):
    TransactionDate: str
    TerminalID: Optional[str] = None
    Channel: Optional[str] = None
    connectionMode: Optional[str] = None
    TransactionType: str
    OriginalMessageID: Optional[str] = None

class FT_Institution(BaseModel):
    InstitutionCode: str
    InstitutionName: Optional[str] = None

class FT_Posting(BaseModel):
    DebitAccount: str
    Amount: str
    Currency: str
    CreditAccount: str
    ChargeAmount: str
    ChargeCurrency: str
    FeeAmount: str
    FeeCurrency: str
    Narrative1: str
    Narrarive2: Optional[str] = None

class SendFundsTransferRequest(BaseModel):
    OperationParameters: FT_OperationParameters
    Institution: FT_Institution
    Posting: FT_Posting
    AdditionalInfo: Optional[List[KeyValue]] = None

class FundsTransferIn(BaseModel):
    serviceName: str = "FT"
    messageID: str
    Connection: Connection
    sendFundsTransferRequest: SendFundsTransferRequest

class FT_ReplyOperationParameters(BaseModel):
    TransactionDate: str
    TransactionReference: str

class FT_ReplyPosting(BaseModel):
    DebitAccount: str
    Amount: str
    ClearedBalance: str
    CreditAccount: str

class SendFundsTransferResponse(BaseModel):
    OperationParameters: FT_ReplyOperationParameters
    Institution: FT_Institution
    Posting: FT_ReplyPosting
    AdditionalInfo: Optional[List[KeyValue]] = None

class FundsTransferOut(BaseModel):
    messageID: str
    statusCode: str
    statusDescription: str
    sendFundsTransferResponse: SendFundsTransferResponse

@app.post("/api/v1/funds-transfer", response_model=FundsTransferOut)
def send_funds_transfer(payload: FundsTransferIn = Body(...)):
    posting = payload.sendFundsTransferRequest.Posting
    member = find_member_by_account(posting.DebitAccount)
    if not member:
        raise HTTPException(status_code=404, detail="Account not found")

    # Convert amounts to float for comparison
    transfer_amount = float(posting.Amount)
    charge_amount = 10.0  # 10 shilling charge
    total_debit_amount = transfer_amount + charge_amount
    
    current_book_balance = float(member["book_balance"])
    current_cleared_balance = float(member["cleared_balance"])

    # Check if account has sufficient funds (including charge)
    if current_book_balance < total_debit_amount or current_cleared_balance < total_debit_amount:
        raise HTTPException(status_code=400, detail="insufficient funds")

    # Calculate new balances (including charge)
    new_book_balance = current_book_balance - total_debit_amount
    new_cleared_balance = current_cleared_balance - total_debit_amount

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Update member's balance
        cursor.execute("""
            UPDATE members 
            SET book_balance = %s, cleared_balance = %s 
            WHERE account_number = %s
        """, (new_book_balance, new_cleared_balance, posting.DebitAccount))

        # Insert main transaction record
        cursor.execute("""
            INSERT INTO transactions (account_number, transaction_date, transaction_reference, debit_credit, amount, narration, posting_date, book_balance, cleared_balance, channel_id)
            VALUES (%s, NOW(), %s, %s, %s, %s, NOW(), %s, %s, %s)
        """, (
            posting.DebitAccount,
            f"Ref_{payload.messageID}",
            "DR",
            transfer_amount,
            posting.Narrative1,
            new_book_balance,
            new_cleared_balance,
            "FT01"
        ))

        # Insert charge transaction record
        cursor.execute("""
            INSERT INTO transactions (account_number, transaction_date, transaction_reference, debit_credit, amount, narration, posting_date, book_balance, cleared_balance, channel_id)
            VALUES (%s, NOW(), %s, %s, %s, %s, NOW(), %s, %s, %s)
        """, (
            posting.DebitAccount,
            f"Chg_{payload.messageID}",
            "DR",
            charge_amount,
            "Transfer Charge",
            new_book_balance,
            new_cleared_balance,
            "FT01"
        ))

        conn.commit()

    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    cursor.close()
    conn.close()

    return FundsTransferOut(
        messageID=payload.messageID,
        statusCode="200",
        statusDescription="Successful",
        sendFundsTransferResponse=SendFundsTransferResponse(
            OperationParameters=FT_ReplyOperationParameters(
                TransactionDate=datetime.now().isoformat(),
                TransactionReference=f"Ref_{payload.messageID}",
            ),
            Institution=payload.sendFundsTransferRequest.Institution,
            Posting=FT_ReplyPosting(
                DebitAccount=posting.DebitAccount,
                Amount=posting.Amount,
                ClearedBalance=str(new_cleared_balance),
                CreditAccount=posting.CreditAccount,
                ChargeAmount=str(charge_amount),  # Include charge in response
                ChargeCurrency="KES",  # Kenyan Shilling
            ),
        ),
    )


# ---------------------------
# Mini-statement
# ---------------------------
class MiniAccount(BaseModel):
    CreditAccount: Optional[str] = None
    DebitAccount: str
    MobileNumber: Optional[str] = None

class MiniOperationParameters(BaseModel):
    TransactionDate: str
    MaxNumberRows: str

class MiniPosting(BaseModel):
    ChargeAmount: str
    ChargeCurrency: str
    FeeAmount: str
    FeeCurrency: str
    Narrative: str

class MiniInstitution(BaseModel):
    InstitutionCode: str

class MiniStatementRequest(BaseModel):
    OperationParameters: MiniOperationParameters
    Account: MiniAccount
    Institution: MiniInstitution
    Posting: MiniPosting
    AdditionalInfo: Optional[List[KeyValue]] = None

class MiniStatementIn(BaseModel):
    serviceName: str = "MiniStatement"
    messageID: str
    Connection: Connection
    ministatementRequest: MiniStatementRequest

class MiniTxn(BaseModel):
    TransactionDate: str
    TransactionReference: str
    DebitCreditFlag: Literal["DR", "CR"]
    Amount: str
    Narration: str
    PostingDate: str
    BookBalance: str
    ClearedRunningBalance: str
    ChannelID: str
    AccountName: str
    AccountNumber: str

class MiniStatementResponse(BaseModel):
    AccountTransactions: List[MiniTxn]

class MiniStatementOut(BaseModel):
    messageID: str
    statusCode: str
    statusDescription: str
    ministatementResponse: MiniStatementResponse

@app.post("/api/v1/mini-statement", response_model=MiniStatementOut)
def mini_statement(payload: MiniStatementIn = Body(...)):
    acc = payload.ministatementRequest.Account.DebitAccount
    member = find_member_by_account(acc)
    if not member:
        raise HTTPException(status_code=404, detail="Account not found")

    txns = find_transactions_by_account(acc, int(payload.ministatementRequest.OperationParameters.MaxNumberRows))

    return MiniStatementOut(
        messageID=payload.messageID,
        statusCode="200",
        statusDescription="Successful",
        ministatementResponse=MiniStatementResponse(
            AccountTransactions=[
                MiniTxn(
                    TransactionDate=txn["transaction_date"].isoformat(),
                    TransactionReference=txn["transaction_reference"],
                    DebitCreditFlag=txn["debit_credit"],
                    Amount=str(txn["amount"]),
                    Narration=txn["narration"],
                    PostingDate=txn["posting_date"].isoformat(),
                    BookBalance=str(txn["book_balance"]),
                    ClearedRunningBalance=str(txn["cleared_balance"]),
                    ChannelID=txn["channel_id"],
                    AccountName=member["member_name"],
                    AccountNumber=acc,
                ) for txn in txns
            ]
        ),
    )


# ---------------------------
# Loan Inquiry
# ---------------------------
class LoanInquiryOperationParameters(BaseModel):
    TransactionDate: str
    TransactionType: str
    Channel: Optional[str] = None
    TerminalID: Optional[str] = None

class LoanInquiryAccount(BaseModel):
    LoanAccount: Optional[str] = None
    DebitAccount: str

class LoanPostingIn(BaseModel):
    CustomerID: Optional[str] = None
    LoanType: Optional[str] = None

class LoanInquiryInput(BaseModel):
    OperationParameters: LoanInquiryOperationParameters
    Account: LoanInquiryAccount
    Posting: Optional[LoanPostingIn] = None
    Institution: MiniInstitution

class LoanInquiryIn(BaseModel):
    serviceName: Literal["LoanInquiry"] = "LoanInquiry"
    messageID: str
    Connection: Connection
    LoanInquiryInput: LoanInquiryInput

class LoanInquiryReplyOperation(BaseModel):
    TransactionDate: str
    TransactionReference: str

class LoanPostingOut(BaseModel):
    PrincipalAmount: Optional[float] = None
    LoanBalance: Optional[float] = None
    LoanType: Optional[str] = None
    Narrative: Optional[str] = None
    CustomerID: Optional[str] = None
    Currency: Optional[str] = None
    NextPaymentDate: Optional[str] = None
    LoanRepaymentDueDate: Optional[str] = None
    Status: Optional[str] = None
    BlacklistStatus: Optional[str] = None
    LoanPeriod: Optional[str] = None
    LoanAmount: Optional[float] = None
    LimitAmount: Optional[float] = None

class LoanInquiryOutput(BaseModel):
    OperationParameters: LoanInquiryReplyOperation
    Account: Optional[LoanInquiryAccount] = None
    Posting: LoanPostingOut
    Institution: MiniInstitution

class LoanInquiryOut(BaseModel):
    messageID: str
    statusCode: str
    statusDescription: str
    LoanInquiryOutput: LoanInquiryOutput

@app.post("/api/v1/loan/inquiry", response_model=LoanInquiryOut)
def loan_inquiry(payload: LoanInquiryIn = Body(...)):
    acc = payload.LoanInquiryInput.Account.DebitAccount
    loans = find_loans_by_account(acc)
    if not loans:
        raise HTTPException(status_code=404, detail="No loans found")

    loan = loans[0]  # assume first loan for now

    posting = LoanPostingOut(
        PrincipalAmount=float(loan["principal_amount"]),
        LoanBalance=float(loan["loan_balance"]),
        LoanType=loan["loan_type"],
        Narrative="Loan inquiry result",
        Currency=loan["currency"],
        NextPaymentDate=str(loan["next_payment_date"]),
        LoanRepaymentDueDate=str(loan["repayment_due_date"]),
        Status=loan["status"]
    )

    return LoanInquiryOut(
        messageID=payload.messageID,
        statusCode="200",
        statusDescription="Successful",
        LoanInquiryOutput=LoanInquiryOutput(
            OperationParameters=LoanInquiryReplyOperation(
                TransactionDate=datetime.now().isoformat(),
                TransactionReference=f"Ref_{payload.messageID}",
            ),
            Account=payload.LoanInquiryInput.Account,
            Posting=posting,
            Institution=payload.LoanInquiryInput.Institution,
        ),
    )


# ---------------------------
# Loan Funds Transfer - Fixed
# ---------------------------
class LoanFT_OperationParameters(BaseModel):
    TransactionDate: str
    TransactionType: str
    Channel: Optional[str] = None
    TerminalID: Optional[str] = None
    OriginalMessageID: Optional[str] = None

class LoanFT_Account(BaseModel):
    LoanAccount: Optional[str] = None

class LoanFT_Posting(BaseModel):
    DebitAccount: Optional[str] = None
    LoanType: str
    LoanPeriod: Optional[str] = None
    Amount: float
    CustomerId: Optional[str] = None
    Currency: str

class LoanFTRequest(BaseModel):
    OperationParameters: LoanFT_OperationParameters
    Account: Optional[LoanFT_Account] = None
    Posting: LoanFT_Posting
    Institution: FT_Institution

class LoanFT_In(BaseModel):
    serviceName: str = "LoanFT"
    messageID: str
    Connection: Connection
    LoanFTRequest: LoanFTRequest

class LoanFT_ReplyOperation(BaseModel):
    TransactionReference: str
    TransactionDate: str
    TransactionType: str

class LoanFT_ReplyAccount(BaseModel):
    LoanAccount: Optional[str] = None
    LoanBalance: Optional[str] = None
    LinkedAccount: Optional[str] = None
    LinkedAccountBookBalance: Optional[str] = None
    LinkedAccountClearedBalance: Optional[str] = None

class LoanFT_Output(BaseModel):
    OperationParameters: LoanFT_ReplyOperation
    Account: LoanFT_ReplyAccount
    Posting: Optional[dict] = None
    Institution: FT_Institution

class LoanFT_Out(BaseModel):
    messageID: str
    statusCode: str
    statusDescription: str
    loanFTOutput: LoanFT_Output

@app.post("/api/v1/loan/ft", response_model=LoanFT_Out, summary="Loan Funds Transfer / Disburse / Repay / Top-up")
def loan_ft(payload: LoanFT_In = Body(...)):
    """
    Handles Loan Disbursement (0033), Loan Repayment (0034), and Loan Top-up (0035).
    Updates loan and linked customer account balances and records transactions.
    """
    try:
        req = payload.LoanFTRequest
        message_id = payload.messageID
        ttype = req.OperationParameters.TransactionType

        if ttype not in {"0033", "0034", "0035"}:
            raise HTTPException(status_code=400, detail="TransactionType must be 0033 (disburse), 0034 (repay), or 0035 (top-up)")

        # Connect to DB
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        loan_account = req.Account.LoanAccount if req.Account else None
        customer_id = req.Posting.CustomerId
        amount = float(req.Posting.Amount)
        narration_map = {
            "0033": "Loan Disbursement",
            "0034": "Loan Repayment", 
            "0035": "Loan Top-up"
        }
        narration = narration_map.get(ttype, "Loan Transaction")
        now = datetime.now()

        # Validate loan account exists
        if not loan_account:
            raise HTTPException(status_code=400, detail="Loan account is required")

        # Get current loan balance and linked customer account number and balances
        cursor.execute("""
            SELECT loan_balance, linked_account_number FROM loans WHERE loan_account = %s FOR UPDATE
        """, (loan_account,))
        loan_row = cursor.fetchone()
        if not loan_row:
            raise HTTPException(status_code=404, detail="Loan account not found")
        
        current_loan_balance = float(loan_row["loan_balance"])
        linked_account = loan_row["linked_account_number"]

        cursor.execute("""
            SELECT book_balance, cleared_balance FROM members WHERE account_number = %s FOR UPDATE
        """, (linked_account,))
        member_row = cursor.fetchone()
        if not member_row:
            raise HTTPException(status_code=404, detail="Linked customer account not found")
        
        current_book_balance = float(member_row["book_balance"])
        current_cleared_balance = float(member_row["cleared_balance"])

        # Initialize variables for response
        new_loan_balance = current_loan_balance
        new_book_balance = current_book_balance
        new_cleared_balance = current_cleared_balance

        # Process transaction based on type
        if ttype == "0033":  # Loan Disbursement (credit loan, credit customer account)
            new_loan_balance = current_loan_balance + amount
            new_book_balance = current_book_balance + amount
            new_cleared_balance = current_cleared_balance + amount

            cursor.execute("""
                UPDATE loans SET loan_balance = %s WHERE loan_account = %s
            """, (new_loan_balance, loan_account))

            cursor.execute("""
                UPDATE members SET book_balance = %s, cleared_balance = %s WHERE account_number = %s
            """, (new_book_balance, new_cleared_balance, linked_account))

            cursor.execute("""
                INSERT INTO loan_transactions (loan_account, transaction_date, transaction_reference, transaction_type, amount, narration)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (loan_account, now, f"Ref_{message_id}", ttype, amount, narration))

            cursor.execute("""
                INSERT INTO transactions (account_number, transaction_date, transaction_reference, debit_credit, amount, narration, posting_date, book_balance, cleared_balance, channel_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (linked_account, now, f"Ref_{message_id}", "CR", amount, narration, now, new_book_balance, new_cleared_balance, "LN01"))

        elif ttype == "0034":  # Loan Repayment (debit customer account, reduce loan balance)
            if current_book_balance < amount or current_cleared_balance < amount:
                raise HTTPException(status_code=400, detail="Insufficient funds in linked customer account for repayment/top-up")

            new_loan_balance = max(current_loan_balance - amount, 0.0)
            new_book_balance = current_book_balance - amount
            new_cleared_balance = current_cleared_balance - amount

            cursor.execute("""
                UPDATE loans SET loan_balance = %s WHERE loan_account = %s
            """, (new_loan_balance, loan_account))

            cursor.execute("""
                UPDATE members SET book_balance = %s, cleared_balance = %s WHERE account_number = %s
            """, (new_book_balance, new_cleared_balance, linked_account))

            cursor.execute("""
                INSERT INTO loan_transactions (loan_account, transaction_date, transaction_reference, transaction_type, amount, narration)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (loan_account, now, f"Ref_{message_id}", ttype, amount, narration))

            cursor.execute("""
                INSERT INTO transactions (account_number, transaction_date, transaction_reference, debit_credit, amount, narration, posting_date, book_balance, cleared_balance, channel_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (linked_account, now, f"Ref_{message_id}", "DR", amount, narration, now, new_book_balance, new_cleared_balance, "LN01"))
        elif ttype == "0035":  # Loan Top-up (credit loan, credit customer account - SAME AS DISBURSEMENT)
            new_loan_balance = current_loan_balance + amount
            new_book_balance = current_book_balance + amount
            new_cleared_balance = current_cleared_balance + amount
            
            # Update loan balance
            cursor.execute("""
                UPDATE loans SET loan_balance = %s WHERE loan_account = %s
            """, (new_loan_balance, loan_account))
            
            # Update member balance
            cursor.execute("""
                UPDATE members SET book_balance = %s, cleared_balance = %s WHERE account_number = %s
            """, (new_book_balance, new_cleared_balance, linked_account))
            
            # Record loan transaction
            cursor.execute("""
                INSERT INTO loan_transactions (loan_account, transaction_date, transaction_reference, transaction_type, amount, narration)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (loan_account, now, f"Ref_{message_id}", ttype, amount, narration))
            
            # Record member transaction (CREDIT for top-up)
            cursor.execute("""
                INSERT INTO transactions (account_number, transaction_date, transaction_reference, debit_credit, amount, narration, posting_date, book_balance, cleared_balance, channel_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (linked_account, now, f"Ref_{message_id}", "CR", amount, narration, now, new_book_balance, new_cleared_balance, "LN01"))

        conn.commit()
        cursor.close()
        conn.close()

        return LoanFT_Out(
            messageID=message_id,
            statusCode="200",
            statusDescription=f"Successful {narration.lower()}",
            loanFTOutput=LoanFT_Output(
                OperationParameters=LoanFT_ReplyOperation(
                    TransactionReference=f"Ref_{message_id}",
                    TransactionDate=now.isoformat(),
                    TransactionType=ttype,
                ),
                Account=LoanFT_ReplyAccount(
                    LoanAccount=loan_account,
                    LoanBalance=str(new_loan_balance),
                    LinkedAccount=linked_account,
                    LinkedAccountBookBalance=str(new_book_balance),
                    LinkedAccountClearedBalance=str(new_cleared_balance),
                ),
                Posting={"CustomerId": customer_id} if customer_id else None,
                Institution=req.Institution,
            ),
        )

    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid LoanFT payload or DB error: {str(e)}")

    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid LoanFT payload or DB error: {str(e)}")