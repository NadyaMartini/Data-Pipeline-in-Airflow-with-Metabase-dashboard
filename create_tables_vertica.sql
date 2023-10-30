CREATE TABLE STV202307035__DWH.global_metrics (
    metric_id IDENTITY(1,1) PRIMARY KEY,
    date_update DATE NOT NULL,
    currency_from INT NOT NULL, 
    amount_total NUMERIC(18, 2) NOT NULL, 
    cnt_transactions INT NOT NULL, 
    avg_transactions_per_account NUMERIC CHECK avg_transactions_per_account >= 0, 
    cnt_accounts_make_transactions INT CHECK cnt_accounts_make_transactions >= 0 
);

CREATE PROJECTION STV202307035__DWH.global_metrics_projection
  AS SELECT * FROM STV202307035__DWH.global_metrics
  SEGMENTED BY HASH(date_update, currency_from) 
  ORDER BY date_update
  ALL NODES;


CREATE TABLE STV202307035__STAGING.transactions (
    transaction_id IDENTITY(1,1) PRIMARY KEY,
    operation_id UUID NOT NULL,
    account_number_from INT NOT NULL,
    account_number_to INT NOT NULL,
    currency_code INT NOT NULL,
    country VARCHAR(255) NOT NULL,
    status VARCHAR(255) NOT NULL,
    transaction_type VARCHAR(255) NOT NULL,
    amount NUMERIC(18, 2) CHECK amount > 0,
    transaction_dt TIMESTAMP NOT NULL
);

CREATE PROJECTION STV202307035__STAGING.transactions_projection
  AS SELECT * FROM STV202307035__STAGING.transactions
  SEGMENTED BY HASH(transaction_dt, account_number_from)
  ORDER BY transaction_dt
  ALL NODES;


CREATE TABLE STV202307035__STAGING.currencies (
    currencies_id IDENTITY(1,1) PRIMARY KEY,
    currency_code INT NOT NULL,          
    currency_code_with INT NOT NULL,     
    date_update DATE NOT NULL,             
    currency_with_div NUMERIC(10, 2) CHECK currency_with_div > 0
);

CREATE PROJECTION STV202307035__STAGING.currencies_projection
  AS SELECT * FROM STV202307035__STAGING.currencies
  SEGMENTED BY HASH(date_update, currency_code)
  ORDER BY date_update
  ALL NODES;



# код для агрегации витрины  

WITH avg_per_account AS (
    SELECT 
        CAST(transaction_dt as DATE) AS date_update,
        currency_code,
        account_number_from,
        AVG(amount) AS avg_amount_per_account
    FROM stg.transactions
    GROUP BY account_number_from, CAST(transaction_dt as DATE), currency_code
)
SELECT 
    CAST(tr.transaction_dt as DATE) AS date_update, 
    tr.currency_code AS currency_from, 
    SUM(tr.amount * cu.currency_with_div) AS amount_total,
    COUNT(tr.operation_id) AS cnt_transactions, 
    AVG(a.avg_amount_per_account) AS avg_amount_per_account,
    COUNT(DISTINCT tr.account_number_from) AS cnt_accounts_make_transactions
FROM stg.transactions AS tr    
LEFT JOIN stg.currencies AS cu 
    ON tr.currency_code = cu.currency_code
    AND CAST(tr.transaction_dt as DATE) = cu.date_update
LEFT JOIN avg_per_account AS a 
    ON tr.account_number_from = a.account_number_from
    AND CAST(tr.transaction_dt as DATE) = a.date_update
WHERE cu.currency_code_with = '470'
GROUP BY CAST(tr.transaction_dt as DATE), tr.currency_code;
