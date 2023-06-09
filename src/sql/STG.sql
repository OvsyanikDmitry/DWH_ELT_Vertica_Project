--STAGING
--CURRENCIES
DROP TABLE IF EXISTS OVSYANIKDMITRYYANDEXRU__STAGING.CURRENCIES CASCADE;
CREATE TABLE OVSYANIKDMITRYYANDEXRU__STAGING.CURRENCIES
(
    
    currency_code INT not null,
    currency_code_with INT not null,
    date_update DATE not null,
    currency_with_div FLOAT not null
)
ORDER BY date_update
SEGMENTED BY hash (date_update::DATE,currency_code) ALL nodes
PARTITION BY date_update::DATE;


--TRANSACTIONS
DROP TABLE IF EXISTS OVSYANIKDMITRYYANDEXRU__STAGING.TRANSACTIONS CASCADE;

CREATE TABLE OVSYANIKDMITRYYANDEXRU__STAGING.TRANSACTIONS
(
    operation_id VARCHAR(255) not null,
    account_number_from INT not null,
    account_number_to INT not null,
    currency_code INT not null,
	country VARCHAR(100) not null,
	status VARCHAR(100) not null,
	transaction_type VARCHAR(100) not null,
	amount INT not null,
	transaction_dt DATETIME not null
)
ORDER BY transaction_dt,operation_id
SEGMENTED BY date_part('month', transaction_dt) ALL nodes
PARTITION BY date_part('day', transaction_dt);


CREATE PROJECTION OVSYANIKDMITRYYANDEXRU__STAGING.TRANSACTIONS_PROJ_DT as
SELECT
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
	country,
	status,
	transaction_type,
	amount,
	transaction_dt
FROM
    OVSYANIKDMITRYYANDEXRU__STAGING.TRANSACTIONS
ORDER BY transaction_dt;

