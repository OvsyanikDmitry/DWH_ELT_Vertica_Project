INSERT INTO OVSYANIKDMITRYYANDEXRU__DWH.GLOBAL_METRICS (
        date_update,
        currency_from,
        amount_total,
        cnt_transactions,
        avg_transactions_per_account,
        cnt_accounts_make_transactions
    )
SELECT
    t.transaction_dt::DATE AS date_update,
    t.currency_code AS currency_from,
    CASE WHEN t.currency_code = 420 THEN SUM(t.amount)
    ELSE SUM(t.amount * c.currency_with_div)
    END AS amount_total,
    COUNT(t.*) AS cnt_transactions,
    amount_total / COUNT(DISTINCT t.account_number_from) AS avg_transactions_per_account,
    COUNT(DISTINCT t.account_number_from) AS cnt_accounts_make_transactions
FROM
    OVSYANIKDMITRYYANDEXRU__STAGING.transactions t
    LEFT JOIN ( SELECT * FROM OVSYANIKDMITRYYANDEXRU__STAGING.currencies
       			 WHERE currency_code_with = 420) c
    ON t.currency_code = c.currency_code
    AND t.transaction_dt::DATE = c.date_update WHERE t.status = 'done'
    AND t.account_number_from > 0
    AND t.account_number_to > 0
    AND t.transaction_dt::DATE = '{date}'
    AND t.transaction_type in (
        'c2a_incoming',
        'c2b_partner_incoming',
        'sbp_incoming',
        'sbp_outgoing',
        'transfer_incoming',
        'transfer_outgoing'
    )
GROUP BY
    t.transaction_dt::DATE,
    t.currency_code
ORDER BY 2
;