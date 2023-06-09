-- Удаляем данные для идемподентности
DELETE FROM OVSYANIKDMITRYYANDEXRU__STAGING.transactions
WHERE transaction_dt::date = '{date}';

-- Загружаем данные
COPY OVSYANIKDMITRYYANDEXRU__STAGING.transactions
FROM LOCAL 'transactions-{date}.csv' DELIMITER ','