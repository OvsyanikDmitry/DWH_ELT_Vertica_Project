--CDM
DROP TABLE IF EXISTS OVSYANIKDMITRYYANDEXRU__DWH.GLOBAL_METRICS CASCADE;
CREATE TABLE OVSYANIKDMITRYYANDEXRU__DWH.GLOBAL_METRICS
(
	date_update DATE not null,
	currency_from VARCHAR not null,
	amount_total NUMERIC(13, 2) not null,
	cnt_transactions INT not null,
	avg_transactions_per_account NUMERIC(13, 2) not null,
	cnt_accounts_make_transactions INT not null
)
ORDER BY date_update
SEGMENTED BY hash(date_update) all nodes
PARTITION BY date_update::date;
