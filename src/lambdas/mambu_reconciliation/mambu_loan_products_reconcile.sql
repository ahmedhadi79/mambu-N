SELECT date(creation_date) AS date,
    COUNT(*) AS target_total_rowcount_for_date,
    COUNT(DISTINCT id) as target_total_loan_products_for_date
FROM datalake_raw.loan_products
WHERE date(creation_date) = CAST(format_datetime (current_timestamp - interval '1' day, 'y-M-d') as date)
GROUP BY date(creation_date)
ORDER BY date desc
