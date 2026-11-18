INSERT INTO mart.currency_rates_daily (rate_date, char_code, rate_per_unit, prev_rate, change_pct, avg_7d)
SELECT
    rate_date,
    char_code,
    rate_per_unit,
    prev_rate,
    ROUND((rate_per_unit - prev_rate) / NULLIF(prev_rate, 0) * 100, 4) AS change_pct,
    avg_7d
FROM (
    SELECT
        rate_date,
        char_code,
        rate / nominal AS rate_per_unit,
        LAG(rate / nominal) OVER w AS prev_rate,
        AVG(rate / nominal) OVER (
            PARTITION BY char_code
            ORDER BY rate_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS avg_7d
    FROM core.currency_rates
    WINDOW w AS (PARTITION BY char_code ORDER BY rate_date)
) tmp
WHERE rate_date = %(ds)s::date
ON CONFLICT (rate_date, char_code) DO UPDATE
SET rate_per_unit = EXCLUDED.rate_per_unit,
    prev_rate = EXCLUDED.prev_rate,
    change_pct = EXCLUDED.change_pct,
    avg_7d = EXCLUDED.avg_7d;
