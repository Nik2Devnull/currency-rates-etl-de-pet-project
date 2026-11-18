INSERT INTO core.currency_rates (rate_date, char_code, name, nominal, rate)
SELECT
    r.rate_date,
    v.value ->> 'CharCode' AS char_code,
    v.value ->> 'Name' AS name,
    (v.value ->> 'Nominal')::int AS nominal,
    (v.value ->> 'Value')::numeric AS rate
FROM stg.currency_rates_raw AS r
CROSS JOIN LATERAL jsonb_each(r.payload -> 'Valute') AS v(key, value)
WHERE r.rate_date = %(ds)s::date 
    AND r.loaded_at = (
        SELECT max(r2.loaded_at)
        FROM stg.currency_rates_raw r2
        WHERE r2.rate_date = r.rate_date
    )
ON CONFLICT (rate_date, char_code) DO UPDATE
SET name = EXCLUDED.name,
    nominal = EXCLUDED.nominal,
    rate = EXCLUDED.rate,
    updated_at = now();
