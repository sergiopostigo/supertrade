-- Get the count of exports by year of shipment
WITH peru_exports_corrected as ( SELECT COALESCE(hs2.heading, hs1.heading) as heading,
                                        COALESCE(hs1.description, hs2.description) as description,
                                        exp.description as details,
                                        exp.exp_id as exporter_id,
                                        net_weight,
                                        gross_weight,
                                        value_usd,
                                        country,
                                        TO_DATE(boarding_date, 'YYYYmmdd') AS boarding_date,
                                        batch_week
        FROM peru_exports exp
        LEFT JOIN peru_exports_headings hs1 ON exp.heading = hs1.heading
        LEFT JOIN peru_exports_headings hs2 ON hs1.mapped_to = hs2.heading)

SELECT CAST(DATE_PART('YEAR',boarding_date) AS INTEGER) AS year,
       COUNT(*) AS count,
       ROUND(COUNT(*)*1.0/(SELECT COUNT(*) FROM peru_exports_corrected)*100,4) as proportion
FROM peru_exports_corrected
GROUP BY year
