/* Get all the distinct available headings after all filters */

WITH peru_exports_corrected as ( SELECT COALESCE(hs2.heading, hs1.heading) as heading,
                                        COALESCE(hs1.description, hs2.description) as description,
                                        exp.description as details,
                                        exp.exp_id as exporter_id,
                                        exp.net_weight,
                                        exp.gross_weight,
                                        exp.value_usd,
                                        exp.country,
                                        TO_DATE(exp.boarding_date, 'YYYYmmdd') AS boarding_date,
                                        exp.batch_week
        FROM peru_exports exp
        LEFT JOIN peru_exports_headings hs1 ON exp.heading = hs1.heading
        LEFT JOIN peru_exports_headings hs2 ON hs1.mapped_to = hs2.heading
        WHERE DATE_PART('YEAR',TO_DATE(exp.boarding_date, 'YYYYmmdd')) >= :year_threshold
        AND exp.value_usd >= :value_usd_threshold
        AND exp.net_weight >= :net_weight_threshold)

SELECT DISTINCT heading
FROM peru_exports_corrected
WHERE heading IN (
    SELECT heading
    FROM peru_exports_corrected
    GROUP BY heading
    HAVING COUNT(*) >= :headings_count_threshold
)
ORDER BY heading ASC