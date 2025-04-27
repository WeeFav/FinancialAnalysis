-- Revenue & Net Income
WITH
    cik_lookup AS (
        SELECT "CIK" FROM ticker_cik_mapping
        WHERE "Symbol" = {{Symbol}}
    ),
	filtered AS (
		SELECT folder, form, period, fy, fp, fs_num.adsh, tag, ddate, qtrs, uom, segments, value
		FROM fs_num
		JOIN fs_sub ON fs_num.adsh = fs_sub.adsh
		WHERE
			cik = (SELECT "CIK" FROM cik_lookup) AND
			tag IN ('RevenueFromContractWithCustomerExcludingAssessedTax', 'SalesRevenueNet', 'Revenues', 'NetIncomeLoss') AND
			((fp = 'FY' AND qtrs = 4) OR (fp <> 'FY' AND qtrs = 1)) AND
			segments ISNULL AND
			EXTRACT(MONTH FROM period) = EXTRACT(MONTH FROM ddate)
	),
	revenue_netincome AS (
		SELECT
			revenue.folder, revenue.form, revenue.period, revenue.fy, revenue.fp, revenue.adsh, revenue.ddate, revenue.qtrs, revenue.uom, revenue.segments, 
			revenue.tag AS revenue_tag, revenue.value AS revenue,
			netincome.tag AS netincome_tag, netincome.value AS netincome
		FROM filtered AS revenue
		JOIN filtered AS netincome ON revenue.adsh = netincome.adsh
		WHERE 
			revenue.tag IN ('RevenueFromContractWithCustomerExcludingAssessedTax', 'SalesRevenueNet', 'Revenues') AND
			netincome.tag IN ('NetIncomeLoss')
	)
SELECT
	curr.fy,
    curr.fp,
    CONCAT(curr.fp, ' ', curr.fy) AS "Quater",
	curr.revenue_tag,
	curr.revenue AS curr_revenue,
	prev.revenue AS prev_revenue,
	((curr.revenue - prev.revenue) / prev.revenue) * 100 AS revenue_growth_rate,
	curr.netincome_tag,
	curr.netincome AS curr_netincome,
	prev.netincome AS prev_netincome,
	((curr.netincome - prev.netincome) / prev.netincome) * 100 AS netincome_growth_rate
FROM revenue_netincome AS curr
LEFT JOIN revenue_netincome AS prev ON (curr.fp = prev.fp) AND (curr.fy = prev.fy + 1)
ORDER BY curr.ddate ASC















WITH
    cik_lookup AS (
        SELECT "CIK" FROM ticker_cik_mapping
        WHERE "Symbol" = {{Symbol}}
    ),
	eps AS (
		SELECT folder, form, period, fy, fp, fs_num.adsh, tag, ddate, qtrs, uom, segments, value
		FROM fs_num
		JOIN fs_sub ON fs_num.adsh = fs_sub.adsh
		WHERE
			cik = (SELECT "CIK" FROM cik_lookup) AND
			tag = 'CommonStockSharesOutstanding' AND
			((fp = 'FY' AND qtrs = 4) OR (fp <> 'FY' AND qtrs = 1)) AND
			segments ISNULL AND
			EXTRACT(MONTH FROM period) = EXTRACT(MONTH FROM ddate)
	),
	eps_stock_ranked AS (
		SELECT
			*,
			RANK() OVER(PARTITION BY folder ORDER BY stock."Date" DESC) AS rank_num
		FROM eps
		JOIN stock ON (stock."Date" <= eps.ddate) AND (stock."Date" >= DATE(eps.ddate) - INTERVAL '10 day')
		WHERE stock."Ticker" = {{Symbol}}
	)
SELECT
	CONCAT(fp, ' ', fy) AS "Quater",
	value AS "EPS",
	eps_stock_ranked."Close" AS "Stock Price",
	eps_stock_ranked."Close" / value AS "P/E Ratio"
FROM eps_stock_ranked
WHERE 
    rank_num = 1 AND
    fp = 
    CASE
        WHEN {{Quater}} = 'All' THEN fp
        ELSE {{Quater}}
    END
ORDER BY ddate ASC;




WITH 
    cik_lookup AS (
        SELECT "CIK" FROM ticker_cik_mapping
        WHERE "Symbol" = 'AAPL'
    ),
    quater_dates AS (
        SELECT *
        FROM fs_sub
        WHERE cik = (SELECT "CIK" FROM cik_lookup)
    )
SELECT
    *
FROM quater_dates
