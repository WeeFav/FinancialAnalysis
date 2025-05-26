-- Revenue CAGR
WITH
    cik_lookup AS (
        SELECT "CIK" FROM ticker_cik_mapping
        WHERE "Symbol" = {{Symbol}}
    ),
	revenue AS (
		SELECT folder, form, period, fy, fp, fs_num.adsh, tag, ddate, qtrs, uom, segments, value
		FROM fs_num
		JOIN fs_sub ON fs_num.adsh = fs_sub.adsh
		WHERE
			cik = (SELECT "CIK" FROM cik_lookup) AND
			tag IN ('RevenueFromContractWithCustomerExcludingAssessedTax', 'SalesRevenueNet', 'Revenues') AND
			(fp = 'FY' AND qtrs = 4) AND
			segments ISNULL AND
			EXTRACT(MONTH FROM period) = EXTRACT(MONTH FROM ddate)
		ORDER BY ddate DESC
		LIMIT 5
	)
SELECT
	(POWER((lastest.value / earliest.value), 0.2) - 1)
FROM
	(SELECT value FROM revenue WHERE fy = (SELECT MAX(fy) FROM revenue)) as lastest,
	(SELECT value FROM revenue WHERE fy = (SELECT MIN(fy) FROM revenue)) as earliest

-- Net Income CAGR
WITH
    cik_lookup AS (
        SELECT "CIK" FROM ticker_cik_mapping
        WHERE "Symbol" = {{Symbol}}
    ),
	netincome AS (
		SELECT folder, form, period, fy, fp, fs_num.adsh, tag, ddate, qtrs, uom, segments, value
		FROM fs_num
		JOIN fs_sub ON fs_num.adsh = fs_sub.adsh
		WHERE
			cik = (SELECT "CIK" FROM cik_lookup) AND
			tag IN ('NetIncomeLoss') AND
			(fp = 'FY' AND qtrs = 4) AND
			segments ISNULL AND
			EXTRACT(MONTH FROM period) = EXTRACT(MONTH FROM ddate)
		ORDER BY ddate DESC
		LIMIT 5
	)
SELECT
	(POWER((lastest.value / earliest.value), 0.2) - 1)
FROM
	(SELECT value FROM netincome WHERE fy = (SELECT MAX(fy) FROM netincome)) as lastest,
	(SELECT value FROM netincome WHERE fy = (SELECT MIN(fy) FROM netincome)) as earliest


-- Revenue
WITH
    cik_lookup AS (
        SELECT "CIK" FROM ticker_cik_mapping
        WHERE "Symbol" = {{Symbol}}
    ),
	revenue AS (
		SELECT folder, form, period, fy, fp, fs_num.adsh, tag, ddate, qtrs, uom, segments, value
		FROM fs_num
		JOIN fs_sub ON fs_num.adsh = fs_sub.adsh
		WHERE
			cik = (SELECT "CIK" FROM cik_lookup) AND
			tag IN ('RevenueFromContractWithCustomerExcludingAssessedTax', 'SalesRevenueNet', 'Revenues') AND
			((fp = 'FY' AND qtrs = 4) OR (fp <> 'FY' AND qtrs = 1)) AND
			segments ISNULL AND
			EXTRACT(MONTH FROM period) = EXTRACT(MONTH FROM ddate)
	)
SELECT
	curr.fy,
    curr.fp,
    CONCAT(curr.fp, ' ', curr.fy) AS "Quater",
	curr.tag,
	curr.value AS curr_revenue,
	prev.value AS prev_revenue,
	((curr.value - prev.value) / prev.value) * 100 AS revenue_growth_rate
FROM revenue AS curr
LEFT JOIN revenue AS prev ON (curr.fp = prev.fp) AND (curr.fy = prev.fy + 1)
WHERE curr.fp = 
    CASE
        WHEN {{Quater}} = 'All' THEN curr.fp
        ELSE {{Quater}}
    END
ORDER BY curr.ddate ASC

-- Net Income
WITH
    cik_lookup AS (
        SELECT "CIK" FROM ticker_cik_mapping
        WHERE "Symbol" = {{Symbol}}
    ),
	netincome AS (
		SELECT folder, form, period, fy, fp, fs_num.adsh, tag, ddate, qtrs, uom, segments, value
		FROM fs_num
		JOIN fs_sub ON fs_num.adsh = fs_sub.adsh
		WHERE
			cik = (SELECT "CIK" FROM cik_lookup) AND
			tag IN ('NetIncomeLoss') AND
			((fp = 'FY' AND qtrs = 4) OR (fp <> 'FY' AND qtrs = 1)) AND
			segments ISNULL AND
			EXTRACT(MONTH FROM period) = EXTRACT(MONTH FROM ddate)
	)
SELECT
	curr.fy,
    curr.fp,
    CONCAT(curr.fp, ' ', curr.fy) AS "Quater",
	curr.tag,
	curr.value AS curr_netincome,
	prev.value AS prev_netincome,
	((curr.value - prev.value) / prev.value) * 100 AS netincome_growth_rate
FROM netincome AS curr
LEFT JOIN netincome AS prev ON (curr.fp = prev.fp) AND (curr.fy = prev.fy + 1)
WHERE curr.fp = 
    CASE
        WHEN {{Quater}} = 'All' THEN curr.fp
        ELSE {{Quater}}
    END
ORDER BY curr.ddate ASC

-- Revenue Growth Rate
WITH
    cik_lookup AS (
        SELECT "CIK" FROM ticker_cik_mapping
        WHERE "Symbol" = {{Symbol}}
    ),
	revenue AS (
		SELECT folder, form, period, fy, fp, fs_num.adsh, tag, ddate, qtrs, uom, segments, value
		FROM fs_num
		JOIN fs_sub ON fs_num.adsh = fs_sub.adsh
		WHERE
			cik = (SELECT "CIK" FROM cik_lookup) AND
			tag IN ('RevenueFromContractWithCustomerExcludingAssessedTax', 'SalesRevenueNet', 'Revenues') AND
			((fp = 'FY' AND qtrs = 4) OR (fp <> 'FY' AND qtrs = 1)) AND
			segments ISNULL AND
			EXTRACT(MONTH FROM period) = EXTRACT(MONTH FROM ddate)
	)
SELECT
	curr.fy,
    curr.fp,
    CONCAT(curr.fp, ' ', curr.fy) AS "Quater",
	curr.tag,
	curr.value AS curr_revenue,
	prev.value AS prev_revenue,
	((curr.value - prev.value) / prev.value) * 100 AS revenue_growth_rate
FROM revenue AS curr
LEFT JOIN revenue AS prev ON (curr.fp = prev.fp) AND (curr.fy = prev.fy + 1)
WHERE curr.fp = 
    CASE
        WHEN {{Quater}} = 'All' THEN curr.fp
        ELSE {{Quater}}
    END
ORDER BY curr.ddate ASC

-- Net Income Growth Rate
WITH
    cik_lookup AS (
        SELECT "CIK" FROM ticker_cik_mapping
        WHERE "Symbol" = {{Symbol}}
    ),
	netincome AS (
		SELECT folder, form, period, fy, fp, fs_num.adsh, tag, ddate, qtrs, uom, segments, value
		FROM fs_num
		JOIN fs_sub ON fs_num.adsh = fs_sub.adsh
		WHERE
			cik = (SELECT "CIK" FROM cik_lookup) AND
			tag IN ('NetIncomeLoss') AND
			((fp = 'FY' AND qtrs = 4) OR (fp <> 'FY' AND qtrs = 1)) AND
			segments ISNULL AND
			EXTRACT(MONTH FROM period) = EXTRACT(MONTH FROM ddate)
	)
SELECT
	curr.fy,
    curr.fp,
    CONCAT(curr.fp, ' ', curr.fy) AS "Quater",
	curr.tag,
	curr.value AS curr_netincome,
	prev.value AS prev_netincome,
	((curr.value - prev.value) / prev.value) * 100 AS netincome_growth_rate
FROM netincome AS curr
LEFT JOIN netincome AS prev ON (curr.fp = prev.fp) AND (curr.fy = prev.fy + 1)
WHERE curr.fp = 
    CASE
        WHEN {{Quater}} = 'All' THEN curr.fp
        ELSE {{Quater}}
    END
ORDER BY curr.ddate ASC

-- Margins
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
			tag IN ('RevenueFromContractWithCustomerExcludingAssessedTax', 'SalesRevenueNet', 'Revenues', 'CostOfGoodsAndServicesSold', 'CostOfRevenue', 'ResearchAndDevelopmentExpense', 'SellingGeneralAndAdministrativeExpense', 'SellingAndMarketingExpense', 'GeneralAndAdministrativeExpense', 'NetIncomeLoss') AND
			((fp = 'FY' AND qtrs = 4) OR (fp <> 'FY' AND qtrs = 1)) AND
			segments ISNULL AND
			EXTRACT(MONTH FROM period) = EXTRACT(MONTH FROM ddate)
	),
	revenue AS (
		SELECT
			folder, form, period, fy, fp, adsh, ddate, qtrs, uom, segments, tag, value AS revenue
		FROM filtered 
		WHERE 
			tag IN ('RevenueFromContractWithCustomerExcludingAssessedTax', 'SalesRevenueNet', 'Revenues')
	),
	cogs AS (
		SELECT
			adsh, value AS cogs
		FROM filtered 
		WHERE 
			tag IN ('CostOfGoodsAndServicesSold', 'CostOfRevenue')
	),
	operating_cost AS (
		SELECT
			MAX(adsh) AS adsh, SUM(value) AS operating_cost
		FROM filtered 
		WHERE 
			tag IN ('ResearchAndDevelopmentExpense', 'SellingGeneralAndAdministrativeExpense', 'SellingAndMarketingExpense', 'GeneralAndAdministrativeExpense')
        GROUP BY adsh
	),
	netincome AS (
		SELECT
			adsh, value AS netincome
		FROM filtered 
		WHERE 
			tag IN ('NetIncomeLoss')
	)
SELECT
	fy,
    fp,
    CONCAT(fp, ' ', fy) AS "Quater",
	revenue,
	cogs,
	operating_cost,
	netincome,
	(revenue - cogs) / revenue AS "Gross Profit Margin",
	(revenue - cogs - operating_cost) / revenue AS "Operating Profit Margin",
	(netincome) / revenue AS "Net Profit Margin"
FROM revenue
JOIN cogs ON revenue.adsh = cogs.adsh
JOIN operating_cost ON revenue.adsh = operating_cost.adsh
JOIN netincome ON revenue.adsh = netincome.adsh
WHERE fp = 
    CASE
        WHEN {{Quater}} = 'All' THEN fp
        ELSE {{Quater}}
    END
ORDER BY ddate ASC

-- D/E, Current Ratio
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
			tag IN ('AssetsCurrent', 'LiabilitiesCurrent', 'Liabilities', 'Assets') AND
			segments ISNULL AND
			EXTRACT(MONTH FROM period) = EXTRACT(MONTH FROM ddate)
	),
	assets AS (
		SELECT
			folder, form, period, fy, fp, adsh, ddate, qtrs, uom, segments, tag, value AS assets
		FROM filtered 
		WHERE 
			tag IN ('Assets')
	),
	current_assets AS (
		SELECT
			adsh, value AS current_assets
		FROM filtered 
		WHERE 
			tag IN ('AssetsCurrent')
	),
	liabilities AS (
		SELECT
			adsh, value AS liabilities
		FROM filtered 
		WHERE 
			tag IN ('Liabilities')
	),
	current_liabilities AS (
		SELECT
			adsh, value AS current_liabilities
		FROM filtered 
		WHERE 
			tag IN ('LiabilitiesCurrent')
	)
SELECT
	fy,
	fp,
	CONCAT(fp, ' ', fy) AS "Quater",
	current_assets,
	assets,
	current_liabilities,
	liabilities,
    liabilities / (assets - liabilities) AS "Debt to Equity Ratio",
    current_assets / current_liabilities AS "Current Ratio"
FROM assets
JOIN current_assets ON assets.adsh = current_assets.adsh
JOIN liabilities ON assets.adsh = liabilities.adsh
JOIN current_liabilities ON assets.adsh = current_liabilities.adsh
WHERE fp = 
    CASE
        WHEN {{Quater}} = 'All' THEN fp
        ELSE {{Quater}}
    END
ORDER BY ddate ASC

-- ROE, ROA
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
			((tag IN ('Assets', 'Liabilities')) OR (tag = 'NetIncomeLoss' AND ((fp = 'FY' AND qtrs = 4) OR (fp <> 'FY' AND qtrs = 1)))) AND
			segments ISNULL AND
			EXTRACT(MONTH FROM period) = EXTRACT(MONTH FROM ddate)
	),
	assets AS (
		SELECT
			folder, form, period, fy, fp, adsh, ddate, qtrs, uom, segments, tag, value AS assets
		FROM filtered 
		WHERE 
			tag = 'Assets'
	),
	liabilities AS (
		SELECT
			adsh, value AS liabilities
		FROM filtered 
		WHERE 
			tag = 'Liabilities'
	),
	netincome AS (
		SELECT
			adsh, value AS netincome
		FROM filtered 
		WHERE 
			tag = 'NetIncomeLoss'
	)
SELECT
	fy,
	fp,
	CONCAT(fp, ' ', fy) AS "Quater",
	assets,
	liabilities,
    netincome,
    netincome / (assets - liabilities) AS "Return On Equity",
    netincome / assets AS "Return On Assets"
FROM assets
JOIN liabilities ON assets.adsh = liabilities.adsh
JOIN netincome ON assets.adsh = netincome.adsh
WHERE fp = 
    CASE
        WHEN {{Quater}} = 'All' THEN fp
        ELSE {{Quater}}
    END
ORDER BY ddate ASC

-- Cash Flow
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
			tag IN ('NetCashProvidedByUsedInOperatingActivities', 'NetCashProvidedByUsedInOperatingActivitiesContinuingOperations', 'NetCashProvidedByUsedInInvestingActivities', 'NetCashProvidedByUsedInInvestingActivitiesContinuingOperations') AND
			qtrs = 
				CASE fp
					WHEN 'Q1' THEN 1
					WHEN 'Q2' THEN 2
					WHEN 'Q3' THEN 3
					WHEN 'FY' THEN 4
				END AND
			segments ISNULL AND
			EXTRACT(MONTH FROM period) = EXTRACT(MONTH FROM ddate)
	),
	ocf AS (
		SELECT folder, form, period, fy, fp, adsh, tag, ddate, qtrs, uom, segments, value AS ocf
		FROM filtered
		WHERE tag IN ('NetCashProvidedByUsedInOperatingActivities', 'NetCashProvidedByUsedInOperatingActivitiesContinuingOperations')
	),
	icf AS (
		SELECT adsh, value AS icf
		FROM filtered
		WHERE tag IN ('NetCashProvidedByUsedInInvestingActivities', 'NetCashProvidedByUsedInInvestingActivitiesContinuingOperations')
	)
SELECT
	fy,
	fp,
	CONCAT(fp, ' ', fy) AS "Quater",
	ocf AS "Operating Cash Flow",
	icf AS "Capital Expenditure",
	(ocf - icf) AS "Free Cash Flow"
FROM ocf
JOIN icf oN ocf.adsh = icf.adsh
WHERE fp = 
    CASE
        WHEN {{Quater}} = 'All' THEN fp
        ELSE {{Quater}}
    END
ORDER BY ddate ASC

-- P/E Ratio

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
			tag = 'EarningsPerShareBasic' AND
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
ORDER BY ddate ASC

-- P/B Ratio
WITH
    cik_lookup AS (
        SELECT "CIK" FROM ticker_cik_mapping
        WHERE "Symbol" = {{Symbol}}
    ),
	fitered AS (
		SELECT folder, form, period, fy, fp, fs_num.adsh, tag, ddate, qtrs, uom, segments, value
		FROM fs_num
		JOIN fs_sub ON fs_num.adsh = fs_sub.adsh
		WHERE
			cik = (SELECT "CIK" FROM cik_lookup) AND
			tag IN ('Assets', 'Liabilities', 'CommonStockSharesOutstanding') AND
			segments ISNULL AND
			EXTRACT(MONTH FROM period) = EXTRACT(MONTH FROM ddate)
	),
	assets AS (
		SELECT folder, form, period, fy, fp, adsh, tag, ddate, qtrs, uom, segments, value AS assets
		FROM fitered
		WHERE tag = 'Assets'
	),
	liabilities AS (
		SELECT adsh, value as liabilities
		FROM fitered
		WHERE tag = 'Liabilities'
	),
	shares AS (
		SELECT adsh, value as shares
		FROM fitered
		WHERE tag = 'CommonStockSharesOutstanding'
	),
	bvps_stock_ranked AS (
		SELECT
			*,
			RANK() OVER(PARTITION BY folder ORDER BY stock."Date" DESC) AS rank_num
		FROM assets
		JOIN liabilities ON assets.adsh = liabilities.adsh
		JOIN shares ON assets.adsh = shares.adsh
		JOIN stock ON (stock."Date" <= assets.ddate) AND (stock."Date" >= DATE(assets.ddate) - INTERVAL '10 day')
		WHERE stock."Ticker" = {{Symbol}}
	)
SELECT
	CONCAT(fp, ' ', fy) AS "Quater",
	assets,
	liabilities,
	shares,
	(assets - liabilities) / shares AS "Book Value Per Share",
	bvps_stock_ranked."Close" / ((assets - liabilities) / shares) AS "P/B Ratio"
FROM bvps_stock_ranked
WHERE 
    rank_num = 1 AND
    fp = 
    CASE
        WHEN {{Quater}} = 'All' THEN fp
        ELSE {{Quater}}
    END
ORDER BY ddate ASC

-- Market Cap
WITH
    cik_lookup AS (
        SELECT "CIK" FROM ticker_cik_mapping
        WHERE "Symbol" = {{Symbol}}
    ),
	shares AS (
		SELECT folder, form, period, fy, fp, fs_num.adsh, tag, ddate, qtrs, uom, segments, value
		FROM fs_num
		JOIN fs_sub ON fs_num.adsh = fs_sub.adsh
		WHERE
			cik = (SELECT "CIK" FROM cik_lookup) AND
			tag = 'CommonStockSharesOutstanding' AND
			segments ISNULL AND
			EXTRACT(MONTH FROM period) = EXTRACT(MONTH FROM ddate)
	),
	shares_stock_ranked AS (
		SELECT
			*,
			RANK() OVER(PARTITION BY folder ORDER BY stock."Date" DESC) AS rank_num
		FROM shares
		JOIN stock ON (stock."Date" <= shares.ddate) AND (stock."Date" >= DATE(shares.ddate) - INTERVAL '10 day')
		WHERE stock."Ticker" = {{Symbol}}
	)
SELECT
	CONCAT(fp, ' ', fy) AS "Quater",
	value as "Number of Shares Outstanding",
	value * shares_stock_ranked."Close" AS "Market Capitalization"
FROM shares_stock_ranked
WHERE 
    rank_num = 1 AND
    fp = 
    CASE
        WHEN {{Quater}} = 'All' THEN fp
        ELSE {{Quater}}
    END
ORDER BY ddate ASC

-- Compare S&P500
WITH
	company AS (
        SELECT stock."Date" AS date, stock."Close" as price
        FROM stock
        WHERE stock."Ticker" = {{Symbol}}
	),
	sp AS (
        SELECT stock."Date" AS date, stock."Close" as price
        FROM stock
        WHERE stock."Ticker" = '^GSPC'
	)
SELECT 
	company.date AS "Date",
	(company.price - cb.price) / cb.price AS "Company Stock Growth",
	(sp.price - spb.price) / spb.price AS "S&P 500 Index Growth"
FROM company
JOIN sp ON company.date = sp.date
JOIN (SELECT price FROM company WHERE date = (SELECT MIN(date) FROM company)) AS cb ON 1 = 1
JOIN (SELECT price FROM sp WHERE date = (SELECT MIN(date) FROM sp)) AS spb ON 1 = 1
ORDER BY company.date DESC

-- Dividend Yield
WITH
    cik_lookup AS (
        SELECT "CIK" FROM ticker_cik_mapping
        WHERE "Symbol" = {{Symbol}}
    ),
	dividend AS (
		SELECT folder, form, period, fy, fp, fs_num.adsh, tag, ddate, qtrs, uom, segments, value
		FROM fs_num
		JOIN fs_sub ON fs_num.adsh = fs_sub.adsh
		WHERE
			cik = (SELECT "CIK" FROM cik_lookup) AND
			tag = 'CommonStockDividendsPerShareDeclared' AND
			((fp = 'FY' AND qtrs = 4) OR (fp <> 'FY' AND qtrs = 1)) AND
			segments ISNULL AND
			EXTRACT(MONTH FROM period) = EXTRACT(MONTH FROM ddate)
	),
	dividend_stock_ranked AS (
		SELECT
			*,
			RANK() OVER(PARTITION BY folder ORDER BY stock."Date" DESC) AS rank_num
		FROM dividend
		JOIN stock ON (stock."Date" <= dividend.ddate) AND (stock."Date" >= DATE(dividend.ddate) - INTERVAL '10 day')
		WHERE stock."Ticker" = {{Symbol}}
	)
SELECT
	CONCAT(fp, ' ', fy) AS "Quater",
	value AS "Dividend",
	dividend_stock_ranked."Close" AS "Stock Price",
	value / dividend_stock_ranked."Close" AS "Dividend Yield"
FROM dividend_stock_ranked
WHERE 
    rank_num = 1 AND
    fp = 
    CASE
        WHEN {{Quater}} = 'All' THEN fp
        ELSE {{Quater}}
    END
ORDER BY ddate ASC






















































