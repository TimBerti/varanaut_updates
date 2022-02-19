CREATE TABLE sector AS
SELECT sector,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY ev_ebit
    ) AS ev_ebit,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY ev_ebitda
    ) AS ev_ebitda,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY price_earnings
    ) AS price_earnings,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY price_ebit
    ) AS price_ebit,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY price_sales
    ) AS price_sales,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY price_cash_flow
    ) AS price_cash_flow,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY price_book
    ) AS price_book,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY dividend_yield
    ) AS dividend_yield,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY payout_ratio
    ) AS payout_ratio,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY gross_profit_margin
    ) AS gross_profit_margin,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY net_profit_margin
    ) AS net_profit_margin,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY operating_cash_flow_margin
    ) AS operating_cash_flow_margin,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY operating_margin
    ) AS operating_margin,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY return_on_assets
    ) AS return_on_assets,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY return_on_net_tangible_assets
    ) AS return_on_net_tangible_assets,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY asset_turnover
    ) AS asset_turnover,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY cash_flow_on_assets
    ) AS cash_flow_on_assets,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY return_on_equity
    ) AS return_on_equity,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY revenue_on_equity
    ) AS revenue_on_equity,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY return_on_capital_employed
    ) AS return_on_capital_employed,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY return_on_capital
    ) AS return_on_capital,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY debt_ratio
    ) AS debt_ratio,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY debt_to_ebit
    ) AS debt_to_ebit,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY debt_to_equity
    ) AS debt_to_equity,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY debt_to_revenue
    ) AS debt_to_revenue,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY current_ratio
    ) AS current_ratio
FROM companies_display
GROUP BY sector;