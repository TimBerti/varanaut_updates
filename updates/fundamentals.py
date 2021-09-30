import requests
import json
from tqdm import tqdm
from datetime import datetime


# ToDo: update_display, update_quarterly


def get_fundamentals(ticker, API_URL, API_TOKEN):
    '''
    Returns fundamental data of ticker.
    '''

    url = API_URL + f'fundamentals/{ticker}?api_token={API_TOKEN}&fmt=json'

    response = requests.get(url, timeout=60)

    fundamentals = json.loads(response.content)

    return fundamentals


def update_display(db, ticker, fundamentals):
    '''
    Updates companies_display at ticker with fundamentals.
    '''

    values = {}

    values['ticker'] = ticker

    try:
        values[f'website'] = fundamentals['General']['WebURL']
    except:
        values[f'website'] = None
    try:
        values[f'currency'] = fundamentals['General']['CurrencyCode']
    except:
        values[f'currency'] = None
    try:
        values[f'name'] = fundamentals['General']['Name']
    except:
        values[f'name'] = None
    try:
        values[f'country'] = fundamentals['General']['CountryISO']
    except:
        values[f'country'] = None
    try:
        values[f'sector'] = fundamentals['General']['Sector']
    except:
        values[f'sector'] = None
    try:
        values[f'industry'] = fundamentals['General']['Industry']
    except:
        values[f'industry'] = None
    try:
        values[f'ipo'] = fundamentals['General']['IPODate']
    except:
        values[f'ipo'] = None
    try:
        values[f'description'] = fundamentals['General']['Description']
    except:
        values[f'description'] = None
    try:
        values[f'isin'] = fundamentals['General']['ISIN']
    except:
        values[f'isin'] = None
    try:
        values[f'fiscal_year_end'] = fundamentals['General']['FiscalYearEnd']
    except:
        values[f'fiscal_year_end'] = None
    try:
        values[f'gic_sector'] = fundamentals['General']['GicSector']
    except:
        values[f'gic_sector'] = None
    try:
        values[f'gic_group'] = fundamentals['General']['GicGroup']
    except:
        values[f'gic_group'] = None
    try:
        values[f'gic_industry'] = fundamentals['General']['GicIndustry']
    except:
        values[f'gic_industry'] = None
    try:
        values[f'gic_sub_industry'] = fundamentals['General']['GicSubIndustry']
    except:
        values[f'gic_sub_industry'] = None
    try:
        values[f'is_delisted'] = fundamentals['General']['IsDelisted']
    except:
        values[f'is_delisted'] = None
    try:
        values[f'full_time_employees'] = fundamentals['General']['FullTimeEmployees']
    except:
        values[f'full_time_employees'] = None
    try:
        values[f'address'] = fundamentals['General']['Address']
    except:
        values[f'address'] = None

    try:
        values[f'short_interest_ratio'] = fundamentals['SharesStats']['ShortRatio']
    except:
        values[f'short_interest_ratio'] = None
    try:
        values[f'short_ratio_float'] = fundamentals['SharesStats']['ShortPercentFloat']
    except:
        values[f'short_ratio_float'] = None
    try:
        values[f'short_ratio_outstanding'] = fundamentals['SharesStats']['ShortPercentOutstanding']
    except:
        values[f'short_ratio_outstanding'] = None
    try:
        values[f'shares_short'] = fundamentals['SharesStats']['SharesShort']
    except:
        values[f'shares_short'] = None
    try:
        values[f'shares_short_prior_month'] = fundamentals['SharesStats']['SharesShortPriorMonth']
    except:
        values[f'shares_short_prior_month'] = None
    try:
        values[f'shares_held_by_insiders'] = fundamentals['SharesStats']['PercentInsiders']
    except:
        values[f'shares_held_by_insiders'] = None
    try:
        values[f'shares_held_by_institutions'] = fundamentals['SharesStats']['PercentInstitutions']
    except:
        values[f'shares_held_by_institutions'] = None
    try:
        values[f'shares_float'] = fundamentals['SharesStats']['SharesFloat']
    except:
        values[f'shares_float'] = None

    ratings_analyst = {}

    try:
        values[f'avg_rating_analyst'] = fundamentals['AnalystRatings']['Rating']
    except:
        values[f'avg_rating_analyst'] = None
    try:
        values[f'target_price_analyst'] = fundamentals['AnalystRatings']['TargetPrice']
    except:
        values[f'target_price_analyst'] = None
    try:
        ratings_analyst['strong_buy'] = fundamentals['AnalystRatings']['StrongBuy']
        ratings_analyst['buy'] = fundamentals['AnalystRatings']['Buy']
        ratings_analyst['hold'] = fundamentals['AnalystRatings']['Hold']
        ratings_analyst['sell'] = fundamentals['AnalystRatings']['Sell']
        ratings_analyst['strong_sell'] = fundamentals['AnalystRatings']['StrongSell']
    except:
        ratings_analyst['strong_buy'] = None
        ratings_analyst['buy'] = None
        ratings_analyst['hold'] = None
        ratings_analyst['sell'] = None
        ratings_analyst['strong_sell'] = None

    ratings_analyst = json.dumps(ratings_analyst)

    values[f'ratings_analyst'] = ratings_analyst

    sql = f'''
        INSERT INTO companies_display (
            ticker
        ) SELECT
            '{ticker}'
        ON CONFLICT(ticker) DO NOTHING;

        UPDATE companies_display
        SET
            currency = {'NULL' if values['currency'] is None else f"'{values['currency']}'"},
            name = {'NULL' if values['name'] is None else f"'{values['name']}'"},
            country = {'NULL' if values['country'] is None else f"'{values['country']}'"},
            sector = {'NULL' if values['sector'] is None else f"'{values['sector']}'"},
            industry = {'NULL' if values['industry'] is None else f"'{values['industry']}'"},
            ipo = TO_TIMESTAMP({'NULL' if values['ipo'] is None else f"'{values['ipo']}'"}, 'YYYY-MM-DD'),
            description = {'NULL' if values['description'] is None else "'" + values['description'].replace("'", "''") + "'"},
            avg_rating_analyst = {'NULL' if values['avg_rating_analyst'] is None else values['avg_rating_analyst']},
            target_price_analyst = {'NULL' if values['target_price_analyst'] is None else values['target_price_analyst']},
            ratings_analyst = {'NULL' if values['ratings_analyst'] is None else f"'{values['ratings_analyst']}'"},
            short_interest_ratio = {'NULL' if values['short_interest_ratio'] is None else values['short_interest_ratio']},
            short_ratio_float = {'NULL' if values['short_ratio_float'] is None else values['short_ratio_float']},
            short_ratio_outstanding = {'NULL' if values['short_ratio_outstanding'] is None else values['short_ratio_outstanding']},
            shares_short = {'NULL' if values['shares_short'] is None else values['shares_short']},
            shares_short_prior_month = {'NULL' if values['shares_short_prior_month'] is None else values['shares_short_prior_month']},
            shares_held_by_insiders = {'NULL' if values['shares_held_by_insiders'] is None else values['shares_held_by_insiders']},
            shares_held_by_institutions = {'NULL' if values['shares_held_by_institutions'] is None else values['shares_held_by_institutions']},
            shares_float = {'NULL' if values['shares_float'] is None else values['shares_float']},
            website = {'NULL' if values['website'] is None else f"'{values['website']}'"},
            isin = {'NULL' if values['isin'] is None else f"'{values['isin']}'"},
            fiscal_year_end = {'NULL' if values['fiscal_year_end'] is None else f"'{values['fiscal_year_end']}'"},
            gic_sector = {'NULL' if values['gic_sector'] is None else f"'{values['gic_sector']}'"},
            gic_group = {'NULL' if values['gic_group'] is None else f"'{values['gic_group']}'"},
            gic_industry = {'NULL' if values['gic_industry'] is None else f"'{values['gic_industry']}'"},
            gic_sub_industry = {'NULL' if values['gic_sub_industry'] is None else f"'{values['gic_sub_industry']}'"},
            is_delisted = {'NULL' if values['is_delisted'] is None else ('true' if values['is_delisted'] else 'false')},
            full_time_employees = {'NULL' if values['full_time_employees'] is None else values['full_time_employees']},
            address = {'NULL' if values['address'] is None else f"'{values['address']}'"}
        WHERE ticker = '{ticker}'
        ;
    '''

    db.get_bind().execute(sql)


def update_annual(db, ticker, fundamentals):
    '''
    Updates companies_annual at ticker with fundamentals.
    '''

    sql = ''

    keys = fundamentals['Financials']['Balance_Sheet']['yearly'].keys()

    for key in filter(lambda x: int(x[:4]) >= datetime.now().year - 10, keys):

        values = {}

        values['ticker'] = ticker

        values['time'] = key

        try:
            values[f'currency'] = fundamentals['General']['CurrencyCode']
        except:
            values[f'currency'] = None
        try:
            values[f'sector'] = fundamentals['General']['Sector']
        except:
            values[f'sector'] = None
        try:
            values[f'industry'] = fundamentals['General']['Industry']
        except:
            values[f'industry'] = None

        balance_sheet = fundamentals['Financials']['Balance_Sheet']['yearly'].get(
            key)
        cash_flow_statement = fundamentals['Financials']['Cash_Flow']['yearly'].get(
            key)
        income_statement = fundamentals['Financials']['Income_Statement']['yearly'].get(
            key)

        # balance sheet

        values[f'total_assets'] = None if balance_sheet is None else balance_sheet.get(
            'totalAssets')
        values[f'intangible_assets'] = None if balance_sheet is None else balance_sheet.get(
            'intangibleAssets')
        values[f'earning_assets'] = None if balance_sheet is None else balance_sheet.get(
            'earningAssets')
        values[f'other_current_assets'] = None if balance_sheet is None else balance_sheet.get(
            'otherCurrentAssets')
        values[f'other_assets'] = None if balance_sheet is None else balance_sheet.get(
            'otherAssets')
        values[f'total_current_assets'] = None if balance_sheet is None else balance_sheet.get(
            'totalCurrentAssets')
        values[f'net_tangible_assets'] = None if balance_sheet is None else balance_sheet.get(
            'netTangibleAssets')
        values[f'deferred_long_term_asset_charges'] = None if balance_sheet is None else balance_sheet.get(
            'deferredLongTermAssetCharges')
        values[f'non_current_assets_other'] = None if balance_sheet is None else balance_sheet.get(
            'nonCurrrentAssetsOther')
        values[f'non_current_assets_total'] = None if balance_sheet is None else balance_sheet.get(
            'nonCurrentAssetsTotal')
        values[f'total_liabilities'] = None if balance_sheet is None else balance_sheet.get(
            'totalLiab')
        values[f'deferred_longterm_liabilities'] = None if balance_sheet is None else balance_sheet.get(
            'deferredLongTermLiab')
        values[f'other_current_liabilities'] = None if balance_sheet is None else balance_sheet.get(
            'otherCurrentLiab')
        values[f'other_liabilities'] = None if balance_sheet is None else balance_sheet.get(
            'otherLiab')
        values[f'total_current_liabilities'] = None if balance_sheet is None else balance_sheet.get(
            'totalCurrentLiabilities')
        values[f'cash_and_short_term_investments'] = None if balance_sheet is None else balance_sheet.get(
            'cashAndShortTermInvestments')
        values[f'cash'] = None if balance_sheet is None else balance_sheet.get(
            'cash')
        values[f'property_plant_and_equipment_gross'] = None if balance_sheet is None else balance_sheet.get(
            'propertyPlantAndEquipmentGross')
        values[f'property_plant_equipment'] = None if balance_sheet is None else balance_sheet.get(
            'propertyPlantEquipment')
        values[f'net_working_capital'] = None if balance_sheet is None else balance_sheet.get(
            'netWorkingCapital')
        values[f'net_invested_capital'] = None if balance_sheet is None else balance_sheet.get(
            'netInvestedCapital')
        values[f'net_debt'] = None if balance_sheet is None else balance_sheet.get(
            'netDebt')
        values[f'short_term_debt'] = None if balance_sheet is None else balance_sheet.get(
            'shortTermDebt')
        values[f'short_long_term_debt'] = None if balance_sheet is None else balance_sheet.get(
            'shortLongTermDebt')
        values[f'short_long_term_debt_total'] = None if balance_sheet is None else balance_sheet.get(
            'shortLongTermDebtTotal')
        values[f'long_term_debt'] = None if balance_sheet is None else balance_sheet.get(
            'longTermDebt')
        values[f'long_term_debt_total'] = None if balance_sheet is None else balance_sheet.get(
            'longTermDebtTotal')
        values[f'capital_lease_obligations'] = None if balance_sheet is None else balance_sheet.get(
            'capitalLeaseObligations')
        values[f'accumulated_amortization'] = None if balance_sheet is None else balance_sheet.get(
            'accumulatedAmortization')
        values[f'accumulated_depreciation'] = None if balance_sheet is None else balance_sheet.get(
            'accumulatedDepreciation')
        values[f'non_current_liabilities_other'] = None if balance_sheet is None else balance_sheet.get(
            'nonCurrentLiabilitiesOther')
        values[f'non_current_liabilities_total'] = None if balance_sheet is None else balance_sheet.get(
            'nonCurrentLiabilitiesTotal')
        values[f'total_permanent_equity'] = None if balance_sheet is None else balance_sheet.get(
            'totalPermanentEquity')
        values[f'other_stockholder_equity'] = None if balance_sheet is None else balance_sheet.get(
            'otherStockholderEquity')
        values[f'total_stockholder_equity'] = None if balance_sheet is None else balance_sheet.get(
            'totalStockholderEquity')
        values[f'common_stock_total_equity'] = None if balance_sheet is None else balance_sheet.get(
            'commonStockTotalEquity')
        values[f'preferred_stock_total_equity'] = None if balance_sheet is None else balance_sheet.get(
            'preferredStockTotalEquity')
        values[f'retained_earnings_total_equity'] = None if balance_sheet is None else balance_sheet.get(
            'retainedEarningsTotalEquity')
        values[f'liabilities_and_stockholders_equity'] = None if balance_sheet is None else balance_sheet.get(
            'liabilitiesAndStockholdersEquity')
        values[f'common_stock'] = None if balance_sheet is None else balance_sheet.get(
            'commonStock')
        values[f'retained_earnings'] = None if balance_sheet is None else balance_sheet.get(
            'retainedEarnings')
        values[f'goodwill'] = None if balance_sheet is None else balance_sheet.get(
            'goodWill')
        values[f'long_term_investments'] = None if balance_sheet is None else balance_sheet.get(
            'longTermInvestments')
        values[f'short_term_investments'] = None if balance_sheet is None else balance_sheet.get(
            'shortTermInvestments')
        values[f'net_receivables'] = None if balance_sheet is None else balance_sheet.get(
            'netReceivables')
        values[f'inventory'] = None if balance_sheet is None else balance_sheet.get(
            'inventory')
        values[f'accounts_payable'] = None if balance_sheet is None else balance_sheet.get(
            'accountsPayable')
        values[f'noncontrolling_interest_in_consolidated_entity'] = None if balance_sheet is None else balance_sheet.get(
            'noncontrollingInterestInConsolidatedEntity')
        values[f'temporary_equity_redeemable_noncontrolling_interests'] = None if balance_sheet is None else balance_sheet.get(
            'temporaryEquityRedeemableNoncontrollingInterests')
        values[f'accumulated_other_comprehensive_income'] = None if balance_sheet is None else balance_sheet.get(
            'accumulatedOtherComprehensiveIncome')
        values[f'additional_paid_in_capital'] = None if balance_sheet is None else balance_sheet.get(
            'additionalPaidInCapital')
        values[f'treasury_stock'] = None if balance_sheet is None else balance_sheet.get(
            'treasuryStock')
        values[f'negative_goodwill'] = None if balance_sheet is None else balance_sheet.get(
            'negativeGoodwill')
        values[f'warrants'] = None if balance_sheet is None else balance_sheet.get(
            'warrants')
        values[f'preferred_stock_redeemable'] = None if balance_sheet is None else balance_sheet.get(
            'preferredStockRedeemable')
        values[f'capital_surplus'] = None if balance_sheet is None else balance_sheet.get(
            'capitalSurpluse')
        values[f'common_stock_shares_outstanding'] = None if balance_sheet is None else balance_sheet.get(
            'commonStockSharesOutstanding')

        # cash flow statement

        values[f'investments'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'investments')
        values[f'change_to_liabilities'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'changeToLiabilities')
        values[f'total_cashflows_from_investing_activities'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'totalCashflowsFromInvestingActivities')
        values[f'net_borrowings'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'netBorrowings')
        values[f'total_cash_from_financing_activities'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'totalCashFromFinancingActivities')
        values[f'change_to_operating_activities'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'changeToOperatingActivities')
        values[f'change_in_cash'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'changeInCash')
        values[f'begin_period_cashflow'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'beginPeriodCashFlow')
        values[f'end_period_cashflow'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'endPeriodCashFlow')
        values[f'total_cash_from_operating_activities'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'totalCashFromOperatingActivities')
        values[f'depreciation'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'depreciation')
        values[f'other_cashflows_from_investing_activities'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'otherCashflowsFromInvestingActivities')
        values[f'dividends_paid'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'dividendsPaid')
        values[f'change_to_inventory'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'changeToInventory')
        values[f'change_to_account_receivables'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'changeToAccountReceivables')
        values[f'sale_purchase_of_stock'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'salePurchaseOfStock')
        values[f'other_cashflows_from_financing_activities'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'otherCashflowsFromFinancingActivities')
        values[f'change_to_net_income'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'changeToNetincome')
        values[f'capital_expenditures'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'capitalExpenditures')
        values[f'change_receivables'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'changeReceivables')
        values[f'cashflows_other_operating'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'cashFlowsOtherOperating')
        values[f'exchange_rate_changes'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'exchangeRateChanges')
        values[f'cash_and_cash_equivalents_changes'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'cashAndCashEquivalentsChanges')
        values[f'change_in_working_capital'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'changeInWorkingCapital')
        values[f'other_non_cash_items'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'otherNonCashItems')
        values[f'free_cashflow'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'freeCashFlow')

        # income statement

        values[f'research_development'] = None if income_statement is None else income_statement.get(
            'researchDevelopment')
        values[f'effect_of_accounting_charges'] = None if income_statement is None else income_statement.get(
            'effectOfAccountingCharges')
        values[f'income_before_tax'] = None if income_statement is None else income_statement.get(
            'incomeBeforeTax')
        values[f'minority_interest'] = None if income_statement is None else income_statement.get(
            'minorityInterest')
        values[f'net_income'] = None if income_statement is None else income_statement.get(
            'netIncome')
        values[f'selling_general_administrative'] = None if income_statement is None else income_statement.get(
            'sellingGeneralAdministrative')
        values[f'selling_and_marketing_expenses'] = None if income_statement is None else income_statement.get(
            'sellingAndMarketingExpenses')
        values[f'gross_profit'] = None if income_statement is None else income_statement.get(
            'grossProfit')
        values[f'reconciled_depreciation'] = None if income_statement is None else income_statement.get(
            'reconciledDepreciation')
        values[f'ebit'] = None if income_statement is None else income_statement.get(
            'ebit')
        values[f'ebitda'] = None if income_statement is None else income_statement.get(
            'ebitda')
        values[f'depreciation_and_amortization'] = None if income_statement is None else income_statement.get(
            'depreciationAndAmortization')
        values[f'non_operating_income_net_other'] = None if income_statement is None else income_statement.get(
            'nonOperatingIncomeNetOther')
        values[f'operating_income'] = None if income_statement is None else income_statement.get(
            'operatingIncome')
        values[f'other_operating_expenses'] = None if income_statement is None else income_statement.get(
            'otherOperatingExpenses')
        values[f'interest_expense'] = None if income_statement is None else income_statement.get(
            'interestExpense')
        values[f'tax_provision'] = None if income_statement is None else income_statement.get(
            'taxProvision')
        values[f'interest_income'] = None if income_statement is None else income_statement.get(
            'interestIncome')
        values[f'net_interest_income'] = None if income_statement is None else income_statement.get(
            'netInterestIncome')
        values[f'extraordinary_items'] = None if income_statement is None else income_statement.get(
            'extraordinaryItems')
        values[f'non_recurring'] = None if income_statement is None else income_statement.get(
            'nonRecurring')
        values[f'other_items'] = None if income_statement is None else income_statement.get(
            'otherItems')
        values[f'income_tax_expense'] = None if income_statement is None else income_statement.get(
            'incomeTaxExpense')
        values[f'total_revenue'] = None if income_statement is None else income_statement.get(
            'totalRevenue')
        values[f'total_operating_expenses'] = None if income_statement is None else income_statement.get(
            'totalOperatingExpenses')
        values[f'cost_of_revenue'] = None if income_statement is None else income_statement.get(
            'costOfRevenue')
        values[f'total_other_income_expense_net'] = None if income_statement is None else income_statement.get(
            'totalOtherIncomeExpenseNet')
        values[f'discontinued_operations'] = None if income_statement is None else income_statement.get(
            'discontinuedOperations')
        values[f'net_income_from_continuing_operations'] = None if income_statement is None else income_statement.get(
            'netIncomeFromContinuingOps')
        values[f'net_income_applicable_to_common_shares'] = None if income_statement is None else income_statement.get(
            'netIncomeApplicableToCommonShares')
        values[f'preferred_stock_and_other_adjustments'] = None if income_statement is None else income_statement.get(
            'preferredStockAndOtherAdjustments')

        # outstanding shares

        values['outstanding_shares'] = None
        if fundamentals.get('outstandingShares') is not None:
            if fundamentals['outstandingShares'].get('annual') is not None:
                for _, value in fundamentals['outstandingShares']['annual'].items():
                    if value['dateFormatted'][:4] == values['time'][:4]:
                        values['outstanding_shares'] = value['shares']
                        break

        sql += f"""
            INSERT INTO companies_annual (
                ticker,
                currency,
                time,
                industry,
                sector,
                total_assets,
                intangible_assets,
                earning_assets,
                other_current_assets,
                other_assets,
                total_current_assets,
                net_tangible_assets,
                deferred_long_term_asset_charges,
                non_current_assets_other,
                non_current_assets_total,
                total_liabilities,
                deferred_longterm_liabilities,
                other_current_liabilities,
                other_liabilities,
                total_current_liabilities,
                cash_and_short_term_investments,
                cash,
                property_plant_and_equipment_gross,
                property_plant_equipment,
                net_working_capital,
                net_invested_capital,
                net_debt,
                short_term_debt,
                short_long_term_debt,
                short_long_term_debt_total,
                long_term_debt,
                long_term_debt_total,
                capital_lease_obligations,
                accumulated_amortization,
                accumulated_depreciation,
                non_current_liabilities_other,
                non_current_liabilities_total,
                total_permanent_equity,
                other_stockholder_equity,
                total_stockholder_equity,
                common_stock_total_equity,
                preferred_stock_total_equity,
                retained_earnings_total_equity,
                liabilities_and_stockholders_equity,
                common_stock,
                retained_earnings,
                goodwill,
                long_term_investments,
                short_term_investments,
                net_receivables,
                inventory,
                accounts_payable,
                noncontrolling_interest_in_consolidated_entity,
                temporary_equity_redeemable_noncontrolling_interests,
                accumulated_other_comprehensive_income,
                additional_paid_in_capital,
                treasury_stock,
                negative_goodwill,
                warrants,
                preferred_stock_redeemable,
                capital_surplus,
                common_stock_shares_outstanding,
                investments,
                change_to_liabilities,
                total_cashflows_from_investing_activities,
                net_borrowings,
                total_cash_from_financing_activities,
                change_to_operating_activities,
                change_in_cash,
                begin_period_cashflow,
                end_period_cashflow,
                total_cash_from_operating_activities,
                depreciation,
                other_cashflows_from_investing_activities,
                dividends_paid,
                change_to_inventory,
                change_to_account_receivables,
                sale_purchase_of_stock,
                other_cashflows_from_financing_activities,
                change_to_net_income,
                capital_expenditures,
                change_receivables,
                cashflows_other_operating,
                exchange_rate_changes,
                cash_and_cash_equivalents_changes,
                change_in_working_capital,
                other_non_cash_items,
                free_cashflow,
                research_development,
                effect_of_accounting_charges,
                income_before_tax,
                minority_interest,
                net_income,
                selling_general_administrative,
                selling_and_marketing_expenses,
                gross_profit,
                reconciled_depreciation,
                ebit,
                ebitda,
                depreciation_and_amortization,
                non_operating_income_net_other,
                operating_income,
                other_operating_expenses,
                interest_expense,
                tax_provision,
                interest_income,
                net_interest_income,
                extraordinary_items,
                non_recurring,
                other_items,
                income_tax_expense,
                total_revenue,
                total_operating_expenses,
                cost_of_revenue,
                total_other_income_expense_net,
                discontinued_operations,
                net_income_from_continuing_operations,
                net_income_applicable_to_common_shares,
                preferred_stock_and_other_adjustments,
                outstanding_shares
            ) SELECT
                {'NULL' if values['ticker'] is None else f"'{values['ticker']}'"},
                {'NULL' if values['currency'] is None else f"'{values['currency']}'"},
                {'NULL' if values['time'] is None else f"'{values['time']}'"},
                {'NULL' if values['industry'] is None else f"'{values['industry']}'"},
                {'NULL' if values['sector'] is None else f"'{values['sector']}'"},
                {'NULL' if values['total_assets'] is None else values['total_assets']},
                {'NULL' if values['intangible_assets'] is None else values['intangible_assets']},
                {'NULL' if values['earning_assets'] is None else values['earning_assets']},
                {'NULL' if values['other_current_assets'] is None else values['other_current_assets']},
                {'NULL' if values['other_assets'] is None else values['other_assets']},
                {'NULL' if values['total_current_assets'] is None else values['total_current_assets']},
                {'NULL' if values['net_tangible_assets'] is None else values['net_tangible_assets']},
                {'NULL' if values['deferred_long_term_asset_charges'] is None else values['deferred_long_term_asset_charges']},
                {'NULL' if values['non_current_assets_other'] is None else values['non_current_assets_other']},
                {'NULL' if values['non_current_assets_total'] is None else values['non_current_assets_total']},
                {'NULL' if values['total_liabilities'] is None else values['total_liabilities']},
                {'NULL' if values['deferred_longterm_liabilities'] is None else values['deferred_longterm_liabilities']},
                {'NULL' if values['other_current_liabilities'] is None else values['other_current_liabilities']},
                {'NULL' if values['other_liabilities'] is None else values['other_liabilities']},
                {'NULL' if values['total_current_liabilities'] is None else values['total_current_liabilities']},
                {'NULL' if values['cash_and_short_term_investments'] is None else values['cash_and_short_term_investments']},
                {'NULL' if values['cash'] is None else values['cash']},
                {'NULL' if values['property_plant_and_equipment_gross'] is None else values['property_plant_and_equipment_gross']},
                {'NULL' if values['property_plant_equipment'] is None else values['property_plant_equipment']},
                {'NULL' if values['net_working_capital'] is None else values['net_working_capital']},
                {'NULL' if values['net_invested_capital'] is None else values['net_invested_capital']},
                {'NULL' if values['net_debt'] is None else values['net_debt']},
                {'NULL' if values['short_term_debt'] is None else values['short_term_debt']},
                {'NULL' if values['short_long_term_debt'] is None else values['short_long_term_debt']},
                {'NULL' if values['short_long_term_debt_total'] is None else values['short_long_term_debt_total']},
                {'NULL' if values['long_term_debt'] is None else values['long_term_debt']},
                {'NULL' if values['long_term_debt_total'] is None else values['long_term_debt_total']},
                {'NULL' if values['capital_lease_obligations'] is None else values['capital_lease_obligations']},
                {'NULL' if values['accumulated_amortization'] is None else values['accumulated_amortization']},
                {'NULL' if values['accumulated_depreciation'] is None else values['accumulated_depreciation']},
                {'NULL' if values['non_current_liabilities_other'] is None else values['non_current_liabilities_other']},
                {'NULL' if values['non_current_liabilities_total'] is None else values['non_current_liabilities_total']},
                {'NULL' if values['total_permanent_equity'] is None else values['total_permanent_equity']},
                {'NULL' if values['other_stockholder_equity'] is None else values['other_stockholder_equity']},
                {'NULL' if values['total_stockholder_equity'] is None else values['total_stockholder_equity']},
                {'NULL' if values['common_stock_total_equity'] is None else values['common_stock_total_equity']},
                {'NULL' if values['preferred_stock_total_equity'] is None else values['preferred_stock_total_equity']},
                {'NULL' if values['retained_earnings_total_equity'] is None else values['retained_earnings_total_equity']},
                {'NULL' if values['liabilities_and_stockholders_equity'] is None else values['liabilities_and_stockholders_equity']},
                {'NULL' if values['common_stock'] is None else values['common_stock']},
                {'NULL' if values['retained_earnings'] is None else values['retained_earnings']},
                {'NULL' if values['goodwill'] is None else values['goodwill']},
                {'NULL' if values['long_term_investments'] is None else values['long_term_investments']},
                {'NULL' if values['short_term_investments'] is None else values['short_term_investments']},
                {'NULL' if values['net_receivables'] is None else values['net_receivables']},
                {'NULL' if values['inventory'] is None else values['inventory']},
                {'NULL' if values['accounts_payable'] is None else values['accounts_payable']},
                {'NULL' if values['noncontrolling_interest_in_consolidated_entity'] is None else values['noncontrolling_interest_in_consolidated_entity']},
                {'NULL' if values['temporary_equity_redeemable_noncontrolling_interests'] is None else values['temporary_equity_redeemable_noncontrolling_interests']},
                {'NULL' if values['accumulated_other_comprehensive_income'] is None else values['accumulated_other_comprehensive_income']},
                {'NULL' if values['additional_paid_in_capital'] is None else values['additional_paid_in_capital']},
                {'NULL' if values['treasury_stock'] is None else values['treasury_stock']},
                {'NULL' if values['negative_goodwill'] is None else values['negative_goodwill']},
                {'NULL' if values['warrants'] is None else values['warrants']},
                {'NULL' if values['preferred_stock_redeemable'] is None else values['preferred_stock_redeemable']},
                {'NULL' if values['capital_surplus'] is None else values['capital_surplus']},
                {'NULL' if values['common_stock_shares_outstanding'] is None else values['common_stock_shares_outstanding']},
                {'NULL' if values['investments'] is None else values['investments']},
                {'NULL' if values['change_to_liabilities'] is None else values['change_to_liabilities']},
                {'NULL' if values['total_cashflows_from_investing_activities'] is None else values['total_cashflows_from_investing_activities']},
                {'NULL' if values['net_borrowings'] is None else values['net_borrowings']},
                {'NULL' if values['total_cash_from_financing_activities'] is None else values['total_cash_from_financing_activities']},
                {'NULL' if values['change_to_operating_activities'] is None else values['change_to_operating_activities']},
                {'NULL' if values['change_in_cash'] is None else values['change_in_cash']},
                {'NULL' if values['begin_period_cashflow'] is None else values['begin_period_cashflow']},
                {'NULL' if values['end_period_cashflow'] is None else values['end_period_cashflow']},
                {'NULL' if values['total_cash_from_operating_activities'] is None else values['total_cash_from_operating_activities']},
                {'NULL' if values['depreciation'] is None else values['depreciation']},
                {'NULL' if values['other_cashflows_from_investing_activities'] is None else values['other_cashflows_from_investing_activities']},
                {'NULL' if values['dividends_paid'] is None else values['dividends_paid']},
                {'NULL' if values['change_to_inventory'] is None else values['change_to_inventory']},
                {'NULL' if values['change_to_account_receivables'] is None else values['change_to_account_receivables']},
                {'NULL' if values['sale_purchase_of_stock'] is None else values['sale_purchase_of_stock']},
                {'NULL' if values['other_cashflows_from_financing_activities'] is None else values['other_cashflows_from_financing_activities']},
                {'NULL' if values['change_to_net_income'] is None else values['change_to_net_income']},
                {'NULL' if values['capital_expenditures'] is None else values['capital_expenditures']},
                {'NULL' if values['change_receivables'] is None else values['change_receivables']},
                {'NULL' if values['cashflows_other_operating'] is None else values['cashflows_other_operating']},
                {'NULL' if values['exchange_rate_changes'] is None else values['exchange_rate_changes']},
                {'NULL' if values['cash_and_cash_equivalents_changes'] is None else values['cash_and_cash_equivalents_changes']},
                {'NULL' if values['change_in_working_capital'] is None else values['change_in_working_capital']},
                {'NULL' if values['other_non_cash_items'] is None else values['other_non_cash_items']},
                {'NULL' if values['free_cashflow'] is None else values['free_cashflow']},
                {'NULL' if values['research_development'] is None else values['research_development']},
                {'NULL' if values['effect_of_accounting_charges'] is None else values['effect_of_accounting_charges']},
                {'NULL' if values['income_before_tax'] is None else values['income_before_tax']},
                {'NULL' if values['minority_interest'] is None else values['minority_interest']},
                {'NULL' if values['net_income'] is None else values['net_income']},
                {'NULL' if values['selling_general_administrative'] is None else values['selling_general_administrative']},
                {'NULL' if values['selling_and_marketing_expenses'] is None else values['selling_and_marketing_expenses']},
                {'NULL' if values['gross_profit'] is None else values['gross_profit']},
                {'NULL' if values['reconciled_depreciation'] is None else values['reconciled_depreciation']},
                {'NULL' if values['ebit'] is None else values['ebit']},
                {'NULL' if values['ebitda'] is None else values['ebitda']},
                {'NULL' if values['depreciation_and_amortization'] is None else values['depreciation_and_amortization']},
                {'NULL' if values['non_operating_income_net_other'] is None else values['non_operating_income_net_other']},
                {'NULL' if values['operating_income'] is None else values['operating_income']},
                {'NULL' if values['other_operating_expenses'] is None else values['other_operating_expenses']},
                {'NULL' if values['interest_expense'] is None else values['interest_expense']},
                {'NULL' if values['tax_provision'] is None else values['tax_provision']},
                {'NULL' if values['interest_income'] is None else values['interest_income']},
                {'NULL' if values['net_interest_income'] is None else values['net_interest_income']},
                {'NULL' if values['extraordinary_items'] is None else values['extraordinary_items']},
                {'NULL' if values['non_recurring'] is None else values['non_recurring']},
                {'NULL' if values['other_items'] is None else values['other_items']},
                {'NULL' if values['income_tax_expense'] is None else values['income_tax_expense']},
                {'NULL' if values['total_revenue'] is None else values['total_revenue']},
                {'NULL' if values['total_operating_expenses'] is None else values['total_operating_expenses']},
                {'NULL' if values['cost_of_revenue'] is None else values['cost_of_revenue']},
                {'NULL' if values['total_other_income_expense_net'] is None else values['total_other_income_expense_net']},
                {'NULL' if values['discontinued_operations'] is None else values['discontinued_operations']},
                {'NULL' if values['net_income_from_continuing_operations'] is None else values['net_income_from_continuing_operations']},
                {'NULL' if values['net_income_applicable_to_common_shares'] is None else values['net_income_applicable_to_common_shares']},
                {'NULL' if values['preferred_stock_and_other_adjustments'] is None else values['preferred_stock_and_other_adjustments']},
                {'NULL' if values['outstanding_shares'] is None else values['outstanding_shares']}
            ON CONFLICT (ticker, time) DO
            UPDATE
            SET
                currency = EXCLUDED.currency,
                industry = EXCLUDED.industry,
                sector = EXCLUDED.sector,
                total_assets = EXCLUDED.total_assets,
                intangible_assets = EXCLUDED.intangible_assets,
                earning_assets = EXCLUDED.earning_assets,
                other_current_assets = EXCLUDED.other_current_assets,
                other_assets = EXCLUDED.other_assets,
                total_current_assets = EXCLUDED.total_current_assets,
                net_tangible_assets = EXCLUDED.net_tangible_assets,
                deferred_long_term_asset_charges = EXCLUDED.deferred_long_term_asset_charges,
                non_current_assets_other = EXCLUDED.non_current_assets_other,
                non_current_assets_total = EXCLUDED.non_current_assets_total,
                total_liabilities = EXCLUDED.total_liabilities,
                deferred_longterm_liabilities = EXCLUDED.deferred_longterm_liabilities,
                other_current_liabilities = EXCLUDED.other_current_liabilities,
                other_liabilities = EXCLUDED.other_liabilities,
                total_current_liabilities = EXCLUDED.total_current_liabilities,
                cash_and_short_term_investments = EXCLUDED.cash_and_short_term_investments,
                cash = EXCLUDED.cash,
                property_plant_and_equipment_gross = EXCLUDED.property_plant_and_equipment_gross,
                property_plant_equipment = EXCLUDED.property_plant_equipment,
                net_working_capital = EXCLUDED.net_working_capital,
                net_invested_capital = EXCLUDED.net_invested_capital,
                net_debt = EXCLUDED.net_debt,
                short_term_debt = EXCLUDED.short_term_debt,
                short_long_term_debt = EXCLUDED.short_long_term_debt,
                short_long_term_debt_total = EXCLUDED.short_long_term_debt_total,
                long_term_debt = EXCLUDED.long_term_debt,
                long_term_debt_total = EXCLUDED.long_term_debt_total,
                capital_lease_obligations = EXCLUDED.capital_lease_obligations,
                accumulated_amortization = EXCLUDED.accumulated_amortization,
                accumulated_depreciation = EXCLUDED.accumulated_depreciation,
                non_current_liabilities_other = EXCLUDED.non_current_liabilities_other,
                non_current_liabilities_total = EXCLUDED.non_current_liabilities_total,
                total_permanent_equity = EXCLUDED.total_permanent_equity,
                other_stockholder_equity = EXCLUDED.other_stockholder_equity,
                total_stockholder_equity = EXCLUDED.total_stockholder_equity,
                common_stock_total_equity = EXCLUDED.common_stock_total_equity,
                preferred_stock_total_equity = EXCLUDED.preferred_stock_total_equity,
                retained_earnings_total_equity = EXCLUDED.retained_earnings_total_equity,
                liabilities_and_stockholders_equity = EXCLUDED.liabilities_and_stockholders_equity,
                common_stock = EXCLUDED.common_stock,
                retained_earnings = EXCLUDED.retained_earnings,
                goodwill = EXCLUDED.goodwill,
                long_term_investments = EXCLUDED.long_term_investments,
                short_term_investments = EXCLUDED.short_term_investments,
                net_receivables = EXCLUDED.net_receivables,
                inventory = EXCLUDED.inventory,
                accounts_payable = EXCLUDED.accounts_payable,
                noncontrolling_interest_in_consolidated_entity = EXCLUDED.noncontrolling_interest_in_consolidated_entity,
                temporary_equity_redeemable_noncontrolling_interests = EXCLUDED.temporary_equity_redeemable_noncontrolling_interests,
                accumulated_other_comprehensive_income = EXCLUDED.accumulated_other_comprehensive_income,
                additional_paid_in_capital = EXCLUDED.additional_paid_in_capital,
                treasury_stock = EXCLUDED.treasury_stock,
                negative_goodwill = EXCLUDED.negative_goodwill,
                warrants = EXCLUDED.warrants,
                preferred_stock_redeemable = EXCLUDED.preferred_stock_redeemable,
                capital_surplus = EXCLUDED.capital_surplus,
                common_stock_shares_outstanding = EXCLUDED.common_stock_shares_outstanding,
                investments = EXCLUDED.investments,
                change_to_liabilities = EXCLUDED.change_to_liabilities,
                total_cashflows_from_investing_activities = EXCLUDED.total_cashflows_from_investing_activities,
                net_borrowings = EXCLUDED.net_borrowings,
                total_cash_from_financing_activities = EXCLUDED.total_cash_from_financing_activities,
                change_to_operating_activities = EXCLUDED.change_to_operating_activities,
                change_in_cash = EXCLUDED.change_in_cash,
                begin_period_cashflow = EXCLUDED.begin_period_cashflow,
                end_period_cashflow = EXCLUDED.end_period_cashflow,
                total_cash_from_operating_activities = EXCLUDED.total_cash_from_operating_activities,
                depreciation = EXCLUDED.depreciation,
                other_cashflows_from_investing_activities = EXCLUDED.other_cashflows_from_investing_activities,
                dividends_paid = EXCLUDED.dividends_paid,
                change_to_inventory = EXCLUDED.change_to_inventory,
                change_to_account_receivables = EXCLUDED.change_to_account_receivables,
                sale_purchase_of_stock = EXCLUDED.sale_purchase_of_stock,
                other_cashflows_from_financing_activities = EXCLUDED.other_cashflows_from_financing_activities,
                change_to_net_income = EXCLUDED.change_to_net_income,
                capital_expenditures = EXCLUDED.capital_expenditures,
                change_receivables = EXCLUDED.change_receivables,
                cashflows_other_operating = EXCLUDED.cashflows_other_operating,
                exchange_rate_changes = EXCLUDED.exchange_rate_changes,
                cash_and_cash_equivalents_changes = EXCLUDED.cash_and_cash_equivalents_changes,
                change_in_working_capital = EXCLUDED.change_in_working_capital,
                other_non_cash_items = EXCLUDED.other_non_cash_items,
                free_cashflow = EXCLUDED.free_cashflow,
                research_development = EXCLUDED.research_development,
                effect_of_accounting_charges = EXCLUDED.effect_of_accounting_charges,
                income_before_tax = EXCLUDED.income_before_tax,
                minority_interest = EXCLUDED.minority_interest,
                net_income = EXCLUDED.net_income,
                selling_general_administrative = EXCLUDED.selling_general_administrative,
                selling_and_marketing_expenses = EXCLUDED.selling_and_marketing_expenses,
                gross_profit = EXCLUDED.gross_profit,
                reconciled_depreciation = EXCLUDED.reconciled_depreciation,
                ebit = EXCLUDED.ebit,
                ebitda = EXCLUDED.ebitda,
                depreciation_and_amortization = EXCLUDED.depreciation_and_amortization,
                non_operating_income_net_other = EXCLUDED.non_operating_income_net_other,
                operating_income = EXCLUDED.operating_income,
                other_operating_expenses = EXCLUDED.other_operating_expenses,
                interest_expense = EXCLUDED.interest_expense,
                tax_provision = EXCLUDED.tax_provision,
                interest_income = EXCLUDED.interest_income,
                net_interest_income = EXCLUDED.net_interest_income,
                extraordinary_items = EXCLUDED.extraordinary_items,
                non_recurring = EXCLUDED.non_recurring,
                other_items = EXCLUDED.other_items,
                income_tax_expense = EXCLUDED.income_tax_expense,
                total_revenue = EXCLUDED.total_revenue,
                total_operating_expenses = EXCLUDED.total_operating_expenses,
                cost_of_revenue = EXCLUDED.cost_of_revenue,
                total_other_income_expense_net = EXCLUDED.total_other_income_expense_net,
                discontinued_operations = EXCLUDED.discontinued_operations,
                net_income_from_continuing_operations = EXCLUDED.net_income_from_continuing_operations,
                net_income_applicable_to_common_shares = EXCLUDED.net_income_applicable_to_common_shares,
                preferred_stock_and_other_adjustments = EXCLUDED.preferred_stock_and_other_adjustments,
                outstanding_shares = EXCLUDED.outstanding_shares
            ;
        """

    if sql != '':
        db.get_bind().execute(sql)


def update_quarterly(db, ticker, fundamentals):
    '''
    Updates companies_quarterly at ticker with fundamentals.
    '''

    sql = ''

    keys = fundamentals['Financials']['Balance_Sheet']['quarterly'].keys()

    for key in filter(lambda x: int(x[:4]) >= datetime.now().year - 10, keys):

        values = {}

        values['ticker'] = ticker

        values['time'] = key

        try:
            values[f'currency'] = fundamentals['General']['CurrencyCode']
        except:
            values[f'currency'] = None
        try:
            values[f'sector'] = fundamentals['General']['Sector']
        except:
            values[f'sector'] = None
        try:
            values[f'industry'] = fundamentals['General']['Industry']
        except:
            values[f'industry'] = None

        balance_sheet = fundamentals['Financials']['Balance_Sheet']['quarterly'].get(
            key)
        cash_flow_statement = fundamentals['Financials']['Cash_Flow']['quarterly'].get(
            key)
        income_statement = fundamentals['Financials']['Income_Statement']['quarterly'].get(
            key)

        # balance sheet

        values[f'total_assets'] = None if balance_sheet is None else balance_sheet.get(
            'totalAssets')
        values[f'intangible_assets'] = None if balance_sheet is None else balance_sheet.get(
            'intangibleAssets')
        values[f'earning_assets'] = None if balance_sheet is None else balance_sheet.get(
            'earningAssets')
        values[f'other_current_assets'] = None if balance_sheet is None else balance_sheet.get(
            'otherCurrentAssets')
        values[f'other_assets'] = None if balance_sheet is None else balance_sheet.get(
            'otherAssets')
        values[f'total_current_assets'] = None if balance_sheet is None else balance_sheet.get(
            'totalCurrentAssets')
        values[f'net_tangible_assets'] = None if balance_sheet is None else balance_sheet.get(
            'netTangibleAssets')
        values[f'deferred_long_term_asset_charges'] = None if balance_sheet is None else balance_sheet.get(
            'deferredLongTermAssetCharges')
        values[f'non_current_assets_other'] = None if balance_sheet is None else balance_sheet.get(
            'nonCurrrentAssetsOther')
        values[f'non_current_assets_total'] = None if balance_sheet is None else balance_sheet.get(
            'nonCurrentAssetsTotal')
        values[f'total_liabilities'] = None if balance_sheet is None else balance_sheet.get(
            'totalLiab')
        values[f'deferred_longterm_liabilities'] = None if balance_sheet is None else balance_sheet.get(
            'deferredLongTermLiab')
        values[f'other_current_liabilities'] = None if balance_sheet is None else balance_sheet.get(
            'otherCurrentLiab')
        values[f'other_liabilities'] = None if balance_sheet is None else balance_sheet.get(
            'otherLiab')
        values[f'total_current_liabilities'] = None if balance_sheet is None else balance_sheet.get(
            'totalCurrentLiabilities')
        values[f'cash_and_short_term_investments'] = None if balance_sheet is None else balance_sheet.get(
            'cashAndShortTermInvestments')
        values[f'cash'] = None if balance_sheet is None else balance_sheet.get(
            'cash')
        values[f'property_plant_and_equipment_gross'] = None if balance_sheet is None else balance_sheet.get(
            'propertyPlantAndEquipmentGross')
        values[f'property_plant_equipment'] = None if balance_sheet is None else balance_sheet.get(
            'propertyPlantEquipment')
        values[f'net_working_capital'] = None if balance_sheet is None else balance_sheet.get(
            'netWorkingCapital')
        values[f'net_invested_capital'] = None if balance_sheet is None else balance_sheet.get(
            'netInvestedCapital')
        values[f'net_debt'] = None if balance_sheet is None else balance_sheet.get(
            'netDebt')
        values[f'short_term_debt'] = None if balance_sheet is None else balance_sheet.get(
            'shortTermDebt')
        values[f'short_long_term_debt'] = None if balance_sheet is None else balance_sheet.get(
            'shortLongTermDebt')
        values[f'short_long_term_debt_total'] = None if balance_sheet is None else balance_sheet.get(
            'shortLongTermDebtTotal')
        values[f'long_term_debt'] = None if balance_sheet is None else balance_sheet.get(
            'longTermDebt')
        values[f'long_term_debt_total'] = None if balance_sheet is None else balance_sheet.get(
            'longTermDebtTotal')
        values[f'capital_lease_obligations'] = None if balance_sheet is None else balance_sheet.get(
            'capitalLeaseObligations')
        values[f'accumulated_amortization'] = None if balance_sheet is None else balance_sheet.get(
            'accumulatedAmortization')
        values[f'accumulated_depreciation'] = None if balance_sheet is None else balance_sheet.get(
            'accumulatedDepreciation')
        values[f'non_current_liabilities_other'] = None if balance_sheet is None else balance_sheet.get(
            'nonCurrentLiabilitiesOther')
        values[f'non_current_liabilities_total'] = None if balance_sheet is None else balance_sheet.get(
            'nonCurrentLiabilitiesTotal')
        values[f'total_permanent_equity'] = None if balance_sheet is None else balance_sheet.get(
            'totalPermanentEquity')
        values[f'other_stockholder_equity'] = None if balance_sheet is None else balance_sheet.get(
            'otherStockholderEquity')
        values[f'total_stockholder_equity'] = None if balance_sheet is None else balance_sheet.get(
            'totalStockholderEquity')
        values[f'common_stock_total_equity'] = None if balance_sheet is None else balance_sheet.get(
            'commonStockTotalEquity')
        values[f'preferred_stock_total_equity'] = None if balance_sheet is None else balance_sheet.get(
            'preferredStockTotalEquity')
        values[f'retained_earnings_total_equity'] = None if balance_sheet is None else balance_sheet.get(
            'retainedEarningsTotalEquity')
        values[f'liabilities_and_stockholders_equity'] = None if balance_sheet is None else balance_sheet.get(
            'liabilitiesAndStockholdersEquity')
        values[f'common_stock'] = None if balance_sheet is None else balance_sheet.get(
            'commonStock')
        values[f'retained_earnings'] = None if balance_sheet is None else balance_sheet.get(
            'retainedEarnings')
        values[f'goodwill'] = None if balance_sheet is None else balance_sheet.get(
            'goodWill')
        values[f'long_term_investments'] = None if balance_sheet is None else balance_sheet.get(
            'longTermInvestments')
        values[f'short_term_investments'] = None if balance_sheet is None else balance_sheet.get(
            'shortTermInvestments')
        values[f'net_receivables'] = None if balance_sheet is None else balance_sheet.get(
            'netReceivables')
        values[f'inventory'] = None if balance_sheet is None else balance_sheet.get(
            'inventory')
        values[f'accounts_payable'] = None if balance_sheet is None else balance_sheet.get(
            'accountsPayable')
        values[f'noncontrolling_interest_in_consolidated_entity'] = None if balance_sheet is None else balance_sheet.get(
            'noncontrollingInterestInConsolidatedEntity')
        values[f'temporary_equity_redeemable_noncontrolling_interests'] = None if balance_sheet is None else balance_sheet.get(
            'temporaryEquityRedeemableNoncontrollingInterests')
        values[f'accumulated_other_comprehensive_income'] = None if balance_sheet is None else balance_sheet.get(
            'accumulatedOtherComprehensiveIncome')
        values[f'additional_paid_in_capital'] = None if balance_sheet is None else balance_sheet.get(
            'additionalPaidInCapital')
        values[f'treasury_stock'] = None if balance_sheet is None else balance_sheet.get(
            'treasuryStock')
        values[f'negative_goodwill'] = None if balance_sheet is None else balance_sheet.get(
            'negativeGoodwill')
        values[f'warrants'] = None if balance_sheet is None else balance_sheet.get(
            'warrants')
        values[f'preferred_stock_redeemable'] = None if balance_sheet is None else balance_sheet.get(
            'preferredStockRedeemable')
        values[f'capital_surplus'] = None if balance_sheet is None else balance_sheet.get(
            'capitalSurpluse')
        values[f'common_stock_shares_outstanding'] = None if balance_sheet is None else balance_sheet.get(
            'commonStockSharesOutstanding')

        # cash flow statement

        values[f'investments'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'investments')
        values[f'change_to_liabilities'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'changeToLiabilities')
        values[f'total_cashflows_from_investing_activities'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'totalCashflowsFromInvestingActivities')
        values[f'net_borrowings'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'netBorrowings')
        values[f'total_cash_from_financing_activities'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'totalCashFromFinancingActivities')
        values[f'change_to_operating_activities'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'changeToOperatingActivities')
        values[f'change_in_cash'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'changeInCash')
        values[f'begin_period_cashflow'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'beginPeriodCashFlow')
        values[f'end_period_cashflow'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'endPeriodCashFlow')
        values[f'total_cash_from_operating_activities'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'totalCashFromOperatingActivities')
        values[f'depreciation'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'depreciation')
        values[f'other_cashflows_from_investing_activities'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'otherCashflowsFromInvestingActivities')
        values[f'dividends_paid'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'dividendsPaid')
        values[f'change_to_inventory'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'changeToInventory')
        values[f'change_to_account_receivables'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'changeToAccountReceivables')
        values[f'sale_purchase_of_stock'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'salePurchaseOfStock')
        values[f'other_cashflows_from_financing_activities'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'otherCashflowsFromFinancingActivities')
        values[f'change_to_net_income'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'changeToNetincome')
        values[f'capital_expenditures'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'capitalExpenditures')
        values[f'change_receivables'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'changeReceivables')
        values[f'cashflows_other_operating'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'cashFlowsOtherOperating')
        values[f'exchange_rate_changes'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'exchangeRateChanges')
        values[f'cash_and_cash_equivalents_changes'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'cashAndCashEquivalentsChanges')
        values[f'change_in_working_capital'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'changeInWorkingCapital')
        values[f'other_non_cash_items'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'otherNonCashItems')
        values[f'free_cashflow'] = None if cash_flow_statement is None else cash_flow_statement.get(
            'freeCashFlow')

        # income statement

        values[f'research_development'] = None if income_statement is None else income_statement.get(
            'researchDevelopment')
        values[f'effect_of_accounting_charges'] = None if income_statement is None else income_statement.get(
            'effectOfAccountingCharges')
        values[f'income_before_tax'] = None if income_statement is None else income_statement.get(
            'incomeBeforeTax')
        values[f'minority_interest'] = None if income_statement is None else income_statement.get(
            'minorityInterest')
        values[f'net_income'] = None if income_statement is None else income_statement.get(
            'netIncome')
        values[f'selling_general_administrative'] = None if income_statement is None else income_statement.get(
            'sellingGeneralAdministrative')
        values[f'selling_and_marketing_expenses'] = None if income_statement is None else income_statement.get(
            'sellingAndMarketingExpenses')
        values[f'gross_profit'] = None if income_statement is None else income_statement.get(
            'grossProfit')
        values[f'reconciled_depreciation'] = None if income_statement is None else income_statement.get(
            'reconciledDepreciation')
        values[f'ebit'] = None if income_statement is None else income_statement.get(
            'ebit')
        values[f'ebitda'] = None if income_statement is None else income_statement.get(
            'ebitda')
        values[f'depreciation_and_amortization'] = None if income_statement is None else income_statement.get(
            'depreciationAndAmortization')
        values[f'non_operating_income_net_other'] = None if income_statement is None else income_statement.get(
            'nonOperatingIncomeNetOther')
        values[f'operating_income'] = None if income_statement is None else income_statement.get(
            'operatingIncome')
        values[f'other_operating_expenses'] = None if income_statement is None else income_statement.get(
            'otherOperatingExpenses')
        values[f'interest_expense'] = None if income_statement is None else income_statement.get(
            'interestExpense')
        values[f'tax_provision'] = None if income_statement is None else income_statement.get(
            'taxProvision')
        values[f'interest_income'] = None if income_statement is None else income_statement.get(
            'interestIncome')
        values[f'net_interest_income'] = None if income_statement is None else income_statement.get(
            'netInterestIncome')
        values[f'extraordinary_items'] = None if income_statement is None else income_statement.get(
            'extraordinaryItems')
        values[f'non_recurring'] = None if income_statement is None else income_statement.get(
            'nonRecurring')
        values[f'other_items'] = None if income_statement is None else income_statement.get(
            'otherItems')
        values[f'income_tax_expense'] = None if income_statement is None else income_statement.get(
            'incomeTaxExpense')
        values[f'total_revenue'] = None if income_statement is None else income_statement.get(
            'totalRevenue')
        values[f'total_operating_expenses'] = None if income_statement is None else income_statement.get(
            'totalOperatingExpenses')
        values[f'cost_of_revenue'] = None if income_statement is None else income_statement.get(
            'costOfRevenue')
        values[f'total_other_income_expense_net'] = None if income_statement is None else income_statement.get(
            'totalOtherIncomeExpenseNet')
        values[f'discontinued_operations'] = None if income_statement is None else income_statement.get(
            'discontinuedOperations')
        values[f'net_income_from_continuing_operations'] = None if income_statement is None else income_statement.get(
            'netIncomeFromContinuingOps')
        values[f'net_income_applicable_to_common_shares'] = None if income_statement is None else income_statement.get(
            'netIncomeApplicableToCommonShares')
        values[f'preferred_stock_and_other_adjustments'] = None if income_statement is None else income_statement.get(
            'preferredStockAndOtherAdjustments')

        # outstanding shares

        values['outstanding_shares'] = None
        if fundamentals.get('outstandingShares') is not None:
            if fundamentals['outstandingShares'].get('quarterly') is not None:
                for _, value in fundamentals['outstandingShares']['quarterly'].items():
                    if value['dateFormatted'][:7] == values['time'][:7]:
                        values['outstanding_shares'] = value['shares']
                        break

        sql += f"""
            INSERT INTO companies_quarterly (
                ticker,
                currency,
                time,
                industry,
                sector,
                total_assets,
                intangible_assets,
                earning_assets,
                other_current_assets,
                other_assets,
                total_current_assets,
                net_tangible_assets,
                deferred_long_term_asset_charges,
                non_current_assets_other,
                non_current_assets_total,
                total_liabilities,
                deferred_longterm_liabilities,
                other_current_liabilities,
                other_liabilities,
                total_current_liabilities,
                cash_and_short_term_investments,
                cash,
                property_plant_and_equipment_gross,
                property_plant_equipment,
                net_working_capital,
                net_invested_capital,
                net_debt,
                short_term_debt,
                short_long_term_debt,
                short_long_term_debt_total,
                long_term_debt,
                long_term_debt_total,
                capital_lease_obligations,
                accumulated_amortization,
                accumulated_depreciation,
                non_current_liabilities_other,
                non_current_liabilities_total,
                total_permanent_equity,
                other_stockholder_equity,
                total_stockholder_equity,
                common_stock_total_equity,
                preferred_stock_total_equity,
                retained_earnings_total_equity,
                liabilities_and_stockholders_equity,
                common_stock,
                retained_earnings,
                goodwill,
                long_term_investments,
                short_term_investments,
                net_receivables,
                inventory,
                accounts_payable,
                noncontrolling_interest_in_consolidated_entity,
                temporary_equity_redeemable_noncontrolling_interests,
                accumulated_other_comprehensive_income,
                additional_paid_in_capital,
                treasury_stock,
                negative_goodwill,
                warrants,
                preferred_stock_redeemable,
                capital_surplus,
                common_stock_shares_outstanding,
                investments,
                change_to_liabilities,
                total_cashflows_from_investing_activities,
                net_borrowings,
                total_cash_from_financing_activities,
                change_to_operating_activities,
                change_in_cash,
                begin_period_cashflow,
                end_period_cashflow,
                total_cash_from_operating_activities,
                depreciation,
                other_cashflows_from_investing_activities,
                dividends_paid,
                change_to_inventory,
                change_to_account_receivables,
                sale_purchase_of_stock,
                other_cashflows_from_financing_activities,
                change_to_net_income,
                capital_expenditures,
                change_receivables,
                cashflows_other_operating,
                exchange_rate_changes,
                cash_and_cash_equivalents_changes,
                change_in_working_capital,
                other_non_cash_items,
                free_cashflow,
                research_development,
                effect_of_accounting_charges,
                income_before_tax,
                minority_interest,
                net_income,
                selling_general_administrative,
                selling_and_marketing_expenses,
                gross_profit,
                reconciled_depreciation,
                ebit,
                ebitda,
                depreciation_and_amortization,
                non_operating_income_net_other,
                operating_income,
                other_operating_expenses,
                interest_expense,
                tax_provision,
                interest_income,
                net_interest_income,
                extraordinary_items,
                non_recurring,
                other_items,
                income_tax_expense,
                total_revenue,
                total_operating_expenses,
                cost_of_revenue,
                total_other_income_expense_net,
                discontinued_operations,
                net_income_from_continuing_operations,
                net_income_applicable_to_common_shares,
                preferred_stock_and_other_adjustments,
                outstanding_shares
            ) SELECT
                {'NULL' if values['ticker'] is None else f"'{values['ticker']}'"},
                {'NULL' if values['currency'] is None else f"'{values['currency']}'"},
                {'NULL' if values['time'] is None else f"'{values['time']}'"},
                {'NULL' if values['industry'] is None else f"'{values['industry']}'"},
                {'NULL' if values['sector'] is None else f"'{values['sector']}'"},
                {'NULL' if values['total_assets'] is None else values['total_assets']},
                {'NULL' if values['intangible_assets'] is None else values['intangible_assets']},
                {'NULL' if values['earning_assets'] is None else values['earning_assets']},
                {'NULL' if values['other_current_assets'] is None else values['other_current_assets']},
                {'NULL' if values['other_assets'] is None else values['other_assets']},
                {'NULL' if values['total_current_assets'] is None else values['total_current_assets']},
                {'NULL' if values['net_tangible_assets'] is None else values['net_tangible_assets']},
                {'NULL' if values['deferred_long_term_asset_charges'] is None else values['deferred_long_term_asset_charges']},
                {'NULL' if values['non_current_assets_other'] is None else values['non_current_assets_other']},
                {'NULL' if values['non_current_assets_total'] is None else values['non_current_assets_total']},
                {'NULL' if values['total_liabilities'] is None else values['total_liabilities']},
                {'NULL' if values['deferred_longterm_liabilities'] is None else values['deferred_longterm_liabilities']},
                {'NULL' if values['other_current_liabilities'] is None else values['other_current_liabilities']},
                {'NULL' if values['other_liabilities'] is None else values['other_liabilities']},
                {'NULL' if values['total_current_liabilities'] is None else values['total_current_liabilities']},
                {'NULL' if values['cash_and_short_term_investments'] is None else values['cash_and_short_term_investments']},
                {'NULL' if values['cash'] is None else values['cash']},
                {'NULL' if values['property_plant_and_equipment_gross'] is None else values['property_plant_and_equipment_gross']},
                {'NULL' if values['property_plant_equipment'] is None else values['property_plant_equipment']},
                {'NULL' if values['net_working_capital'] is None else values['net_working_capital']},
                {'NULL' if values['net_invested_capital'] is None else values['net_invested_capital']},
                {'NULL' if values['net_debt'] is None else values['net_debt']},
                {'NULL' if values['short_term_debt'] is None else values['short_term_debt']},
                {'NULL' if values['short_long_term_debt'] is None else values['short_long_term_debt']},
                {'NULL' if values['short_long_term_debt_total'] is None else values['short_long_term_debt_total']},
                {'NULL' if values['long_term_debt'] is None else values['long_term_debt']},
                {'NULL' if values['long_term_debt_total'] is None else values['long_term_debt_total']},
                {'NULL' if values['capital_lease_obligations'] is None else values['capital_lease_obligations']},
                {'NULL' if values['accumulated_amortization'] is None else values['accumulated_amortization']},
                {'NULL' if values['accumulated_depreciation'] is None else values['accumulated_depreciation']},
                {'NULL' if values['non_current_liabilities_other'] is None else values['non_current_liabilities_other']},
                {'NULL' if values['non_current_liabilities_total'] is None else values['non_current_liabilities_total']},
                {'NULL' if values['total_permanent_equity'] is None else values['total_permanent_equity']},
                {'NULL' if values['other_stockholder_equity'] is None else values['other_stockholder_equity']},
                {'NULL' if values['total_stockholder_equity'] is None else values['total_stockholder_equity']},
                {'NULL' if values['common_stock_total_equity'] is None else values['common_stock_total_equity']},
                {'NULL' if values['preferred_stock_total_equity'] is None else values['preferred_stock_total_equity']},
                {'NULL' if values['retained_earnings_total_equity'] is None else values['retained_earnings_total_equity']},
                {'NULL' if values['liabilities_and_stockholders_equity'] is None else values['liabilities_and_stockholders_equity']},
                {'NULL' if values['common_stock'] is None else values['common_stock']},
                {'NULL' if values['retained_earnings'] is None else values['retained_earnings']},
                {'NULL' if values['goodwill'] is None else values['goodwill']},
                {'NULL' if values['long_term_investments'] is None else values['long_term_investments']},
                {'NULL' if values['short_term_investments'] is None else values['short_term_investments']},
                {'NULL' if values['net_receivables'] is None else values['net_receivables']},
                {'NULL' if values['inventory'] is None else values['inventory']},
                {'NULL' if values['accounts_payable'] is None else values['accounts_payable']},
                {'NULL' if values['noncontrolling_interest_in_consolidated_entity'] is None else values['noncontrolling_interest_in_consolidated_entity']},
                {'NULL' if values['temporary_equity_redeemable_noncontrolling_interests'] is None else values['temporary_equity_redeemable_noncontrolling_interests']},
                {'NULL' if values['accumulated_other_comprehensive_income'] is None else values['accumulated_other_comprehensive_income']},
                {'NULL' if values['additional_paid_in_capital'] is None else values['additional_paid_in_capital']},
                {'NULL' if values['treasury_stock'] is None else values['treasury_stock']},
                {'NULL' if values['negative_goodwill'] is None else values['negative_goodwill']},
                {'NULL' if values['warrants'] is None else values['warrants']},
                {'NULL' if values['preferred_stock_redeemable'] is None else values['preferred_stock_redeemable']},
                {'NULL' if values['capital_surplus'] is None else values['capital_surplus']},
                {'NULL' if values['common_stock_shares_outstanding'] is None else values['common_stock_shares_outstanding']},
                {'NULL' if values['investments'] is None else values['investments']},
                {'NULL' if values['change_to_liabilities'] is None else values['change_to_liabilities']},
                {'NULL' if values['total_cashflows_from_investing_activities'] is None else values['total_cashflows_from_investing_activities']},
                {'NULL' if values['net_borrowings'] is None else values['net_borrowings']},
                {'NULL' if values['total_cash_from_financing_activities'] is None else values['total_cash_from_financing_activities']},
                {'NULL' if values['change_to_operating_activities'] is None else values['change_to_operating_activities']},
                {'NULL' if values['change_in_cash'] is None else values['change_in_cash']},
                {'NULL' if values['begin_period_cashflow'] is None else values['begin_period_cashflow']},
                {'NULL' if values['end_period_cashflow'] is None else values['end_period_cashflow']},
                {'NULL' if values['total_cash_from_operating_activities'] is None else values['total_cash_from_operating_activities']},
                {'NULL' if values['depreciation'] is None else values['depreciation']},
                {'NULL' if values['other_cashflows_from_investing_activities'] is None else values['other_cashflows_from_investing_activities']},
                {'NULL' if values['dividends_paid'] is None else values['dividends_paid']},
                {'NULL' if values['change_to_inventory'] is None else values['change_to_inventory']},
                {'NULL' if values['change_to_account_receivables'] is None else values['change_to_account_receivables']},
                {'NULL' if values['sale_purchase_of_stock'] is None else values['sale_purchase_of_stock']},
                {'NULL' if values['other_cashflows_from_financing_activities'] is None else values['other_cashflows_from_financing_activities']},
                {'NULL' if values['change_to_net_income'] is None else values['change_to_net_income']},
                {'NULL' if values['capital_expenditures'] is None else values['capital_expenditures']},
                {'NULL' if values['change_receivables'] is None else values['change_receivables']},
                {'NULL' if values['cashflows_other_operating'] is None else values['cashflows_other_operating']},
                {'NULL' if values['exchange_rate_changes'] is None else values['exchange_rate_changes']},
                {'NULL' if values['cash_and_cash_equivalents_changes'] is None else values['cash_and_cash_equivalents_changes']},
                {'NULL' if values['change_in_working_capital'] is None else values['change_in_working_capital']},
                {'NULL' if values['other_non_cash_items'] is None else values['other_non_cash_items']},
                {'NULL' if values['free_cashflow'] is None else values['free_cashflow']},
                {'NULL' if values['research_development'] is None else values['research_development']},
                {'NULL' if values['effect_of_accounting_charges'] is None else values['effect_of_accounting_charges']},
                {'NULL' if values['income_before_tax'] is None else values['income_before_tax']},
                {'NULL' if values['minority_interest'] is None else values['minority_interest']},
                {'NULL' if values['net_income'] is None else values['net_income']},
                {'NULL' if values['selling_general_administrative'] is None else values['selling_general_administrative']},
                {'NULL' if values['selling_and_marketing_expenses'] is None else values['selling_and_marketing_expenses']},
                {'NULL' if values['gross_profit'] is None else values['gross_profit']},
                {'NULL' if values['reconciled_depreciation'] is None else values['reconciled_depreciation']},
                {'NULL' if values['ebit'] is None else values['ebit']},
                {'NULL' if values['ebitda'] is None else values['ebitda']},
                {'NULL' if values['depreciation_and_amortization'] is None else values['depreciation_and_amortization']},
                {'NULL' if values['non_operating_income_net_other'] is None else values['non_operating_income_net_other']},
                {'NULL' if values['operating_income'] is None else values['operating_income']},
                {'NULL' if values['other_operating_expenses'] is None else values['other_operating_expenses']},
                {'NULL' if values['interest_expense'] is None else values['interest_expense']},
                {'NULL' if values['tax_provision'] is None else values['tax_provision']},
                {'NULL' if values['interest_income'] is None else values['interest_income']},
                {'NULL' if values['net_interest_income'] is None else values['net_interest_income']},
                {'NULL' if values['extraordinary_items'] is None else values['extraordinary_items']},
                {'NULL' if values['non_recurring'] is None else values['non_recurring']},
                {'NULL' if values['other_items'] is None else values['other_items']},
                {'NULL' if values['income_tax_expense'] is None else values['income_tax_expense']},
                {'NULL' if values['total_revenue'] is None else values['total_revenue']},
                {'NULL' if values['total_operating_expenses'] is None else values['total_operating_expenses']},
                {'NULL' if values['cost_of_revenue'] is None else values['cost_of_revenue']},
                {'NULL' if values['total_other_income_expense_net'] is None else values['total_other_income_expense_net']},
                {'NULL' if values['discontinued_operations'] is None else values['discontinued_operations']},
                {'NULL' if values['net_income_from_continuing_operations'] is None else values['net_income_from_continuing_operations']},
                {'NULL' if values['net_income_applicable_to_common_shares'] is None else values['net_income_applicable_to_common_shares']},
                {'NULL' if values['preferred_stock_and_other_adjustments'] is None else values['preferred_stock_and_other_adjustments']},
                {'NULL' if values['outstanding_shares'] is None else values['outstanding_shares']}
            ON CONFLICT (ticker, time) DO
            UPDATE
            SET
                currency = EXCLUDED.currency,
                industry = EXCLUDED.industry,
                sector = EXCLUDED.sector,
                total_assets = EXCLUDED.total_assets,
                intangible_assets = EXCLUDED.intangible_assets,
                earning_assets = EXCLUDED.earning_assets,
                other_current_assets = EXCLUDED.other_current_assets,
                other_assets = EXCLUDED.other_assets,
                total_current_assets = EXCLUDED.total_current_assets,
                net_tangible_assets = EXCLUDED.net_tangible_assets,
                deferred_long_term_asset_charges = EXCLUDED.deferred_long_term_asset_charges,
                non_current_assets_other = EXCLUDED.non_current_assets_other,
                non_current_assets_total = EXCLUDED.non_current_assets_total,
                total_liabilities = EXCLUDED.total_liabilities,
                deferred_longterm_liabilities = EXCLUDED.deferred_longterm_liabilities,
                other_current_liabilities = EXCLUDED.other_current_liabilities,
                other_liabilities = EXCLUDED.other_liabilities,
                total_current_liabilities = EXCLUDED.total_current_liabilities,
                cash_and_short_term_investments = EXCLUDED.cash_and_short_term_investments,
                cash = EXCLUDED.cash,
                property_plant_and_equipment_gross = EXCLUDED.property_plant_and_equipment_gross,
                property_plant_equipment = EXCLUDED.property_plant_equipment,
                net_working_capital = EXCLUDED.net_working_capital,
                net_invested_capital = EXCLUDED.net_invested_capital,
                net_debt = EXCLUDED.net_debt,
                short_term_debt = EXCLUDED.short_term_debt,
                short_long_term_debt = EXCLUDED.short_long_term_debt,
                short_long_term_debt_total = EXCLUDED.short_long_term_debt_total,
                long_term_debt = EXCLUDED.long_term_debt,
                long_term_debt_total = EXCLUDED.long_term_debt_total,
                capital_lease_obligations = EXCLUDED.capital_lease_obligations,
                accumulated_amortization = EXCLUDED.accumulated_amortization,
                accumulated_depreciation = EXCLUDED.accumulated_depreciation,
                non_current_liabilities_other = EXCLUDED.non_current_liabilities_other,
                non_current_liabilities_total = EXCLUDED.non_current_liabilities_total,
                total_permanent_equity = EXCLUDED.total_permanent_equity,
                other_stockholder_equity = EXCLUDED.other_stockholder_equity,
                total_stockholder_equity = EXCLUDED.total_stockholder_equity,
                common_stock_total_equity = EXCLUDED.common_stock_total_equity,
                preferred_stock_total_equity = EXCLUDED.preferred_stock_total_equity,
                retained_earnings_total_equity = EXCLUDED.retained_earnings_total_equity,
                liabilities_and_stockholders_equity = EXCLUDED.liabilities_and_stockholders_equity,
                common_stock = EXCLUDED.common_stock,
                retained_earnings = EXCLUDED.retained_earnings,
                goodwill = EXCLUDED.goodwill,
                long_term_investments = EXCLUDED.long_term_investments,
                short_term_investments = EXCLUDED.short_term_investments,
                net_receivables = EXCLUDED.net_receivables,
                inventory = EXCLUDED.inventory,
                accounts_payable = EXCLUDED.accounts_payable,
                noncontrolling_interest_in_consolidated_entity = EXCLUDED.noncontrolling_interest_in_consolidated_entity,
                temporary_equity_redeemable_noncontrolling_interests = EXCLUDED.temporary_equity_redeemable_noncontrolling_interests,
                accumulated_other_comprehensive_income = EXCLUDED.accumulated_other_comprehensive_income,
                additional_paid_in_capital = EXCLUDED.additional_paid_in_capital,
                treasury_stock = EXCLUDED.treasury_stock,
                negative_goodwill = EXCLUDED.negative_goodwill,
                warrants = EXCLUDED.warrants,
                preferred_stock_redeemable = EXCLUDED.preferred_stock_redeemable,
                capital_surplus = EXCLUDED.capital_surplus,
                common_stock_shares_outstanding = EXCLUDED.common_stock_shares_outstanding,
                investments = EXCLUDED.investments,
                change_to_liabilities = EXCLUDED.change_to_liabilities,
                total_cashflows_from_investing_activities = EXCLUDED.total_cashflows_from_investing_activities,
                net_borrowings = EXCLUDED.net_borrowings,
                total_cash_from_financing_activities = EXCLUDED.total_cash_from_financing_activities,
                change_to_operating_activities = EXCLUDED.change_to_operating_activities,
                change_in_cash = EXCLUDED.change_in_cash,
                begin_period_cashflow = EXCLUDED.begin_period_cashflow,
                end_period_cashflow = EXCLUDED.end_period_cashflow,
                total_cash_from_operating_activities = EXCLUDED.total_cash_from_operating_activities,
                depreciation = EXCLUDED.depreciation,
                other_cashflows_from_investing_activities = EXCLUDED.other_cashflows_from_investing_activities,
                dividends_paid = EXCLUDED.dividends_paid,
                change_to_inventory = EXCLUDED.change_to_inventory,
                change_to_account_receivables = EXCLUDED.change_to_account_receivables,
                sale_purchase_of_stock = EXCLUDED.sale_purchase_of_stock,
                other_cashflows_from_financing_activities = EXCLUDED.other_cashflows_from_financing_activities,
                change_to_net_income = EXCLUDED.change_to_net_income,
                capital_expenditures = EXCLUDED.capital_expenditures,
                change_receivables = EXCLUDED.change_receivables,
                cashflows_other_operating = EXCLUDED.cashflows_other_operating,
                exchange_rate_changes = EXCLUDED.exchange_rate_changes,
                cash_and_cash_equivalents_changes = EXCLUDED.cash_and_cash_equivalents_changes,
                change_in_working_capital = EXCLUDED.change_in_working_capital,
                other_non_cash_items = EXCLUDED.other_non_cash_items,
                free_cashflow = EXCLUDED.free_cashflow,
                research_development = EXCLUDED.research_development,
                effect_of_accounting_charges = EXCLUDED.effect_of_accounting_charges,
                income_before_tax = EXCLUDED.income_before_tax,
                minority_interest = EXCLUDED.minority_interest,
                net_income = EXCLUDED.net_income,
                selling_general_administrative = EXCLUDED.selling_general_administrative,
                selling_and_marketing_expenses = EXCLUDED.selling_and_marketing_expenses,
                gross_profit = EXCLUDED.gross_profit,
                reconciled_depreciation = EXCLUDED.reconciled_depreciation,
                ebit = EXCLUDED.ebit,
                ebitda = EXCLUDED.ebitda,
                depreciation_and_amortization = EXCLUDED.depreciation_and_amortization,
                non_operating_income_net_other = EXCLUDED.non_operating_income_net_other,
                operating_income = EXCLUDED.operating_income,
                other_operating_expenses = EXCLUDED.other_operating_expenses,
                interest_expense = EXCLUDED.interest_expense,
                tax_provision = EXCLUDED.tax_provision,
                interest_income = EXCLUDED.interest_income,
                net_interest_income = EXCLUDED.net_interest_income,
                extraordinary_items = EXCLUDED.extraordinary_items,
                non_recurring = EXCLUDED.non_recurring,
                other_items = EXCLUDED.other_items,
                income_tax_expense = EXCLUDED.income_tax_expense,
                total_revenue = EXCLUDED.total_revenue,
                total_operating_expenses = EXCLUDED.total_operating_expenses,
                cost_of_revenue = EXCLUDED.cost_of_revenue,
                total_other_income_expense_net = EXCLUDED.total_other_income_expense_net,
                discontinued_operations = EXCLUDED.discontinued_operations,
                net_income_from_continuing_operations = EXCLUDED.net_income_from_continuing_operations,
                net_income_applicable_to_common_shares = EXCLUDED.net_income_applicable_to_common_shares,
                preferred_stock_and_other_adjustments = EXCLUDED.preferred_stock_and_other_adjustments,
                outstanding_shares = EXCLUDED.outstanding_shares
            ;
        """

    if sql != '':
        db.get_bind().execute(sql)


def update_fundamentals(db, tickers, API_URL, API_TOKEN):
    '''
    Updates companies_analytics.
    '''

    workload = len(tickers)

    print(f'Startet fundamentals update, workload: {workload}')

    # loop through companies
    for ticker in tqdm(tickers):

        for i in range(10):
            try:
                fundamentals = get_fundamentals(ticker, API_URL, API_TOKEN)
                break
            except requests.exceptions.ReadTimeout:
                if i < 9:
                    continue
                else:
                    print(f'Too many timeouts at {ticker}.')

        if fundamentals is not None:
            if fundamentals.get('General') is not None:
                if fundamentals['General'].get('Type') is not None:
                    if fundamentals['General']['Type'] == 'Common Stock':
                        try:
                            update_annual(db, ticker, fundamentals)
                            update_quarterly(db, ticker, fundamentals)
                        except Exception as e:
                            print(f'Error at {ticker}: {str(e)[:1000]}')

        db.commit()

    print('Finished fundamentals update.')
