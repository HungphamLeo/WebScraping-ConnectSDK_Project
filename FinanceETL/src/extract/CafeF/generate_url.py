
URL_BASE = "https://s.cafef.vn/bao-cao-tai-chinh/"
BALANCE_SHEET = "bsheet"
INCOME_STATEMENT = "incsta"
CASH_FLOW = "cashflow"
CONST_TABLE = 4


def generate_balance_sheet_url(company_name, year, report_type="4 QUARTER", quarter="1"):   
    if report_type == "4 QUARTER":
        # print(f"{URL_BASE}{company_name}/{BALANCE_SHEET}/{year}/{quarter}/0/0/0/bao-cao-tai-chinh-.chn")
        return f"{URL_BASE}{company_name}/{BALANCE_SHEET}/{year}/{quarter}/0/0/0/bao-cao-tai-chinh-.chn"
    elif report_type == "1 YEAR":
        return f"{URL_BASE}{company_name}/{BALANCE_SHEET}/{year}/0/0/0/0/bao-cao-tai-chinh-.chn"
    else:
        raise ValueError("Invalid report_type. Choose either '4 QUARTER' or '1 YEAR'.")

def generate_income_statement_url(company_name, year, report_type="4 QUARTER", quarter="1"):
    if report_type == "4 QUARTER":
        return f"{URL_BASE}{company_name}/{INCOME_STATEMENT}/{year}/{quarter}/0/0/0/bao-cao-tai-chinh-.chn"
    elif report_type == "1 YEAR":
        return f"{URL_BASE}{company_name}/{INCOME_STATEMENT}/{year}/0/0/0/0/bao-cao-tai-chinh-.chn"
    else:
        raise ValueError("Invalid report_type. Choose either '4 QUARTER' or '1 YEAR'.")

def generate_cash_flow_url(company_name, year, report_type="4 QUARTER", quarter="1"):
    if report_type == "4 QUARTER":
        return f"{URL_BASE}{company_name}/{CASH_FLOW}/{year}/{quarter}/0/0/0/bao-cao-tai-chinh-.chn"
    elif report_type == "1 YEAR":
        return f"{URL_BASE}{company_name}/{CASH_FLOW}/{year}/0/0/0/0/bao-cao-tai-chinh-.chn"
    else:
        raise ValueError("Invalid report_type. Choose either '4 QUARTER' or '1 YEAR'.")


