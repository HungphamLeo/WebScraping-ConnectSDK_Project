from get_company_info import CompanyInfoBase
from config import BANK_LIST

company_name = 'vib'
company_info = CompanyInfoBase()
table = company_info.get_all_company_info(BANK_LIST)
print(table)