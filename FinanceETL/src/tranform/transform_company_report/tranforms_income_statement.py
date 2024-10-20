import pandas as pd
import json
import pyarrow as pa
import pyarrow.hdfs as hdfs
from .transform_company_report_base import TransformStatementBase
from ...config.config import Config

class TransformIncomeStatement(TransformStatementBase)
    def __init__(self, file_path: str, file_type: str):
        self.file_path = file_path
        self.file_type = file_type
        self.data = None

    def transform_data(self, data, source: str):
        if not self.data:
            return None
        if source == Config.ALPHA_VANTAGE:
            return self.transform_alpha_vantage_data(data)
        elif source == Config.CAFE_F:
            return self.transform_cafeF_data(data)
        else:
            raise ValueError("Unsupported source: {}".format(source))
    
    def transform_alpha_vantage_data(self, data):
        transformed_data = {
            'Revenue from sale of goods and rendering of services': data.loc['totalRevenue'],
            'Deductions': data.loc['costOfRevenue'],
            'Net revenue from sale of goods and rendering of services': data.loc['totalRevenue'] - data.loc['costOfRevenue'],
            'Costs of goods sold and services rendered': data.loc['costofGoodsAndServicesSold'],
            'Gross profit from sale of goods and rendering of services': data.loc['grossProfit'],
            'Income from financial activities': data.loc['investmentIncomeNet'],
            'Expenses from financial activities': data.loc['interestExpense'],
            'In which: Interest expenses': data.loc['interestExpense'],
            'Share in profits of associates': data.loc.get('nonInterestIncome', 0),
            'Selling expenses': data.loc.get('sellingGeneralAndAdministrative', 0),
            'Administration revenue': data.loc.get('operatingExpenses', 0),
            'Operating profit': data.loc['operatingIncome'],
            'Other Income': data.loc.get('otherNonOperatingIncome', 0),
            'Other expense': data.loc.get('depreciation', 0) + data.loc.get('depreciationAndAmortization', 0),
            'Other profit': data.loc['netIncomeFromContinuingOperations'] - data.loc['incomeBeforeTax'],
            'Net profit before tax': data.loc['incomeBeforeTax'],
            'Current corporate income tax expense': data.loc['incomeTaxExpense'],
            'Deferred corporate income tax expense': data.loc.get('deferredTaxExpense', 0),
            'Net profit after tax': data.loc['netIncome'],
            'Net profit after tax of the parent': data.loc.get('netIncome', 0),
            'Equity holders of NCI': data.loc.get('netIncome', 0),
            'Diluted EPS': data.loc.get('dilutedEPS', 0)
        }
        return transformed_data


    def transform_cafeF_data(self, data):
        transformed_data = {
            'Revenue from sale of goods and rendering of services': data.loc['1. Doanh thu bán hàng và cung cấp dịch vụ'],
            'Deductions': data.loc['2. Các khoản giảm trừ doanh thu'],
            'Net revenue from sale of goods and rendering of services': data.loc['3. Doanh thu thuần về bán hàng và cung cấp dịch vụ (10 = 01 - 02)'],
            'Costs of goods sold and services rendered': data.loc['4. Giá vốn hàng bán'],
            'Gross profit from sale of goods and rendering of services': data.loc['5. Lợi nhuận gộp về bán hàng và cung cấp dịch vụ(20=10-11)'],
            'Income from financial activities': data.loc['6. Doanh thu hoạt động tài chính'],
            'Expenses from financial activities': data.loc['7. Chi phí tài chính'],
            'In which: Interest expenses': data.loc.get('7.1 Trong đó: Chi phí lãi vay', 0),
            'Share in profits of associates': data.loc.get('8. Phần lãi lỗ trong công ty liên doanh, liên kết', 0),
            'Selling expenses': data.loc.get('9. Chi phí bán hàng', 0),
            'Administration revenue': data.loc.get('10. Chi phí quản lý doanh nghiệp', 0),
            'Operating profit': data.loc['11. Lợi nhuận thuần từ hoạt động kinh doanh{30=20+(21-22) + 24 - (25+26)}'],
            'Other Income': data.loc.get('12. Thu nhập khác', 0),
            'Other expense': data.loc.get('13. Chi phí khác', 0),
            'Other profit': data.loc['14. Lợi nhuận khác(40=31-32)'],
            'Net profit before tax': data.loc['15. Tổng lợi nhuận kế toán trước thuế(50=30+40)'],
            'Current corporate income tax expense': data.loc['16. Chi phí thuế TNDN hiện hành'],
            'Deferred corporate income tax expense': data.loc.get('17. Chi phí thuế TNDN hoãn lại', 0),
            'Net profit after tax': data.loc['18. Lợi nhuận sau thuế thu nhập doanh nghiệp(60=50-51-52)'],
            'Net profit after tax of the parent': data.loc.get('19. Lợi nhuận sau thuế công ty mẹ', 0),
            'Equity holders of NCI': data.loc.get('20. Lợi nhuận sau thuế công ty mẹ không kiểm soát', 0),
            'Diluted EPS': data.loc.get('22. Lãi suy giảm trên cổ phiếu (*)', 0)
        }
        return transformed_data

    