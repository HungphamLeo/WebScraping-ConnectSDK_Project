import pandas as pd
import json
import pyarrow as pa
import pyarrow.hdfs as hdfs
from ...config.config import Config

class TransformStatementBase:
    def __init__(self, file_path: str, file_type: str):
        self.file_path = file_path
        self.file_type = file_type
        self.data = None
    
    def read_data(self):
        hdfs_client = hdfs.connect('localhost', port=50070)
        if self.file_type == 'json':
            with hdfs_client.open(self.file_path, 'rb') as f:
                self.data = json.load(f)
        elif self.file_type == 'xlsx':
            with hdfs_client.open(self.file_path, 'rb') as f:
                self.data = pd.read_excel(f)
        elif self.file_type == 'csv':
            with hdfs_client.open(self.file_path, 'rb') as f:
                self.data = pd.read_csv(f)
        else:
            raise ValueError("Unsupported file type: {}".format(self.file_type))
        
    def mapping_alpha_vantage_income_statement(self, data):
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


    def mapping_cafeF_income_statement(self, data):
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

    
    def mapping_alpha_vantage_balance_sheet(self,data):
        transformed_data = {
            'ASSETS': {
                'Current Assets': {
                    'Cash and cash equivalents': {
                        'Cash': data.get('cashAndCashEquivalentsAtCarryingValue', 0),
                        'Cash equivalents': data.get('cashAndShortTermInvestments', 0),
                    },
                    'Short-term investments': {
                        'Securities held-for-trading': data.get('shortTermInvestments', 0),
                        'Provision for securities': 0,
                        'Held-to-maturity investments': data.get('longTermInvestments', 0),
                    },
                    'Current receivables': {
                        'Trade receivables': data.get('currentNetReceivables', 0),
                        'Advances to suppliers': 0, 
                        'Receivables from related parties': 0,  
                        'Receivables from construction contract': 0,  
                        'Receivables from short-term loans': 0,  
                        'Other receivables': 0,  
                        'Provision for obsolete inventories': 0,  
                        'Shortage of assets awaiting resolution': 0,  
                    },
                    'Inventories': {
                        'Inventories': data.get('inventory', 0),
                        'Provision for obsolete inventories': 0,  
                    },
                    'Other short-term assets': {
                        'Short-term prepaid expenses': 0,  
                        'Value added tax deductibles': 0,  
                        'Statutory obligations': 0,  
                        'Trading Government bonds': 0,  
                        'Other current assets': 0,  
                    },
                },
                'Non-current assets': {
                    'Non-current receivables': {
                        'Long-term trade receivables': 0,  
                        'Long-term advance to suppliers': 0,  
                        'Working capital from subunits': 0,  
                        'Long-term receivables from related parties': 0,  
                        'Long-term loan receivables': 0,  
                        'Long-term other receivables': 0,  
                        'Provision for bad debts': 0,  
                    },
                    'Fixed assets': {
                        'Tangible fixed assets': {
                            'Cost': 0,  
                            'Accumulated depreciation': 0,  
                        },
                        'Fixed assets of finance leasing': {
                            'Cost': 0,  
                            'Accumulated depreciation': 0,  
                        },
                        'Intangible fixed assets': {
                            'Cost': 0,  
                            'Accumulated depreciation': 0,  
                        },
                    },
                    'Investment properties': {
                        'Cost': 0,  
                        'Accumulated depreciation': 0,  
                    },
                    'Long-term assets in progress': {
                        'Long-term work in progress': 0,  
                        'Long-term construction in progress': 0,  
                    },
                    'Long-term investments': {
                        'Investments in subsidiary': 0,  
                        'Investment in joint-venture, associates': 0,  
                        'Other long-term investments': 0,  
                        'Provision for long-term investments': 0,  
                        'Held-to-maturity investments': 0,  
                    },
                    'Other long-term assets': {
                        'Long-term prepaid expenses': 0,  
                        'Deferred tax assets': 0,  
                        'LT Equipment, materials and spare parts': 0,  
                        'Other long-term assets': 0,  
                        'Goodwill': 0,  
                    },
                },
                'Total Assets': data.get('totalAssets', 0),
            },
            'RESOURCES': {
                'Liabilities': {
                    'Current liabilities': {
                        'Trade payables': data.get('currentAccountsPayable', 0),
                        'Advances from customers': 0,  
                        'Statutory obligations': 0,  
                        'Payables to employees': 0,  
                        'Accrued expenses': 0,  
                        'Payables to related parties': 0,  
                        'Payables from construction contract': 0,  
                        'Short-term deferred revenue': 0,  
                        'Other ST payables': 0,  
                        'Short-term loan and payable for finance leasing': 0,  
                        'Provision for ST payable': 0,  
                        'Reward and welfare funds': 0,  
                        'Stabilization fund': 0,  
                        'Trading Government bonds': 0,  
                    },
                    'Non-current liabilities': {
                        'Long-term trade payables': 0,  
                        'Long-term advance to customers': 0,  
                        'Long-term accruals': 0,  
                        'Working capital from subunits': 0,  
                        'Long-term payables to related parties': 0,  
                        'Long-term deferred revenue': 0,  
                        'Other long-term liabilities': 0,  
                        'Long-term loans and debts': 0,  
                        'Convertible bond': 0,  
                        'Preference shares': 0,  
                        'Deferred tax liabilities': 0,  
                        'Provision for bad debts': 0,  
                        'The development of science and technology fund': 0,  
                    },
                },
                'Owner\'s equity': {
                    'Capital': {
                        'Contributed chartered capital': 0,  
                        'Ordinary shares': 0,  
                        'Preference shares': 0,  
                    },
                    'Surplus Equity': {
                        'Share premium': 0,  
                        'Other equity': 0,  
                        'Treasury shares': 0,  
                        'Asset revaluation difference': 0,  
                        'Foreign exchange gain/loss': 0,  
                        'Supplementary capital reserve fund': 0,  
                        'Financial reserve fund': 0,  
                        'Other fund of owners’ equity': 0,  
                    },
                    'Undistributed earnings': {
                        'Previous year undistributed earnings': 0,  
                        'This year undistributed earnings': 0,  
                        'Construction investment fund': 0,  
                        'Non-controlling interest': 0,  
                    },
                },
                'Total Resources': data.get('totalLiabilities', 0) + data.get('totalShareholderEquity', 0),
            }
        }
        
        return transformed_data
    
    def mapping_cafeF_balance_sheet(self,data):
        transformed_data = {
            'ASSETS': {
                'CURRENT ASSETS': {
                    'Cash and Cash Equivalents': {
                        'Cash': data.get('1. Tiền', 0),
                        'Cash Equivalents': data.get('2. Các khoản tương đương tiền', 0),
                    },
                    'Short-term Financial Investments': {
                        'Trading Securities': data.get('1. Chứng khoán kinh doanh', 0),
                        'Provision for Decline in Value of Trading Securities': data.get('2. Dự phòng giảm giá chứng khoán kinh doanh', 0),
                        'Investments Held to Maturity': data.get('3. Đầu tư nắm giữ đến ngày đáo hạn', 0),
                    },
                    'Short-term Receivables': {
                        'Receivables from Customers': data.get('1. Phải thu ngắn hạn của khách hàng', 0),
                        'Advance Payments to Suppliers': data.get('2. Trả trước cho người bán ngắn hạn', 0),
                        'Internal Receivables': data.get('3. Phải thu nội bộ ngắn hạn', 0),
                        'Receivables from Construction Contracts': data.get('4. Phải thu theo tiến độ kế hoạch hợp đồng xây dựng', 0),
                        'Short-term Loans Receivable': data.get('5. Phải thu về cho vay ngắn hạn', 0),
                        'Other Short-term Receivables': data.get('6. Phải thu ngắn hạn khác', 0),
                        'Provision for Doubtful Receivables': data.get('7. Dự phòng phải thu ngắn hạn khó đòi', 0),
                        'Assets Awaiting Settlement': data.get('8. Tài sản Thiếu chờ xử lý', 0),
                    },
                    'Inventory': {
                        'Inventory': data.get('1. Hàng tồn kho', 0),
                        'Provision for Decline in Value of Inventory': data.get('2. Dự phòng giảm giá hàng tồn kho', 0),
                    },
                    'Other Current Assets': {
                        'Prepaid Expenses': data.get('1. Chi phí trả trước ngắn hạn', 0),
                        'Recoverable VAT': data.get('2. Thuế GTGT được khấu trừ', 0),
                        'Taxes and Other Receivables from the State': data.get('3. Thuế và các khoản khác phải thu Nhà nước', 0),
                        'Government Bond Buyback Transactions': data.get('4. Giao dịch mua bán lại trái phiếu Chính phủ', 0),
                        'Other Current Assets': data.get('5. Tài sản ngắn hạn khác', 0),
                    },
                },
                'LONG-TERM ASSETS': {
                    'Long-term Receivables': {
                        'Receivables from Customers (Long-term)': data.get('1. Phải thu dài hạn của khách hàng', 0),
                        'Advance Payments to Suppliers (Long-term)': data.get('2. Trả trước cho người bán dài hạn', 0),
                        'Working Capital in Subsidiaries': data.get('3. Vốn kinh doanh ở đơn vị trực thuộc', 0),
                        'Internal Receivables (Long-term)': data.get('4. Phải thu nội bộ dài hạn', 0),
                        'Long-term Loans Receivable': data.get('5. Phải thu về cho vay dài hạn', 0),
                        'Other Long-term Receivables': data.get('6. Phải thu dài hạn khác', 0),
                        'Provision for Doubtful Long-term Receivables': data.get('7. Dự phòng phải thu dài hạn khó đòi', 0),
                    },
                    'Fixed Assets': {
                        'Tangible Fixed Assets': {
                            'Original Cost': data.get('1. Tài sản cố định hữu hình. Nguyên giá', 0),
                            'Accumulated Depreciation': data.get('1. Tài sản cố định hữu hình. Giá trị hao mòn lũy kế', 0),
                        },
                        'Financial Lease Assets': {
                            'Original Cost': data.get('2. Tài sản cố định thuê tài chính. Nguyên giá', 0),
                            'Accumulated Depreciation': data.get('2. Tài sản cố định thuê tài chính. Giá trị hao mòn lũy kế', 0),
                        },
                        'Intangible Assets': {
                            'Original Cost': data.get('3. Tài sản cố định vô hình. Nguyên giá', 0),
                            'Accumulated Amortization': data.get('3. Tài sản cố định vô hình. Giá trị hao mòn lũy kế', 0),
                        },
                    },
                    'Investment Properties': {
                        'Original Cost': data.get('1. Bất động sản đầu tư. Nguyên giá', 0),
                        'Accumulated Depreciation': data.get('1. Bất động sản đầu tư. Giá trị hao mòn lũy kế', 0),
                    },
                    'Long-term Work in Progress': {
                        'Production Costs in Progress': data.get('1. Chi phí sản xuất, kinh doanh dở dang dài hạn', 0),
                        'Construction in Progress': data.get('2. Chi phí xây dựng cơ bản dở dang', 0),
                    },
                    'Long-term Financial Investments': {
                        'Investments in Subsidiaries': data.get('1. Đầu tư vào công ty con', 0),
                        'Investments in Associates and Joint Ventures': data.get('2. Đầu tư vào công ty liên kết, liên doanh', 0),
                        'Investments in Other Units': data.get('3. Đầu tư góp vốn vào đơn vị khác', 0),
                        'Provision for Long-term Financial Investments': data.get('4. Dự phòng đầu tư tài chính dài hạn', 0),
                        'Investments Held to Maturity': data.get('5. Đầu tư nắm giữ đến ngày đáo hạn', 0),
                    },
                    'Other Long-term Assets': {
                        'Prepaid Expenses (Long-term)': data.get('1. Chi phí trả trước dài hạn', 0),
                        'Deferred Tax Assets': data.get('2. Tài sản thuế thu nhập hoãn lại', 0),
                        'Long-term Spare Parts': data.get('3. Thiết bị, vật tư, phụ tùng thay thế dài hạn', 0),
                        'Other Long-term Assets': data.get('4. Tài sản dài hạn khác', 0),
                        'Goodwill': data.get('5. Lợi thế thương mại', 0),
                    },
                },
                'TOTAL ASSETS': data.get('TỔNG CỘNG TÀI SẢN', 0),
            },
            'LIABILITIES': {
                'CURRENT LIABILITIES': {
                    'Short-term Payables': {
                        'Payables to Suppliers': data.get('1. Phải trả người bán ngắn hạn', 0),
                        'Advance Payments from Customers': data.get('2. Người mua trả tiền trước ngắn hạn', 0),
                        'Taxes and Other Payables to the State': data.get('3. Thuế và các khoản phải nộp nhà nước', 0),
                        'Payables to Employees': data.get('4. Phải trả người lao động', 0),
                        'Accrued Expenses': data.get('5. Chi phí phải trả ngắn hạn', 0),
                        'Internal Payables': data.get('6. Phải trả nội bộ ngắn hạn', 0),
                        'Contractual Progress Payables': data.get('7. Phải trả theo tiến độ kế hoạch hợp đồng xây dựng', 0),
                        'Unearned Revenue': data.get('8. Doanh thu chưa thực hiện ngắn hạn', 0),
                        'Other Short-term Payables': data.get('9. Phải trả ngắn hạn khác', 0),
                        'Short-term Loans and Financial Lease Obligations': data.get('10. Vay và nợ thuê tài chính ngắn hạn', 0),
                        'Provision for Short-term Liabilities': data.get('11. Dự phòng phải trả ngắn hạn', 0),
                        'Bonus and Welfare Fund': data.get('12. Quỹ khen thưởng phúc lợi', 0),
                        'Price Stabilization Fund': data.get('13. Quỹ bình ổn giá', 0),
                        'Government Bond Buyback Transactions (Short-term)': data.get('14. Giao dịch mua bán lại trái phiếu Chính phủ', 0),
                    },
                },
                'LONG-TERM LIABILITIES': {
                    'Long-term Payables': {
                        'Payables to Suppliers (Long-term)': data.get('1. Phải trả người bán dài hạn', 0),
                        'Advance Payments from Customers (Long-term)': data.get('2. Người mua trả tiền trước dài hạn', 0),
                        'Accrued Expenses (Long-term)': data.get('3. Chi phí phải trả dài hạn', 0),
                        'Internal Payables (Working Capital)': data.get('4. Phải trả nội bộ về vốn kinh doanh', 0),
                        'Internal Payables (Long-term)': data.get('5. Phải trả nội bộ dài hạn', 0),
                        'Unearned Revenue (Long-term)': data.get('6. Doanh thu chưa thực hiện dài hạn', 0),
                        'Other Long-term Payables': data.get('7. Phải trả dài hạn khác', 0),
                    },
                    'Long-term Loans Payable': data.get('8. Vay dài hạn', 0),
                    'Financial Lease Obligations': data.get('9. Nợ thuê tài chính dài hạn', 0),
                    'Provision for Long-term Liabilities': data.get('10. Dự phòng phải trả dài hạn', 0),
                },
                'TOTAL LIABILITIES': data.get('C. NỢ PHẢI TRẢ', 0), 
            },
            'EQUITY': {
                'Chartered Capital': data.get('Vốn điều lệ', 0),
                'Additional Capital': data.get('Vốn bổ sung', 0),
                'Retained Earnings': data.get('Lợi nhuận chưa phân phối', 0),
                'Accumulated Other Comprehensive Income': data.get('Lợi nhuận khác', 0),
                'Reserves': data.get('Quỹ dự trữ', 0),
                'Other Comprehensive Income': data.get('Thu nhập toàn diện khác', 0),
                'TOTAL EQUITY': data.get('TỔNG CỘNG NGUỒN VỐN', 0), 
            },
        }

        return transformed_data


    def mapping_alpha_vantage_cash_flow(self,data):
        mapping_dict = {
            "Cash flows from operating activities": data.get("I. Lưu chuyển tiền từ hoạt động kinh doanh", 0),
            "Net profit before tax": data.get("1. Lợi nhuận trước thuế", 0),
            "Adjustments for": data.get("2. Điều chỉnh cho các khoản", 0),
            "Depreciation and amortisation": data.get("- Khấu hao TSCĐ và BĐSĐT", 0),
            "Provision for decline in value of investments": data.get("- Các khoản dự phòng", 0),
            "Unrealised foreign exchange losses": data.get("- Lãi, lỗ chênh lệch tỷ giá hối đoái do đánh giá lại các khoản mục tiền tệ có gốc ngoại tệ", 0),
            "Gain from disposal of equity investments in other entities": data.get("- Lãi, lỗ từ hoạt động đầu tư", 0),
            "Interest expenses": data.get("- Chi phí lãi vay", 0),
            "Other adjustment": data.get("- Các khoản điều chỉnh khác", 0),
            "Operating income before changes in working capital": data.get("3. Lợi nhuận từ hoạt động kinh doanh trước thay đổi vốn lưu động", 0),
            "Decrease/(increase) in receivables": data.get("- Tăng, giảm các khoản phải thu", 0),
            "Decrease/(increase) in inventories": data.get("- Tăng, giảm hàng tồn kho", 0),
            "Increase/Decreases in payables": data.get("- Tăng, giảm các khoản phải trả (Không kể lãi vay phải trả, thuế thu nhập doanh nghiệp phải nộp)", 0),
            "Decrease/(Increase) in prepaid expenses": data.get("- Tăng, giảm chi phí trả trước", 0),
            "Decrease/(Increase) in securities held for trading": data.get("- Tăng, giảm chứng khoán kinh doanh", 0),
            "Interest paid": data.get("- Tiền lãi vay đã trả", 0),
            "Enterprise income tax paid": data.get("- Thuế thu nhập doanh nghiệp đã nộp", 0),
            "Other income from business activities": data.get("- Tiền thu khác từ hoạt động kinh doanh", 0),
            "Other cash inflows/(outflows) from operating activities": data.get("- Tiền chi khác cho hoạt động kinh doanh", 0),
            "Total cash flows from operating activities": data.get("Lưu chuyển tiền thuần từ hoạt động kinh doanh", 0),

            "Cash flows from investing activities": data.get("II. Lưu chuyển tiền từ hoạt động đầu tư", 0),
            "Purchase and construction of fixed assets and other long-term assets": data.get("1.Tiền chi để mua sắm, xây dựng TSCĐ và các tài sản dài hạn khác", 0),
            "Proceeds from disposals of assets": data.get("2.Tiền thu từ thanh lý, nhượng bán TSCĐ và các tài sản dài hạn khác", 0),
            "Loans provided to related parties and other": data.get("3.Tiền chi cho vay, mua các công cụ nợ của đơn vị khác", 0),
            "Collection of loans provided to related parties and other": data.get("4.Tiền thu hồi cho vay, bán lại các công cụ nợ của đơn vị khác", 0),
            "Payments for equity investments in other entities": data.get("5.Tiền chi đầu tư góp vốn vào đơn vị khác", 0),
            "Proceed from collection investment in other entity": data.get("6.Tiền thu hồi đầu tư góp vốn vào đơn vị khác", 0),
            "Interest and dividend received": data.get("7.Tiền thu lãi cho vay, cổ tức và lợi nhuận được chia", 0),
            "Total cash flows from investing activities": data.get("Lưu chuyển tiền thuần từ hoạt động đầu tư", 0),

            "Cash flows from financing activities": data.get("III. Lưu chuyển tiền từ hoạt động tài chính", 0),
            "Proceeds from issuance of ordinary shares": data.get("1.Tiền thu từ phát hành cổ phiếu, nhận vốn góp của chủ sở hữu", 0),
            "Money to return contributed capital to owners, buy back shares of the issued enterprise": data.get("2.Tiền trả lại vón góp cho các chủ sở hữu, mua lại cổ phiếu của doanh nghiệp đã phát hành", 0),
            "Proceeds from bond issuance and borrowings": data.get("3.Tiền thu từ đi vay", 0),
            "Payments of loan": data.get("4.Tiền chi trả nợ gốc vay", 0),
            "Payments for principal of finance leaser": data.get("5.Tiền chi trả nợ gốc thuê tài chính", 0),
            "Dividend paid to owner": data.get("6.Cổ tức, lợi nhuận đã trả cho chủ sở hữu", 0),
            "Proceeds from capital contributions of non-controlling shareholders": data.get("7.Tiền thu từ vốn góp của cổ đông không kiểm soát", 0),
            "Total cash flows from financing activities": data.get("Lưu chuyển tiền thuần từ hoạt động tài chính", 0),

            "Net cash increase/(decrease)": data.get("Lưu chuyển tiền thuần trong kỳ (50 = 20+30+40)", 0),
            "Cash and cash equivalents at the beginning of the period": data.get("Tiền và tương đương tiền đầu kỳ", 0),
            "Impact of exchange rate fluctuation": data.get("Ảnh hưởng của thay đổi tỷ giá hối đoái quy đổi ngoại tệ", 0),
            "Cash and cash equivalents at the end of the period": data.get("Tiền và tương đương tiền cuối kỳ (70 = 50+60+61)", 0)
        }
        
        
        return mapping_dict

    def mapping_cafeF_cash_flow(self, data):
        mapping_dict = {
            "Cash flows from operating activities": data.get("operatingCashflow", 0),
            "Payments for operating activities": data.get("paymentsForOperatingActivities", 0),
            "Proceeds from operating activities": data.get("proceedsFromOperatingActivities", 0),
            "Change in operating liabilities": data.get("changeInOperatingLiabilities", 0),
            "Change in operating assets": data.get("changeInOperatingAssets", 0),
            "Depreciation, depletion, and amortization": data.get("depreciationDepletionAndAmortization", 0),
            "Capital expenditures": data.get("capitalExpenditures", 0),
            "Change in receivables": data.get("changeInReceivables", 0),
            "Change in inventory": data.get("changeInInventory", 0),
            "Net profit or loss": data.get("profitLoss", 0),
            "Cash flow from investing activities": data.get("cashflowFromInvestment", 0),
            "Cash flow from financing activities": data.get("cashflowFromFinancing", 0),
            "Proceeds from repayments of short-term debt": data.get("proceedsFromRepaymentsOfShortTermDebt", 0),
            "Payments for repurchase of common stock": data.get("paymentsForRepurchaseOfCommonStock", 0),
            "Payments for repurchase of equity": data.get("paymentsForRepurchaseOfEquity", 0),
            "Payments for repurchase of preferred stock": data.get("paymentsForRepurchaseOfPreferredStock", 0),
            "Dividend payout": data.get("dividendPayout", 0),
            "Dividend payout (common stock)": data.get("dividendPayoutCommonStock", 0),
            "Dividend payout (preferred stock)": data.get("dividendPayoutPreferredStock", 0),
            "Proceeds from issuance of common stock": data.get("proceedsFromIssuanceOfCommonStock", 0),
            "Proceeds from issuance of long-term debt and capital securities (net)": data.get("proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet", 0),
            "Proceeds from issuance of preferred stock": data.get("proceedsFromIssuanceOfPreferredStock", 0),
            "Proceeds from repurchase of equity": data.get("proceedsFromRepurchaseOfEquity", 0),
            "Proceeds from sale of treasury stock": data.get("proceedsFromSaleOfTreasuryStock", 0),
            "Change in cash and cash equivalents": data.get("changeInCashAndCashEquivalents", 0),
            "Change in exchange rate": data.get("changeInExchangeRate", 0),
            "Net income": data.get("netIncome", 0)
        }
        return mapping_dict
        
    
    

    
    