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
    
    def transform_alpha_vantage_data(data):
        transformed_data = {
            'ASSETS': {
                'Current Assets': {
                    'Cash and cash equivalents': {
                        'Cash': data.get('cashAndCashEquivalentsAtCarryingValue', 0),
                        'Cash equivalents': data.get('cashAndShortTermInvestments', 0),
                    },
                    'Short-term investments': {
                        'Securities held-for-trading': data.get('shortTermInvestments', 0),
                        'Provision for securities': 0,  # Placeholder, add logic if needed
                        'Held-to-maturity investments': data.get('longTermInvestments', 0),
                    },
                    'Current receivables': {
                        'Trade receivables': data.get('currentNetReceivables', 0),
                        'Advances to suppliers': 0,  # Placeholder, add logic if needed
                        'Receivables from related parties': 0,  # Placeholder, add logic if needed
                        'Receivables from construction contract': 0,  # Placeholder
                        'Receivables from short-term loans': 0,  # Placeholder
                        'Other receivables': 0,  # Placeholder
                        'Provision for obsolete inventories': 0,  # Placeholder
                        'Shortage of assets awaiting resolution': 0,  # Placeholder
                    },
                    'Inventories': {
                        'Inventories': data.get('inventory', 0),
                        'Provision for obsolete inventories': 0,  # Placeholder
                    },
                    'Other short-term assets': {
                        'Short-term prepaid expenses': 0,  # Placeholder
                        'Value added tax deductibles': 0,  # Placeholder
                        'Statutory obligations': 0,  # Placeholder
                        'Trading Government bonds': 0,  # Placeholder
                        'Other current assets': 0,  # Placeholder
                    },
                },
                'Non-current assets': {
                    'Non-current receivables': {
                        'Long-term trade receivables': 0,  # Placeholder
                        'Long-term advance to suppliers': 0,  # Placeholder
                        'Working capital from subunits': 0,  # Placeholder
                        'Long-term receivables from related parties': 0,  # Placeholder
                        'Long-term loan receivables': 0,  # Placeholder
                        'Long-term other receivables': 0,  # Placeholder
                        'Provision for bad debts': 0,  # Placeholder
                    },
                    'Fixed assets': {
                        'Tangible fixed assets': {
                            'Cost': 0,  # Placeholder
                            'Accumulated depreciation': 0,  # Placeholder
                        },
                        'Fixed assets of finance leasing': {
                            'Cost': 0,  # Placeholder
                            'Accumulated depreciation': 0,  # Placeholder
                        },
                        'Intangible fixed assets': {
                            'Cost': 0,  # Placeholder
                            'Accumulated depreciation': 0,  # Placeholder
                        },
                    },
                    'Investment properties': {
                        'Cost': 0,  # Placeholder
                        'Accumulated depreciation': 0,  # Placeholder
                    },
                    'Long-term assets in progress': {
                        'Long-term work in progress': 0,  # Placeholder
                        'Long-term construction in progress': 0,  # Placeholder
                    },
                    'Long-term investments': {
                        'Investments in subsidiary': 0,  # Placeholder
                        'Investment in joint-venture, associates': 0,  # Placeholder
                        'Other long-term investments': 0,  # Placeholder
                        'Provision for long-term investments': 0,  # Placeholder
                        'Held-to-maturity investments': 0,  # Placeholder
                    },
                    'Other long-term assets': {
                        'Long-term prepaid expenses': 0,  # Placeholder
                        'Deferred tax assets': 0,  # Placeholder
                        'LT Equipment, materials and spare parts': 0,  # Placeholder
                        'Other long-term assets': 0,  # Placeholder
                        'Goodwill': 0,  # Placeholder
                    },
                },
                'Total Assets': data.get('totalAssets', 0),
            },
            'RESOURCES': {
                'Liabilities': {
                    'Current liabilities': {
                        'Trade payables': data.get('currentAccountsPayable', 0),
                        'Advances from customers': 0,  # Placeholder
                        'Statutory obligations': 0,  # Placeholder
                        'Payables to employees': 0,  # Placeholder
                        'Accrued expenses': 0,  # Placeholder
                        'Payables to related parties': 0,  # Placeholder
                        'Payables from construction contract': 0,  # Placeholder
                        'Short-term deferred revenue': 0,  # Placeholder
                        'Other ST payables': 0,  # Placeholder
                        'Short-term loan and payable for finance leasing': 0,  # Placeholder
                        'Provision for ST payable': 0,  # Placeholder
                        'Reward and welfare funds': 0,  # Placeholder
                        'Stabilization fund': 0,  # Placeholder
                        'Trading Government bonds': 0,  # Placeholder
                    },
                    'Non-current liabilities': {
                        'Long-term trade payables': 0,  # Placeholder
                        'Long-term advance to customers': 0,  # Placeholder
                        'Long-term accruals': 0,  # Placeholder
                        'Working capital from subunits': 0,  # Placeholder
                        'Long-term payables to related parties': 0,  # Placeholder
                        'Long-term deferred revenue': 0,  # Placeholder
                        'Other long-term liabilities': 0,  # Placeholder
                        'Long-term loans and debts': 0,  # Placeholder
                        'Convertible bond': 0,  # Placeholder
                        'Preference shares': 0,  # Placeholder
                        'Deferred tax liabilities': 0,  # Placeholder
                        'Provision for bad debts': 0,  # Placeholder
                        'The development of science and technology fund': 0,  # Placeholder
                    },
                },
                'Owner\'s equity': {
                    'Capital': {
                        'Contributed chartered capital': 0,  # Placeholder
                        'Ordinary shares': 0,  # Placeholder
                        'Preference shares': 0,  # Placeholder
                    },
                    'Surplus Equity': {
                        'Share premium': 0,  # Placeholder
                        'Other equity': 0,  # Placeholder
                        'Treasury shares': 0,  # Placeholder
                        'Asset revaluation difference': 0,  # Placeholder
                        'Foreign exchange gain/loss': 0,  # Placeholder
                        'Supplementary capital reserve fund': 0,  # Placeholder
                        'Financial reserve fund': 0,  # Placeholder
                        'Other fund of owners’ equity': 0,  # Placeholder
                    },
                    'Undistributed earnings': {
                        'Previous year undistributed earnings': 0,  # Placeholder
                        'This year undistributed earnings': 0,  # Placeholder
                        'Construction investment fund': 0,  # Placeholder
                        'Non-controlling interest': 0,  # Placeholder
                    },
                },
                'Total Resources': data.get('totalLiabilities', 0) + data.get('totalShareholderEquity', 0),
            }
        }
        return transformed_data
    
    def transform_cafef_data(data):
        mapping = {
            "TÀI SẢN": "ASSETS",
            "A- TÀI SẢN NGẮN HẠN": "Current Assets",
            "I. Tiền và các khoản tương đương tiền": "Cash and cash equivalents",
            "1. Tiền": "Cash",
            "2. Các khoản tương đương tiền": "Cash equivalents",
            "II. Các khoản đầu tư tài chính ngắn hạn": "Short-term investments",
            "1. Chứng khoán kinh doanh": "Securities held - for -trading",
            "2. Dự phòng giảm giá chứng khoán kinh doanh": "Provision for securities",
            "3. Đầu tư nắm giữ đến ngày đáo hạn": "Held-to-maturity investments",
            "III. Các khoản phải thu ngắn hạn": "Current receivables",
            "1. Phải thu ngắn hạn của khách hàng": "Trade receivables",
            "2. Trả trước cho người bán ngắn hạn": "Advances to suppliers",
            "3. Phải thu nội bộ ngắn hạn": "Receivables from related parties",
            "4. Phải thu theo tiến độ kế hoạch hợp đồng xây dựng": "Receivables from construction contract",
            "5. Phải thu về cho vay ngắn hạn": "Receivables from short-term loans",
            "6. Phải thu ngắn hạn khác": "Other receivables",
            "7. Dự phòng phải thu ngắn hạn khó đòi": "Provision for obsolete inventories",
            "8. Tài sản Thiếu chờ xử lý": "Shortage of assets awaiting resolution",
            "IV. Hàng tồn kho": "Inventories",
            "1. Hàng tồn kho": "Inventories",
            "2. Dự phòng giảm giá hàng tồn kho": "Provision for obsolete inventories",
            "V.Tài sản ngắn hạn khác": "Other short-term assets",
            "1. Chi phí trả trước ngắn hạn": "Short-term prepaid expenses",
            "2. Thuế GTGT được khấu trừ": "Value added tax deductibles",
            "3. Thuế và các khoản khác phải thu Nhà nước": "Statutory obligations",
            "4. Giao dịch mua bán lại trái phiếu Chính phủ": "Trading Government bonds",
            "5. Tài sản ngắn hạn khác": "Other current assets",
            "B. TÀI SẢN DÀI HẠN": "Non-current assets",
            "I. Các khoản phải thu dài hạn": "Non-current receivables",
            "1. Phải thu dài hạn của khách hàng": "Long term trade receivables",
            "2. Trả trước cho người bán dài hạn": "Long term advance to suppliers",
            "3. Vốn kinh doanh ở đơn vị trực thuộc": "Working capital from subunits",
            "4. Phải thu nội bộ dài hạn": "Long term receivables from related parties",
            "5. Phải thu về cho vay dài hạn": "Long term loan receivables",
            "6. Phải thu dài hạn khác": "Long term other receivables",
            "7. Dự phòng phải thu dài hạn khó đòi": "Provision for bad debts",
            "II.Tài sản cố định": "Fixed assets",
            "1. Tài sản cố định hữu hình": "Tangible fixed assets",
            " - Nguyên giá": "Cost",
            " - Giá trị hao mòn lũy kế": "Accumulated depreciation",
            "2. Tài sản cố định thuê tài chính": "Fixed assets of finance leasing",
            "3. Tài sản cố định vô hình": "Intangible fixed assets",
            "4. Bất động sản đầu tư": "Investment properties",
            "5. Tài sản dở dang dài hạn": "Long term asstes in progress",
            "1. Chi phí sản xuất, kinh doanh dở dang dài hạn": "Long term work in progress",
            "2. Chi phí xây dựng cơ bản dở dang": "Long-term construction in progress",
            "V. Đầu tư tài chính dài hạn": "Long-term investments",
            "1. Đầu tư vào công ty con": "Investments in subsidiary",
            "2. Đầu tư vào công ty liên kết, liên doanh": "Investment in joint-venture, associates",
            "3. Đầu tư góp vốn vào đơn vị khác": "Other long-term investments",
            "4. Dự phòng đầu tư tài chính dài hạn": "Provision for long-term investments",
            "5. Đầu tư nắm giữ đến ngày đáo hạn": "Held-to-maturity investments",
            "VI. Tài sản dài hạn khác": "Other long-term assets",
            "1. Chi phí trả trước dài hạn": "Long-term prepaid expenses",
            "2. Tài sản thuế thu nhập hoãn lại": "Deferred tax assets",
            "3. Thiết bị, vật tư, phụ tùng thay thế dài hạn": "LT Equipment, materials and spare parts",
            "4. Tài sản dài hạn khác": "Other long-term assets",
            "5. Lợi thế thương mại": "Goodwill",
            "TỔNG CỘNG TÀI SẢN": "Total Assets",
            "NGUỒN VỐN": "RESOURCES",
            "C. NỢ PHẢI TRẢ": "Liabilities",
            "I. Nợ ngắn hạn": "Current liabilities",
            "1. Phải trả người bán ngắn hạn": "Trade payables",
            "2. Người mua trả tiền trước ngắn hạn": "Advances from customers",
            "3. Thuế và các khoản phải nộp nhà nước": "Statutory obligations",
            "4. Phải trả người lao động": "Payables to employees",
            "5. Chi phí phải trả ngắn hạn": "Accrued expenses",
            "6. Phải trả nội bộ ngắn hạn": "Payables to related parties",
            "7. Phải trả theo tiến độ kế hoạch hợp đồng xây dựng": "Payables from construction contract",
            "8. Doanh thu chưa thực hiện ngắn hạn": "Shorterm-deferred revenue",
            "9. Phải trả ngắn hạn khác": "Other ST payables",
            "10. Vay và nợ thuê tài chính ngắn hạn": "Short-term loan and payable for finance leasing",
            "11. Dự phòng phải trả ngắn hạn": "Provision for ST payable",
            "12. Quỹ khen thưởng phúc lợi": "Reward and welfare funds",
            "13. Quỹ bình ổn giá": "Stabilization fund",
            "14. Giao dịch mua bán lại trái phiếu Chính phủ": "Trading Government bonds",
            "II. Nợ dài hạn": "Non-current liabilities",
            "1. Phải trả người bán dài hạn": "Long term trade payables",
            "2. Người mua trả tiền trước dài hạn": "Long term advance to customers",
            "3. Chi phí phải trả dài hạn": "Long term accruals",
            "4. Phải trả nội bộ về vốn kinh doanh": "Working capital from subunits",
            "5. Phải trả nội bộ dài hạn": "Long term payables to related parties",
            "6. Doanh thu chưa thực hiện dài hạn": "Long term deferred revenue",
            "7. Phải trả dài hạn khác": "Other long term liabilities",
            "8. Vay và nợ thuê tài chính dài hạn": "Long term loans and debts",
            "9. Trái phiếu chuyển đổi": "Convertible bond",
            "10. Cổ phiếu ưu đãi": "Preference shares",
            "11. Thuế thu nhập hoãn lại phải trả": "Deferred tax liabilities",
            "12. Dự phòng phải trả dài hạn": "Provision for bad debts",
            "13. Quỹ phát triển khoa học và công nghệ": "The development of science and technology fund",
            "D.VỐN CHỦ SỞ HỮU": "Owner's equity",
            "I. Vốn chủ sở hữu": "Capital",
            "1. Vốn góp của chủ sở hữu": "Contributed chartered capital",
            " - Cổ phiếu phổ thông có quyền biểu quyết": "Ordinary shares",
            " - Cổ phiếu ưu đãi": "Preference shares",
            "2. Thặng dư vốn cổ phần": "Suplus Equity",
            "3. Quyền chọn chuyển đổi trái phiếu": "Share premium",
            "4. Vốn khác của chủ sở hữu": "Other equity",
            "5. Cổ phiếu quỹ": "Treasury shares",
            "6. Chênh lệch đánh giá lại tài sản": "Asset revaluation difference",
            "7. Chênh lệch tỷ giá hối đoái": "Foreign exchange gain/loss",
            "8. Quỹ đầu tư phát triển": "Supplementary capital reserve fund",
            "9. Lợi nhuận chưa phân phối": "Retained earnings",
            "10. Các quỹ khác": "Other funds",
            "TỔNG CỘNG NGUỒN VỐN": "Total sources of funding"
        }
        new_data = []
        for row in data:
            new_row = []
            for item in row:
                new_row.append(mapping.get(item, item)) 
            new_data.append(new_row)
        return new_data

