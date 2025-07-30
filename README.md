# WebScraping-ConnectSDK_Project (Finance ETL)

## Mục tiêu dự án

- Xây dựng hệ thống ETL (Extract – Transform – Load) cho dữ liệu tài chính từ nhiều nguồn khác nhau (đa nguồn).
- Tự động thu thập, xử lý và chuẩn hóa dữ liệu báo cáo tài chính (Balance Sheet, Income Statement, Cash Flow...) của các công ty niêm yết tại Việt Nam.
- Lưu trữ dữ liệu dạng raw và đã xử lý vào các hệ thống lưu trữ khác nhau, hỗ trợ phân tích, thống kê, hoặc trading tự động.

## Kiến trúc tổng thể

1. **Extract** (Trích xuất dữ liệu)
   - Thu thập dữ liệu từ các nguồn:
     - **Alpha_Vantage**: Kết nối API để lấy các dữ liệu kinh tế vĩ mô, giá chứng khoán, dữ liệu tài chính.
     - **CafeF**: Web scraping dữ liệu HTML để lấy báo cáo tài chính định kỳ (năm/quý) của các công ty Việt Nam.
     - **XTB**: Kết nối qua socket/API để lấy dữ liệu giao dịch (realtime), lưu trữ vào Redis để truy xuất nhanh.
2. **Transform** (Chuyển đổi dữ liệu)
   - Chuẩn hóa dữ liệu từ nhiều nguồn về cùng một cấu trúc.
   - Xử lý mapping cho từng trường dữ liệu, đối chiếu các chỉ tiêu tài chính giữa các nguồn (ví dụ mapping bảng cân đối kế toán, dòng tiền, lợi nhuận...).
   - Hỗ trợ kiểm soát chất lượng dữ liệu.
3. **Load** (Lưu trữ dữ liệu)
   - Dữ liệu raw lưu trữ vào Data Lake (HDFS).
   - Dữ liệu đã xử lý lưu vào MySQL (cho dữ liệu quan hệ) và MongoDB (cho dữ liệu phi cấu trúc/phân tích sâu).

## Công nghệ sử dụng

- **Ngôn ngữ**: Python 100%
- **Web Scraping**: Sử dụng requests, BeautifulSoup hoặc các thư viện scraping khác.
- **API Connector**: Kết nối đến các nguồn qua REST API, Websocket.
- **Xử lý dữ liệu**: Xây dựng các class transform để mapping và chuẩn hóa dữ liệu tài chính.
- **Storage**:
  - HDFS (Data Lake): Lưu dữ liệu thô (raw).
  - MySQL: Lưu dữ liệu đã ETL, phục vụ nghiệp vụ.
  - MongoDB: Lưu dữ liệu bán cấu trúc hoặc phục vụ phân tích nâng cao.
  - Redis: Lưu dữ liệu realtime trading (của XTB).
- **Hệ thống**: Có khả năng mở rộng để bổ sung thêm nguồn dữ liệu mới hoặc luồng xử lý mới.

## Đầu ra của hệ thống

- Bộ dữ liệu tài chính sạch, chuẩn hóa, thống nhất từ nhiều nguồn cho các công ty niêm yết.
- Dữ liệu lưu trữ sẵn sàng cho các bài toán phân tích, dashboard, hoặc trading algorithm.
- Có thể tích hợp thêm các mô-đun phân tích, dự báo, cảnh báo tài chính dựa trên dữ liệu đã chuẩn hóa.

## Sơ đồ luồng dữ liệu

```
[Alpha_Vantage/API]   [CafeF/Web Scraping]   [XTB/API/Socket]
         |                     |                     |
         |----------------[Extract Layer]------------|
                               |
                        [Transform Layer]
                               |
           ------------------------------------------------
           |                       |                     |
        [HDFS]                 [MySQL]               [MongoDB]
   (raw data lake)      (processed data - ETL)   (semi-structured data)
                               |
                            [Redis]
                      (realtime trading data)
```

## Ghi chú

- Hệ thống dễ dàng mở rộng thêm nguồn dữ liệu tài chính khác hoặc tích hợp các tầng xử lý dữ liệu nâng cao.
- Các script transform có khả năng mapping báo cáo tài chính từ nhiều chuẩn khác nhau về một cấu trúc chung.
