Finance ETL include:
 - extract data from multisourcing: Alpha_Valtage, Web CafeF, XTb
    Alpha_Valtage: get data from this platform, build function to get economics, stocks data, price ,... and format the common structure
    CafeF: get data by craw web data html. We focus to get finance statement annual or quarter of vietnam stocks: income statement, balance sheet, cash flow.
    => transform to final data of XTB and CafeF same.
    XTB: connect with platform with socket, api to get data for trading and save the real-time data on Redis for easily to get

Data lake: HDFS => Save raw data 
Load_db: Save the data proccessed to MySql and Mongo
