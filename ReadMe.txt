1.設定config
    "MongodbServerIP" : "127.0.0.1", //MongoDB ip
    "DBName":"Leapsy_clock_in_system", //資料庫名稱
    "Collection": "record-csv", //資料表名稱
    "StartDate" : "20201012",	//讀檔起始年月日
    "EndDate" : "20201105",	//讀檔結束年月日
    "FolderPath":"\\\\leapsy-nas3\\CheckInRecord\\",	目錄資料夾
    "bitOfEmployeeID": 3  //存到資料庫的員工編號要取幾位數

2.st檔案都在路徑"FolderPath"下,檔案要用子目錄分類，以年月來分類，一個資料夾為同一個月份的資料
ex:資料夾201701 裡面的資料為 20170101.st ~ 20170131.st 

3.ts檔名格式為年月日
ex:20201012.st

4.ts匯出格式:
每行為固定位置的: 卡號 日期 時間 員工編號 姓名 進出訊息
ex:          101  1769464887002017/06/0714:45:37正常進出*3                              NO                                                00005曾偉權         00   

5.每個欄位都在固定位置出現，僅特定欄位有差異，程式有做判斷(因此抓的內容都是以固定文字位置去抓)

6.csv或ts檔案以記事本開啟編碼都要是Big5，用此軟體轉成UTF-8才不會有問題