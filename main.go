package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	//"labix.org/v2/mgo"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"golang.org/x/text/encoding/traditionalchinese" // 繁體中文編碼
	"golang.org/x/text/transform"
)

//設定檔
var config Config = Config{}
var worker = runtime.NumCPU()

// 指定編碼:將繁體Big5轉成UTF-8才會正確
var big5ToUTF8Decoder = traditionalchinese.Big5.NewDecoder()

//Log檔
var log_info *logrus.Logger
var log_err *logrus.Logger

//檔案的開始與結束日期(轉Time格式)
var dateStart time.Time
var dateEnd time.Time

// 設定檔(欄位命名要大寫)
type Config struct {
	MongodbServerIP  string //IP
	DBName           string
	Collection       string
	StartDate        string // 讀檔開始日
	EndDate          string // 讀檔結束日
	FolderPath       string // 目錄資料夾路徑
	BitsOfEmployeeID string //存到資料庫的員工編號要取幾位數
}

// 打卡紀錄(.ts檔) 按照ts檔順序
type DailyRecordByTsFile struct {
	CardID     string    `cardID`     //卡號
	Date       string    `date`       //日期
	Time       string    `time`       //時間
	EmployeeID string    `employeeID` //員工編號
	Name       string    `name`       //姓名
	Message    string    `msg`        //進出訊息
	DateTime   time.Time `dateTime`   //日期+時間
}

/** 初始化配置 */
func init() {

	fmt.Println("執行init()初始化")

	/**設定LOG檔層級與輸出格式*/
	//使用Info層級
	path := "./log/info/info"
	writer, _ := rotatelogs.New(
		path+".%Y%m%d%H",                            // 檔名格式
		rotatelogs.WithLinkName(path),               // 生成软链，指向最新日志文件
		rotatelogs.WithMaxAge(10080*time.Minute),    // 文件最大保存時間(保留七天)
		rotatelogs.WithRotationTime(60*time.Minute), // 日誌切割時間間隔(一小時存一個檔案)
	)

	// 設定LOG等級
	pathMap := lfshook.WriterMap{
		logrus.InfoLevel: writer,
		//logrus.PanicLevel: writer, //若執行發生錯誤則會停止不進行下去
	}

	log_info = logrus.New()
	log_info.Hooks.Add(lfshook.NewHook(pathMap, &logrus.JSONFormatter{})) //Log檔綁訂相關設定

	fmt.Println("結束Info等級設定")

	//Error層級
	path = "./log/err/err"
	writer, _ = rotatelogs.New(
		path+".%Y%m%d%H",                            // 檔名格式
		rotatelogs.WithLinkName(path),               // 生成软链，指向最新日志文件
		rotatelogs.WithMaxAge(10080*time.Minute),    // 文件最大保存時間(保留七天)
		rotatelogs.WithRotationTime(60*time.Minute), // 日誌切割時間間隔(一小時存一個檔案)
	)

	// 設定LOG等級
	pathMap = lfshook.WriterMap{
		//logrus.InfoLevel: writer,
		logrus.ErrorLevel: writer,
		//logrus.PanicLevel: writer, //若執行發生錯誤則會停止不進行下去
	}

	log_err = logrus.New()
	log_err.Hooks.Add(lfshook.NewHook(pathMap, &logrus.JSONFormatter{})) //Log檔綁訂相關設定

	fmt.Println("結束Error等級設定")
	log_info.Info("結束Error等級設定")

	/*json設定檔*/
	file, _ := os.Open("config.json")
	buf := make([]byte, 2048)

	//將設定讀到config變數中
	n, _ := file.Read(buf)
	fmt.Println(string(buf))
	err := json.Unmarshal(buf[:n], &config)
	if err != nil {
		panic(err)

		log_err.WithFields(logrus.Fields{
			"err": err,
		}).Error("將設定讀到config變數中失敗")

		fmt.Println(err)
	}
}

func main() {

	/**轉換空白區隔+ts檔*/
	//計算開始結束日期
	countDateStartAndEnd()
	runtime.GOMAXPROCS(runtime.NumCPU())
	//轉換開始結束日期
	ImportDailyRecordBy_TsFile()
	//刪除多餘的Record
	deleteEmptyAndRedundantRecord()

}

/*導入每日打卡資料(.ts)*/
func ImportDailyRecordBy_TsFile() {

	// 建立 channel 存放 DailyRecord型態資料
	chanDailyRecordByTsFile := make(chan DailyRecordByTsFile)

	// 標記完成
	dones := make(chan struct{}, worker)

	// 將日打卡紀錄檔案內容讀出，並加到 chanDailyRecord 裡面
	go addDailyRecordForManyDays_TsFile(chanDailyRecordByTsFile)

	log_info.Info("抓出chanDailyRecordByTsFile: ", chanDailyRecordByTsFile)

	log_info.WithFields(logrus.Fields{
		"len(chanDailyRecordByTsFile)": len(chanDailyRecordByTsFile),
	}).Info("確認初始資料量")

	// 將chanDailyRecord 插入mongodb資料庫
	for i := 0; i < worker; i++ {
		go insertDailyRecord_TsFile(chanDailyRecordByTsFile, dones)
	}
	//等待完成
	awaitForCloseResult(dones)
	fmt.Println("日打卡紀錄(.ts)插入完畢")
}

func addDailyRecordForManyDays_TsFile(chanDailyRecordByTsFile chan<- DailyRecordByTsFile) {

	//readTsFile("20170626.st")
	//readTsFile("20170630.st")

	// 會包含dateEnd最後一天
	for myTime := dateStart; myTime != dateEnd.AddDate(0, 0, 1); myTime = myTime.AddDate(0, 0, 1) {

		// 檔名(年月日).ts
		fileName := myTime.Format("20060102") + ".st"
		log_info.Info("讀檔: ", fileName)
		fmt.Println("讀檔:", fileName)

		// 年月資料夾路徑
		folderNameByYearMonth := myTime.Format("200601")

		// 判斷檔案是否存在
		_, err := os.Lstat(config.FolderPath + folderNameByYearMonth + "\\" + fileName)
		//_, err := os.Lstat("\\\\leapsy-nas3\\CheckInRecord\\20170605-20201011(st)\\201706\\" + fileName)

		// 檔案不存在
		if err != nil {
			log_info.WithFields(logrus.Fields{
				"err": err,
				//"fileName": "\\\\leapsy-nas3\\CheckInRecord\\20170605-20201011(st)\\201706\\" + fileName,
				"fileName": config.FolderPath + folderNameByYearMonth + "\\" + fileName,
			}).Info("檔案不存在")

		} else {
			//檔案若存在

			//file, err := os.Open("Z:\\" + fileName) 打開每日打卡紀錄檔案(本機要先登入過目的地磁碟機才能正常運作)
			//file, err := os.Open("\\\\leapsy-nas3\\CheckInRecord\\20170605-20201011(st)\\201706\\" + fileName)
			file, err := os.Open(config.FolderPath + folderNameByYearMonth + "\\" + fileName)

			log_info.WithFields(logrus.Fields{
				"err":      err,
				"fileName": "\\\\leapsy-nas3\\CheckInRecord\\20170605-20201011(st)\\201706\\" + fileName,
			}).Info("打開檔案")

			// 讀檔
			if err != nil {

				log_err.WithFields(logrus.Fields{
					"err":      err,
					"fileName": fileName,
				}).Error("打開檔案失敗")

				//log.Fatal(err)
			}

			// 最後回收資源
			defer func() {
				if err = file.Close(); err != nil {
					log_err.WithFields(logrus.Fields{
						"err":      err,
						"fileName": fileName,
					}).Error("關閉檔案失敗")

					//log.Fatal(err)
				}
			}()

			// 讀檔
			scanner := bufio.NewScanner(file)

			// 一行一行讀
			counter := 0         // 行號
			for scanner.Scan() { // internally, it advances token based on sperator

				// 讀進一行(Big5)
				big5Line := scanner.Text()
				counter++

				//轉成utf8(繁體)
				utf8Line, _, _ := transform.String(big5ToUTF8Decoder, big5Line)

				// ADMIN(不入資料庫)
				if strings.Compare("ADMIN", utf8Line[140:145]) == 0 {
					fmt.Println("找到ADMIN:", utf8Line[140:145])

					log_info.WithFields(logrus.Fields{
						"fileName": fileName,
						"行號":       counter,
						"值":        utf8Line[140:145],
					}).Info("找到ADMIN")

				} else if strings.Compare(" ", utf8Line[144:145]) == 0 {
					// 空白(不入資料庫)
					fmt.Println("找到空白:", utf8Line[144:145])

					log_info.WithFields(logrus.Fields{
						"fileName": fileName,
						"行號":       counter,
						"值":        utf8Line[144:145],
					}).Info("找到空白")

				} else if strings.Compare("按密碼", utf8Line[58:67]) == 0 {
					// 人名(正常進出(按密碼))(入資料庫)
					fmt.Println("Name找到值: 非ADMIN 非空白 是按密碼)")

					//名字的開始位置
					positionOfName := 147

					//取卡號
					cardID := utf8Line[15:27]
					//取日期
					date := utf8Line[27:37]
					//取時間
					time := utf8Line[37:45]
					//取員工編號
					//employeeID := utf8Line[142:147]
					employeeID := getEmployeeIDwithNbit(utf8Line[142:147], config.BitsOfEmployeeID)
					//取姓名
					name := getName(utf8Line, positionOfName)
					//取進出訊息
					msg := utf8Line[45:68]
					//取dateTime
					dateTime := getDateTime(date, time)

					fmt.Println("整行抓取資料:", utf8Line[15:27], utf8Line[27:37], utf8Line[37:45], utf8Line[142:147], name, utf8Line[45:68], dateTime)

					log_info.WithFields(logrus.Fields{
						"fileName": fileName,
						"行號":       counter,
						"卡號":       cardID,
						"日期":       date,
						"時間":       time,
						"員工編號":     employeeID,
						//"姓名":       utf8Line[147:156],
						"姓名":   name,
						"進出訊息": msg,
						"日期時間": dateTime,
					}).Info("找到(按密碼):")

					dailyrecordbytsfile := DailyRecordByTsFile{
						cardID,
						date,
						time,
						employeeID,
						//utf8Line[147:156],
						name,
						msg,
						dateTime}

					// 存到channel裡面
					chanDailyRecordByTsFile <- dailyrecordbytsfile

				} else {
					// 人名(正常進出 / 密碼錯誤)(入資料庫)
					fmt.Println("Name找到值: 非ADMIN 非空白 非按密碼 行號=", counter)

					//名字的開始位置
					positionOfName := 144

					//取卡號
					cardID := utf8Line[15:27]
					//取日期
					date := utf8Line[27:37]
					//取時間
					time := utf8Line[37:45]
					//取員工編號
					//employeeID := utf8Line[139:144]
					employeeID := getEmployeeIDwithNbit(utf8Line[139:144], config.BitsOfEmployeeID)
					//取名字
					name := getName(utf8Line, positionOfName)
					//取進出訊息
					msg := utf8Line[45:57]
					//取dateTime
					dateTime := getDateTime(date, time)

					fmt.Println("整行抓取資料:", utf8Line[15:27], utf8Line[27:37], utf8Line[37:45], utf8Line[139:144], name, utf8Line[45:57], dateTime)

					log_info.WithFields(logrus.Fields{
						"fileName": fileName,
						"行號":       counter,
						"卡號":       cardID,
						"日期":       date,
						"時間":       time,
						"員工編號":     employeeID,
						//"姓名":       utf8Line[144:153],
						"姓名":   name,
						"進出訊息": msg,
						"日期時間": dateTime,
					}).Info("找到人名")

					dailyrecordbytsfile := DailyRecordByTsFile{
						cardID,
						date,
						time,
						employeeID,
						//utf8Line[144:153],
						name,
						msg,
						dateTime}

					// 存到channel裡面
					chanDailyRecordByTsFile <- dailyrecordbytsfile

				}
			}
		}
	}

	close(chanDailyRecordByTsFile) // 關閉儲存的channel

	log_info.WithFields(logrus.Fields{}).Info("所有檔案讀取完成，已關閉儲存的channel")

}

/** 取名字 */
func getName(utf8Line string, positionOfName int) string {

	name := ""
	finished := false
	//姓名從positionOfName位置開始判斷

	for i := positionOfName; finished != true; i++ {

		nextOne := utf8Line[i+1 : i+2] //下一個字
		nextTwo := utf8Line[i+2 : i+3] //下下一個字
		// fmt.Println("i=", i, "nextOne=", nextOne, "nextTwo", nextTwo)

		// if strings.Compare(" ", nextOne) == 0 {
		// 	fmt.Print("nextOne是空白")
		// }

		// if strings.Compare(" ", nextTwo) == 0 {
		// 	fmt.Print("nextTwo是空白")
		// }

		// 若下兩個都是空白 表示144~i都是name
		if (strings.Compare(" ", nextOne) == 0) && (strings.Compare(" ", nextTwo) == 0) {

			name = utf8Line[positionOfName : i+1] //(不包含i+1這位置)

			finished = true
		}
	}

	return name
}

/** 組合年 */
func getDateTime(myDate string, myTime string) time.Time {

	// fmt.Println("myDate=", myDate)
	// fmt.Println("myTime=", myTime)

	//2020/11/04
	year, err := strconv.Atoi(myDate[0:4])
	if nil != err {
		fmt.Printf("字串轉換數字錯誤 year=", year)
	}

	month, err := strconv.Atoi(myDate[5:7])
	if nil != err {
		fmt.Printf("字串轉換數字錯誤 month=", month)
	}

	day, err := strconv.Atoi(myDate[8:10])
	if nil != err {
		fmt.Printf("字串轉換數字錯誤 day=", day)
	}

	//14:18:01
	hr, err := strconv.Atoi(myTime[0:2])
	if nil != err {
		fmt.Printf("字串轉換數字錯誤 hr=", hr)
	}

	min, err := strconv.Atoi(myTime[3:5])
	if nil != err {
		fmt.Printf("字串轉換數字錯誤 min=", min)
	}

	sec, err := strconv.Atoi(myTime[6:8])
	if nil != err {
		fmt.Printf("字串轉換數字錯誤 sec=", sec)
	}

	msec, err := strconv.Atoi("0")
	if nil != err {
		fmt.Printf("字串轉換數字錯誤 msec=", msec)
	}

	t := time.Date(year, time.Month(month), day, hr, min, sec, msec, time.Local)
	fmt.Printf("%+v\n", t)
	return t

}

/** 取員工編號共n位數*/
func getEmployeeIDwithNbit(employeeID string, n string) string {

	//取string右邊n位數公式 [ length(id)-n : length(id)]

	myN, err := strconv.Atoi(n)
	if nil != err {
		fmt.Printf("字串轉換數字錯誤 n=", n)
	}

	result := employeeID[len(employeeID)-myN : len(employeeID)]
	return result
}

/** 讀取單檔 TsFile (目前沒用到)*/
func readFile_Ts(fileName string) {

	// 讀檔
	file, err := os.Open(fileName)
	if err != nil {

		log_err.WithFields(logrus.Fields{
			"err":      err,
			"fileName": fileName,
		}).Error("打開檔案失敗")

		log.Fatal(err)
	}
	defer func() {
		if err = file.Close(); err != nil {
			log_err.WithFields(logrus.Fields{
				"err":      err,
				"fileName": fileName,
			}).Error("關閉檔案失敗")

			log.Fatal(err)
		}
	}()

	// 讀檔
	scanner := bufio.NewScanner(file)

	// 一行一行讀
	counter := 0         // 行號
	for scanner.Scan() { // internally, it advances token based on sperator

		// 讀進一行(Big5)
		big5Name := scanner.Text()
		counter++

		//轉成utf8(繁體)
		utf8Line, _, _ := transform.String(big5ToUTF8Decoder, big5Name)

		//fmt.Println(utf8Line[140:145]) //判斷ADMIN
		//fmt.Println(utf8Line[140:141]) //判斷空白
		//fmt.Println(utf8Line[144:153]) //判斷中文名

		// 排除 ADMIN / 排除空白/ 才取名字
		if strings.Compare("ADMIN", utf8Line[140:145]) == 0 {
			fmt.Println("找到ADMIN:", utf8Line[140:145])

			log_info.WithFields(logrus.Fields{
				"fileName": fileName,
				"行號":       counter,
				"值":        utf8Line[140:145],
			}).Info("找到ADMIN")

		} else if strings.Compare(" ", utf8Line[140:141]) == 0 {
			fmt.Println("找到空白:", utf8Line[140:141])

			log_info.WithFields(logrus.Fields{
				"fileName": fileName,
				"行號":       counter,
				"值":        utf8Line[140:141],
			}).Info("找到空白")

		} else {

			fmt.Println("找到人名", utf8Line[15:27], utf8Line[27:37], utf8Line[37:45], utf8Line[139:144], utf8Line[144:153])

			log_info.WithFields(logrus.Fields{
				"fileName": fileName,
				"行號":       counter,
				"卡號":       utf8Line[15:27],
				"日期":       utf8Line[27:37],
				"時間":       utf8Line[37:45],
				"員工編號":     utf8Line[139:144],
				"姓名":       utf8Line[144:153],
			}).Info("找到人名")

		}

	}

}

/** 轉換開始結束日期格式 變成time.Time格式*/
func countDateStartAndEnd() {
	/** 取得開始日期 **/
	stringStartDate := config.StartDate

	// 取出年月日
	startYear, _ := strconv.Atoi(stringStartDate[0:4])
	startMonth, _ := strconv.Atoi(stringStartDate[4:6])
	startDay, _ := strconv.Atoi(stringStartDate[6:8])

	// 轉成time格式
	dateStart = time.Date(startYear, time.Month(startMonth), startDay, 0, 0, 0, 0, time.Local)
	fmt.Println("檔案開始日期:", dateStart)
	log_info.Info("檔案開始日期: ", dateStart)

	/** 取得結束日期 **/
	stringEndDate := config.EndDate

	// 取出年月日
	endYear, _ := strconv.Atoi(stringEndDate[0:4])
	endMonth, _ := strconv.Atoi(stringEndDate[4:6])
	endDay, _ := strconv.Atoi(stringEndDate[6:8])

	// 轉成time格式
	dateEnd = time.Date(endYear, time.Month(endMonth), endDay, 0, 0, 0, 0, time.Local)
	fmt.Println("檔案結束日期:", dateEnd)
	log_info.Info("檔案結束日期: ", dateEnd)
}

/** 將所有日打卡紀錄，全部插入到 mongodb */
func insertDailyRecord_TsFile(chanDailyRecordByTsFile <-chan DailyRecordByTsFile, dones chan<- struct{}) {
	//开启loop个协程

	log_info.Info("開始插入MONGODB")

	session, err := mgo.Dial(config.MongodbServerIP)
	if err != nil {
		fmt.Println("打卡紀錄插入錯誤(連上DB錯誤)")

		log_err.WithFields(logrus.Fields{
			"err": err,
		}).Error("打卡紀錄插入錯誤(連上DB錯誤)")

		panic(err)
	}

	defer session.Close()

	c := session.DB(config.DBName).C(config.Collection)
	log_info.Info("連上DBName:", config.DBName, "Collection", config.Collection)

	//確認資料筆數
	// ch := make(chan int, 100)
	// for i := 0; i < 34; i++ {
	// 	ch <- 0
	// }
	// fmt.Println("資料量:", len(ch))

	log_info.WithFields(logrus.Fields{
		"len(chanDailyRecordByTsFile)": len(chanDailyRecordByTsFile),
	}).Info("確認資料量")

	for dailyrecord := range chanDailyRecordByTsFile {
		// fmt.Println("插入一筆打卡資料：", dailyrecord)
		// log_info.Info("插入一筆打卡資料:", dailyrecord)

		c.Insert(&dailyrecord)
	}

	dones <- struct{}{}
}

func deleteEmptyAndRedundantRecord() {

	log_info.Info("開始刪除多餘紀錄")

	session, err := mgo.Dial(config.MongodbServerIP)
	if err != nil {
		fmt.Println("連上DB錯誤")

		log_err.WithFields(logrus.Fields{
			"err": err,
		}).Error("連上DB錯誤")

		//自己與caller皆中斷
		panic(err)
	}

	defer session.Close()

	// Get collection
	c := session.DB(config.DBName).C(config.Collection)
	log_info.Info("連上DBName:", config.DBName, "Collection", config.Collection)

	// Delete record(不合理的時間與code)
	err = c.Remove(
		bson.M{
			"dateTime": bson.M{
				"$lt": time.Date(0001, 1, 1, 0, 0, 0, 0, time.UTC)}})

	if err != nil {
		fmt.Printf("remove fail %v\n", err)

		log_err.WithFields(logrus.Fields{
			"err": err,
		}).Error("刪除Record錯誤")
		//自己與caller皆中斷
		panic(err)
	}
}

// 等待結束
func awaitForCloseResult(dones <-chan struct{}) {
	for {
		<-dones
		worker--
		if worker <= 0 {
			return
		}
	}
}
