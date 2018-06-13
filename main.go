package main

import (
	"compress/bzip2"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const serverPort string = ":8080"

const fileURL string = "https://guvm.mvd.ru/upload/expired-passports/list_of_expired_passports.csv.bz2"

//const fileURL string = "http://data.loc/list_of_expired_passports.csv.bz2" //адрес на локальном сервере чтоб с инета долго не тянуть
const localFileName string = "list_of_expired_passports.csv.bz2" //имя локального файла куда будет идти закачка
const csvFileName string = "list_of_expired_passports.csv"       //имя разжатого файла
const tryCount int = 3                                           //колво попыток в процессе обновления информации до полного отбоя
const logFileName = "passport.log"
const (
	dbHost          = "tcp(localhost:3306)"
	dbName          = "passport_check"
	dbTable         = "passports"
	dbUser          = "root"
	dbPassword      = ""
	dbInsertBufSize = 100 //размер порции данных на вставку в базу
)

var tryTimeOffset int           //смещение в часах до следующей попытки обновления данных если предыдущая провалилась
var lastUpdateTime = time.Now() //время последнего результативно отработавшего апдейта данных в базе
var tmpl *template.Template

var cmdline chan string //канал для передачи команд в горутину шедулера

var updateIsWorking bool = false //маркер определяющий работает ли уже процесс апдейта
/*
загружает файл из сети
возвращает ошибку при хагрузке если возникла(например нет связи)
*/
func loadFileFromInet() (err error) {

	var f *os.File
	//создаём запрос
	req := &http.Request{
		Method: http.MethodGet,
	}

	req.URL, _ = url.Parse(fileURL)
	//выполняем запрос
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	//просим по завершении функции закрыть тело результата запроса
	defer resp.Body.Close()
	//открываем локальный файл
	f, err = os.OpenFile(localFileName, os.O_CREATE|os.O_WRONLY, 0600)

	if err != nil {
		return err
	}
	defer f.Close()
	//копируем данные из тела ответа в файл
	_, err = io.Copy(f, resp.Body)
	if err != nil {
		return err
	}
	fmt.Println("скачали")
	return nil
}

/*
функция разжимает скачанный архив
возвращает ошибку(например переполнение диска)
*/
func decompress() (err error) {
	var src *os.File
	var dst *os.File
	err = nil
	src, err = os.OpenFile(localFileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err = os.OpenFile(csvFileName, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer dst.Close()
	//натравливаем декомпрессор на файл архива получая ридер откуда далее читаем разжатые данные
	decompressor := bzip2.NewReader(src)
	_, err = io.Copy(dst, decompressor)
	fmt.Println("разжали")
	return err
}

/*
функция пишет блок данных в базу
принимает указатель на соединение к базе, массив строк с данными на запись и кол-во записей которое в этом массиве актуально
возвращает кол-во фактически внесенных записей и ошибку
*/
func writeDataToDB(conn *sql.DB, data [dbInsertBufSize]string, valueCount int) (queryCount int64, err error) {
	var i int
	err = nil
	var result sql.Result
	if valueCount > dbInsertBufSize {
		valueCount = dbInsertBufSize
	}
	sqlText := "insert into " + dbTable + "(number) values "
	//формируем текст запроса к базе
	//цикл обрываем за один шаг до окончания чтобы последний элемент вставить без запятой в конце
	for i = 0; i < valueCount-1; i++ {

		sqlText += "('" + data[i] + "'),"
	}
	sqlText += "('" + data[i] + "')"
	//выполняем запрос
	result, err = conn.Exec(sqlText)
	if nil != err {
		crush(err)
		return
	}
	//получаем кол-во фактически внесенных значений в базу
	queryCount, err = result.RowsAffected()
	if nil != err {
		crush(err)
		return
	}
	return
}

/*
парсит csv файл и нажрав определенное кол-во записей в массив отправляет их в БД
возвращает ошибку
*/
func parseCsvAndWriteDB() (err error) {
	var dbConn *sql.DB                     //указатель на соединение с БД
	var parsedData [dbInsertBufSize]string //массив буфер куда нажираем записи парсером
	var csvSrc *os.File
	var record []string
	var i int
	var recordsSended int64
	var recordsAffected int64
	var n int64
	fmt.Println("начали писать в базу")
	dbConn, err = sql.Open("mysql", dbUser+":"+dbPassword+"@"+dbHost+"/"+dbName+"?charset=utf8")
	if err != nil {
		crush(err)
		return err
	}
	defer dbConn.Close()
	//очищаем таблицу от старых значений
	_, err = dbConn.Query("TRUNCATE TABLE " + dbTable + ";")
	if err != nil {
		crush(err)
		return err
	}

	csvSrc, err = os.OpenFile(csvFileName, os.O_RDONLY, 0600)
	if err != nil {
		crush(err)
		return err
	}
	defer csvSrc.Close()
	//получаем ридер парсера откуда уже будем читать массивы строк со значениями из таблицы csv
	csvParser := csv.NewReader(csvSrc)
	record, err = csvParser.Read() //читаем первую запись с заголовками столбцов чтобы не вставить их в базу
	//пока не достигли конца файла
	for err != io.EOF {
		//заполняем буферный массив данными
		for i = 0; i < dbInsertBufSize; i++ {
			record, err = csvParser.Read()

			if err != nil {
				if err == io.EOF {
					break
				} else {
					crush(err)
					return err
				}

			}
			parsedData[i] = record[0] + record[1]

		}
		recordsSended += (int64)(i)
		//отправляем буфер на запись в базу
		n, _ = writeDataToDB(dbConn, parsedData, i)
		recordsAffected += n

	}
	fmt.Println("обновление закончено. записей отправлено в базу " + fmt.Sprintf("%d", recordsSended) +
		" записей внесено " + fmt.Sprintf("%d", recordsAffected))
	return nil
}

//вызывается в случае провала процесса обновления данных. корректирует время для следующего обновления и логирует проблему
//принимает ошибку которую собсно логирует
func crush(err error) {
	//логируем в файл
	log.Println(time.Now().String() + err.Error())
	//логируем в консоль
	fmt.Print(time.Now().String() + err.Error())
	//увеличиваем смещение времени для расписания обновления
	if tryTimeOffset == 24 {
		tryTimeOffset = 0
	} else {
		tryTimeOffset++
	}

}

/*
функция содержит в себе вызовы всех шагов обновления БД
*/
func updateProcess() {
	var i int
	var err error
	var mutex sync.Mutex //мютекс для блокировки изменения переменных
	//включаем блокировку мютекса
	mutex.Lock()
	//меняем маркер
	updateIsWorking = true
	//разблокируем мютекс
	mutex.Unlock()
	//цикл для совершения нескольких попыток выполнения шага
	for i = 0; i <= tryCount; i++ {
		e := loadFileFromInet()
		if nil == e {
			break
		}
	}
	//если таки все попытки провалились тогда уже выходим из процесса обновления
	if i > tryCount {
		err = errors.New("internet problem")
		crush(err)
		return
	}
	for i = 0; i <= tryCount; i++ {
		e := decompress()
		if nil == e {
			break
		}
	}
	if i > tryCount {
		err = errors.New("decompress problem")
		crush(err)
		return
	}
	err = parseCsvAndWriteDB()
	if err != nil {
		crush(err)
		return
	}
	mutex.Lock()
	//обновляем отметку времени последнего обновления базы
	lastUpdateTime = time.Now()
	updateIsWorking = false
	mutex.Unlock()
}

/*
горутина планировщик. занимается запуском обновления БД по расписанию и выполнением команд от юзера
принимает канал по которому приходят команды
*/
func sheduler(cmdln <-chan string) {

	for true {
		//передаём управление шедулеру рантайма чтобы и другие горутины могли поработать
		runtime.Gosched()
		//спим одну секунду
		time.Sleep(time.Second)
		//смотрим если ли чтото в канале команд
		//если есть то выполняем команду
		select {
		case cmd := <-cmdln:
			{
				switch cmd {
				case "update":
					{
						if true != updateIsWorking {
							fmt.Println("процесс обновления запущен по команде")
							updateProcess()
						}
					}

				}
			}
		default:
			{
				//это чтоб не блочилась горутина если в канале команд пусто
			}
		}
		//если подошло время обновления то запускаем его
		if (time.Now().Hour() == (0 + tryTimeOffset)) && (lastUpdateTime.Day() < time.Now().Day()) {

			fmt.Println("процесс обновления запущен по расписанию")
			updateProcess()
			tryTimeOffset = 0 //если все прошло норм то сбрасываем значение смещения

		}
	}

}

/*
производит поиск в базе
принимает на вход слайс строк с номерами на проверку
отдает слайс строк с номерами которые были найдены в базе
*/
func checkNumbersInDB(data []string) (result []string) {
	var dbConn *sql.DB
	var err error
	var rows *sql.Rows
	result = make([]string, len(data), len(data))
	var sqlText = "select number from " + dbTable + " where "
	var i int
	for i = 0; i <= len(data)-2; i++ {
		sqlText += " number = '" + data[i] + "' or "
	}
	sqlText += " number = '" + data[i] + "'"
	dbConn, err = sql.Open("mysql", dbUser+":"+dbPassword+"@"+dbHost+"/"+dbName+"?charset=utf8")
	if err != nil {
		crush(err)
	}
	defer dbConn.Close()
	rows, err = dbConn.Query(sqlText)
	if err != nil {
		crush(err)
	}
	i = 0
	for rows.Next() {
		var number string
		err = rows.Scan(&number)
		if err != nil {
			crush(err)
		}
		result[i] = number
		i++
	}
	return result
}

/*
обработчик маршрута на корень сайта
показывает шаблон с формой для ввода номеров на проверку
также если запущен процесс обновления базы показывает уведомление об этом
*/
func rootHandler(w http.ResponseWriter, req *http.Request) {
	//структура передаваемая в шаблон
	type sr struct {
		CheckNumbersCount string
		WrongNumbers      []string
	}
	var searchResult sr
	//номера для проверки полученные от клиента
	var numbersForCheck []string
	//если запущена функция апдейта базы то сообщаем и выходим
	if true == updateIsWorking {
		maintance(w)
		return
	}

	//тут преобразуем строку с номерами в слайс и передаём его функции которая проверит номера по базе
	if req.Method == "POST" {
		//если отправлен запрос на апдейт базы
		params := req.PostFormValue("update")
		if params != "" {
			//отправляем в командный канал шедулера команду на апдейт
			cmdline <- "update"
			maintance(w)
			return
		}
		//берем из пост параметра список номеров на проверку
		params = req.PostFormValue("numbers")
		//делаем из списка номеров массив разделив строку по символу переноса строки
		numbersForCheck = strings.Split(params, "\n")
		searchResult.CheckNumbersCount = fmt.Sprintf("%d", len(numbersForCheck))
		searchResult.WrongNumbers = make([]string, 0, len(numbersForCheck))
		searchResult.WrongNumbers = checkNumbersInDB(numbersForCheck)
	}
	//отвечаем пользователю шаблоном с формой и результатом поиска если он есть
	err := tmpl.ExecuteTemplate(w, "index.html", searchResult)
	if nil != err {
		crush(err)
	}

}

//пишет в поток ответа браузеру клиента что сервер в режиме обслуживания
func maintance(w http.ResponseWriter) {
	io.WriteString(w, "sorry, but service in maintence mode")
}

func main() {
	//открываем файл лога
	file, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if nil != err {
		defer file.Close()
	}
	log.SetOutput(file)
	//компилируем шаблон для вывода в браузер клиента
	tmpl = template.Must(template.ParseFiles("index.html"))
	//создаём канал для отправки команд горутине шедулера
	cmdline = make(chan string, 1)
	//запуск горутины шедулера
	go sheduler(cmdline)
	//вешаем обработчик маршрута на хттп сервер
	http.HandleFunc("/", rootHandler)
	//запуск хттп сервера
	http.ListenAndServe(serverPort, nil)
}
