package main

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"encoding/csv"
	"os"
	"strconv"
	"archive/zip"
	"io/ioutil"
)

const USERS_CSV = "users.csv"
const SALES_CSV = "sales.csv"

const DESTINATION_DIR = "./archive"

type User struct {
	User_Id int
	Name string
}

type Order struct {
	Order_Id int
	User_Id int
	Order_Amount float64
}

type UserChan struct {
	User User
	endProcess int
}

type OrderChan struct {
	Order Order
	endProcess int
}

func main() {
	db1, err := sqlx.Connect("mysql", "root:qqqwww@/test")

	if err != nil {
		fmt.Println(err)
		return
	}

	db2, err := sqlx.Connect("mysql", "root:qqqwww@/test1")

	if err != nil {
		fmt.Println(err)
		return
	}

	userChan := make(chan UserChan)
	orderChan := make(chan OrderChan)
	countUsersChan := make(chan int)
	countOrdersChan := make(chan int)

	go getUser(db1, userChan)
	go getUser(db2, userChan)

	go getOrder(db1, orderChan)
	go getOrder(db2, orderChan)

	go writeUser(userChan, countUsersChan)
	go writeOrder(orderChan, countOrdersChan)

	userResult := <- countUsersChan
	orderResult := <- countOrdersChan
	fmt.Printf("Всего users %d \n", userResult)
	fmt.Printf("Всего sales %d \n", orderResult)

	err = os.Mkdir(DESTINATION_DIR, 0744)

	if err != nil && os.IsExist(err) != true {
		fmt.Println(err)
		return
	}

	compressor(USERS_CSV)
	compressor(SALES_CSV)
}

func getUser(db *sqlx.DB, userChan chan UserChan) {
	rows, err := db.Queryx("SELECT user_id, name FROM `users`")
	defer rows.Close()

	if err != nil {
		fmt.Println(err)
		return
	}

	var userResult UserChan

	for rows.Next() {
		if err := rows.StructScan(&userResult.User); err != nil {
			fmt.Println(err)
		} else {
			userChan <- userResult
		}
	}

	userResult.endProcess = 1

	userChan <-userResult
}
func getOrder(db *sqlx.DB, orderChan chan OrderChan) {
	rows, err := db.Queryx("SELECT order_id, user_id, order_amount FROM `sales`")
	defer rows.Close()

	if err != nil {
		fmt.Println(err)
		return
	}

	var orderResult OrderChan

	for rows.Next() {
		if err := rows.StructScan(&orderResult.Order); err != nil {
			fmt.Println(err)
		} else {
			orderChan <- orderResult
		}
	}

	orderResult.endProcess = 1

	orderChan <-orderResult
}

func writeUser(userChan chan UserChan, countUsersChan chan int) {
	var countUsers int
	var countProcess int
	var userResult UserChan

	csvFile, err := os.Create(USERS_CSV)

	if err != nil {
		fmt.Println(err)
		return
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)

	for {
		if countProcess >= 2 {
			break
		}

		userResult = <- userChan

		if userResult.endProcess == 0 {
			record := []string{strconv.Itoa(userResult.User.User_Id), userResult.User.Name}
			err := writer.Write(record)
			if err != nil {
				fmt.Println(err)
			} else {
				countUsers++
			}
		} else {
			countProcess += userResult.endProcess
		}
	}

	writer.Flush()
	countUsersChan <- countUsers
}

func writeOrder(orderChan chan OrderChan, countOrdersChan chan int) {
	var countOrders int
	var countProcess int
	var orderResult OrderChan

	csvFile, err := os.Create(SALES_CSV)

	if err != nil {
		fmt.Println(err)
		return
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)

	for {
		if countProcess >= 2 {
			break
		}

		orderResult = <- orderChan

		if orderResult.endProcess == 0 {
			record := []string{
				strconv.Itoa(orderResult.Order.Order_Id),
				strconv.Itoa(orderResult.Order.User_Id),
				strconv.FormatFloat(orderResult.Order.Order_Amount, 'f', 2, 64),
		}
			err := writer.Write(record)
			if err != nil {
				fmt.Println(err)
			} else {
				countOrders++
			}
		} else {
			countProcess += orderResult.endProcess
		}
	}

	writer.Flush()
	countOrdersChan <- countOrders
}

func compressor(fileSource string) {
	archive, _ := os.Create(DESTINATION_DIR + "/" + fileSource + ".zip")
	w := zip.NewWriter(archive)

	byteFile, err := ioutil.ReadFile(fileSource)
	if err != nil {
		fmt.Println(err)
		return
	}

	f, err := w.Create(fileSource)
	if err != nil {
		fmt.Println(err)
		return
	}

	_, err = f.Write(byteFile)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = w.Close()
	if err != nil {
		fmt.Println(err)
		return
	}

	err = os.Remove(fileSource)

	if err != nil {
		fmt.Println(err)
		return
	}
}
