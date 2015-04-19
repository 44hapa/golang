package main

import (
    "fmt"
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
    "log"
)

const totalRow int = 1000000

func main() {
    db, err := sql.Open("mysql", "root:qqqwww@/test")

    reportUser := make(chan string)
    reportSales := make(chan string)

    if (nil != err) {
        log.Fatal(err)
    }

    go populateUser(db, reportUser)
    go populateSales(db, reportSales)

    echoUser := <-reportUser
    echoSales := <-reportSales
    fmt.Println(echoUser)
    fmt.Println(echoSales)

    fmt.Println("All tables populate")
}

func populateUser(db *sql.DB, reportUser chan string) {
    insertQuery := "insert into users (name) values (?)"

    for i := 0; i < totalRow; i++ {
        _, err := db.Exec(insertQuery, "qwe")
        if (nil != err) {
            log.Fatal(err)
        }
    }
    reportUser <- "Populate users complite"
}

func populateSales(db *sql.DB, reportSales chan string) {
    insertQuery := "insert into sales (user_id, order_amount) values (?, ?)"

    for i := 0; i < totalRow; i++ {
        _, err := db.Exec(insertQuery, 10, 19.19)
        if (nil != err) {
            log.Fatal(err)
        }
    }
    reportSales <- "Populate sales complite"
}