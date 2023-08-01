package main

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
	"encoding/csv"
	"strconv"
)

func main() {
  os.Remove("database.sqlite3")

	db, err := sql.Open("sqlite3", "database.sqlite3")
	if err != nil {
		log.Fatal("Failed to open database: ", err)
		os.Exit(1)
	}
	defer db.Close()

	sqlStmt := `
		CREATE TABLE prefectures(
			id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			capital TEXT NOT NULL,
			population INTEGER NOT NULL,
			area REAL NOT NULL,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		log.Fatal("Failed to create table: ", err)
		os.Exit(1)
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatal("Failed to begin transaction: ", err)
		os.Exit(1)
	}
	stmt, err := tx.Prepare(`
		INSERT INTO prefectures(name, capital, population, area)
		VALUES(?, ?, ?, ?)
	`)
	if err != nil {
		log.Fatal("Failed to prepare statement: ", err)
		os.Exit(1)
	}
	defer stmt.Close()

  file, err := os.Open("prefectures.csv")
  if err != nil {
    log.Fatal("Failed to open csv: ", err)
		os.Exit(1)
  }
  defer file.Close()

  r := csv.NewReader(file)
  rows, err := r.ReadAll()
  if err != nil {
    log.Fatal(err)
  }
  for i, v := range rows[1:] {
		if len(v) < 5 {
			log.Fatal("Invalid record: ", i, v)
			continue
		}
		name := v[1]
		capital := v[2]
		population, err := strconv.Atoi(v[3])
		if err != nil {
			log.Fatal("Failed to convert population: ", err)
			continue
		}
		area, err := strconv.ParseFloat(v[4], 64)
		if err != nil {
			log.Fatal("Failed to convert area: ", err)
			continue
		}
		_, err = stmt.Exec(name, capital, population, area)
		if err != nil {
			log.Fatal("Failed to insert record: ", err)
			continue
		}
  }

	err = tx.Commit()
	if err != nil {
		log.Fatal("Failed to commit transaction: ", err)
	}

	records, err := db.Query("SELECT id, name, capital, population, area, population / area AS density from prefectures")
	if err != nil {
		log.Fatal(err)
	}
	defer records.Close()
	for records.Next() {
		var id int
		var name string
		var capital string
		var population int
		var area float64
		var density float64
		err = records.Scan(&id, &name, &capital, &population, &area, &density)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("id:", id, "name:", name, "capital:", capital, "population:", population, "area:", area, "density:", density)
	}
	err = records.Err()
	if err != nil {
		log.Fatal(err)
	}
}
