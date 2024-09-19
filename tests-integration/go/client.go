package main

import (
  "log"
  "database/sql"
  _"github.com/lib/pq"
)
func main() {
  conninfo := "host=127.0.0.1 port=5432 user=tom password=pencil dbname=localdb"
  db, err := sql.Open("postgres", conninfo)
  if err != nil {
    log.Fatal(err)
  }
  defer db.Close()
  
  _, err = db.Exec("INSERT INTO testtable VALUES (1)")
  if err != nil {
    log.Fatal(err)
  }
  
  rows, err := db.Query("SELECT * FROM testtable")
  if err != nil {
    log.Fatal(err)
  }

  for rows.Next() {
    log.Println(rows)
  }
}
