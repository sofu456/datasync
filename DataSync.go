package main

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/mgo.v2"

	"github.com/getlantern/systray"
	_ "github.com/go-sql-driver/mysql"
	"go.mongodb.org/mongo-driver/bson"
)

var stop_flag bool
var session *mgo.Session
var mysqldb *sql.DB

func main() {
	systray.Run(func() {
		stop_flag = false
		ico, _ := os.ReadFile("name.ico")
		systray.SetTitle("File Sync")
		systray.SetIcon(ico)
		systray.SetTooltip("File Sync")
		start := systray.AddMenuItem("Start", "Start Sync")
		start.Disable()
		stop := systray.AddMenuItem("Stop", "Stop Sync")
		systray.AddSeparator()
		quit := systray.AddMenuItem("Quit", "Quit the application")

		//config
		json_data, _ := os.ReadFile("DataSynConfig.json")
		json_obj := make([]map[string]interface{}, 0)
		if err := json.Unmarshal(json_data, &json_obj); err != nil {
			log.Fatal(err)
		}
		//mysql
		var err error
		mysqldb, err = sql.Open("mysql", "root:123@tcp(127.0.0.1)/qar?charset=utf8")
		if err != nil {
			log.Fatal(err)
		}
		//mongdb
		session, err = mgo.Dial("mongodb://root:root@127.0.0.1:27017")
		if err != nil {
			log.Fatal(err)
			return
		}
		names, err := session.DatabaseNames()
		if err != nil {
			log.Print(err, names)
		}
		go Run(json_obj)
		go func() {
			for {
				select {
				case <-start.ClickedCh:
					go Run(json_obj)
					stop.Enable()
					start.Disable()
				case <-stop.ClickedCh:
					stop_flag = true
					start.Enable()
					stop.Disable()
				case <-quit.ClickedCh:
					stop_flag = true
					mysqldb.Close()
					session.Close()
					systray.Quit()
				}
			}
		}()
	}, nil)
}

func Run(json_obj []map[string]interface{}) {
	stop_flag = false
	for _, obj := range json_obj {
		go func() {
			for !stop_flag {
				bucket := obj["Instance"].(string) + "-" + obj["TargetDirectory"].(string)
				gridfs := session.DB("qar").GridFS(bucket)
				dirpath := obj["SourceDirectory"].(string)
				filepath.Walk(dirpath, func(path string, info os.FileInfo, err error) error {
					if err != nil || info.IsDir() {
						return err
					}

					filename := strings.Replace(path[len(dirpath)+1:], "\\", "_", -1)
					iter := gridfs.Find(bson.M{"filename": filename})
					if n, _ := iter.Count(); n == 0 {
						data, _ := os.Open(path)
						file, err := gridfs.Create(filename)
						if err != nil {
							log.Print(err)
							return nil
						}
						buf := make([]byte, 1024)
						for {
							n, err := data.Read(buf)
							file.Write(buf[0:n])
							if err != nil || err == io.EOF {
								if err != io.EOF {
									log.Print(err)
								}
								break
							}
						}
						file.Close()
						data.Close()
					}
					var row_name, row_path string
					err = mysqldb.QueryRow("select name,path from data_file where name=? and path=?", filepath.Base(filename), bucket).Scan(&row_name, &row_path)
					if err != nil {
						mysqldb.Exec("insert into data_file(name,instance_id,path,starttime,endtime) values(?,?,?,?,?)", filepath.Base(filename), 1, bucket, info.ModTime(), info.ModTime())
					}
					return nil
				})
			}
		}()
	}
}
