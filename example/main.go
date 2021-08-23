package main

import (
	Starter "github.com/ynsluhan/mysql-datasource-starter"
	"github.com/spf13/viper"
	"log"
	"os"
	"path"
)

type User struct {
	Id   int
	Name string
	Age  int
}

/**
* @Author: yNsLuHan
* @Description:
* @File: main
* @Version: 1.0.0
* @Date: 2021/8/23 3:09 下午
 */
func main() {
	var basePath, err = os.Getwd()
	//
	var configPath = path.Join(basePath, "example")
	//
	if err != nil {
		log.Fatal("ERROR", err)
	}
	//
	config := viper.New()
	config.SetConfigType("yaml")
	config.SetConfigName("application")
	config.AddConfigPath(configPath)
	// 读取配置
	err = config.ReadInConfig()
	//
	if err != nil {
		log.Fatal(err)
	}
	// 初始化MySQL数据库数据源
	Starter.InitDataSources(config, "mysql.datasource")
	// 获取数据源map
	source := Starter.GetDataSource()
	// 获取某个数据源 db
	db := source["master"].Db
	//
	var data []User
	// 查询
	Starter.GetStruct(db, "select * from user", &data)
	//
	log.Println(data)

}
