package Starter

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/viper"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
	"strconv"
	"time"
)

// 存储数据库连接源切片 默认长度和容量都为1
var datasourceList = make(map[string]DbStruct, 1)

/**
 * @Author yNsLuHan
 * @Description:
 */
type DbStruct struct {
	Db   *sqlx.DB
	Gorm *gorm.DB
}

/*
mysql yaml 配置文件 模板

mysql:
  datasource:
    - master:
        host: 127.0.0.1
        port: 3306
        user: root
        password: root123
        database: db
        max-idle: 2
        max-pool-size: 5
		idle-timeout: 1
		max-lifetime: 1
		load-gorm: true		# 是否加载gorm

    - slave:
        host: 47.108.217.109
        port: 3206
        user: ynsluhan
        password: yNsluhan#0817
        database: arc-order
        max-idle: 2
        max-pool-size: 5
		idle-timeout: 1
		max-lifetime: 1
		load-gorm: true		# 是否加载gorm

*/

/**
* @Author: yNsLuHan
* @Description:
* @File: MysqlPool
* @Version: 1.0.0
* @Date: 2021/8/23 12:45 下午
 */
func InitDataSources(config *viper.Viper, name string) {
	// mysql.datasource 开头
	mysqlDatasourceMaps := config.Get(name)
	// 判断是否为空
	if mysqlDatasourceMaps != nil && mysqlDatasourceMaps.(map[string]interface{}) != nil {
		// 获取到连接源的map {master:map[interface{}]interface{}, slave1:map[interface{}]interface{}, slave2:map[interface{}]interface{}, ...}
		maps := mysqlDatasourceMaps.(map[string]interface{})
		// 进行遍历 获取到单个数据源
		// 列master,  因为每个数据的结构是 map[interface{}]interface{} 类型，所有需要遍历获取到 key  和 value
		for datasourceName, m := range maps {
			// 进行数据库连接操作
			//datasourceList = SetDatasource(datasourceList, m.(map[string]interface{}), datasourceName)
			SetDatasource(datasourceList, m.(map[string]interface{}), datasourceName)
		}
	}
}

/**
 * @Author yNsLuHan
 * @Description:
 * @Time 2021-08-23 14:21:11
 * @param datasourceList
 * @param m2
 * @param datasourceName
 * @return map[string]DbStruct
 */
func SetDatasource(datasourceList map[string]DbStruct, m2 map[string]interface{}, datasourceName string) {
	// 连接必要属性
	//log.Println("INFO 正在获取数据库连接参数..")
	host := GetStringMustOption("host", m2).(string)
	port := strconv.Itoa(GetStringMustOption("port", m2).(int))
	user := GetStringMustOption("user", m2).(string)
	password := GetStringMustOption("password", m2).(string)
	database := GetStringMustOption("database", m2).(string)
	url := GetStringMustOption("url", m2).(string)
	// 选择性数据
	// 连接最大空闲数量
	interfaceMaxIdle := GetIntOption("max-idle", m2)
	// 连接池大小
	interfacePoolSize := GetIntOption("max-pool-size", m2)
	// 连接最大空闲时间
	interfaceIdleTimeout := GetIntOption("idle-timeout", m2)
	// 连接最大生存时间
	interfaceMaxLifetime := GetIntOption("max-lifetime", m2)
	// 是否初始化gorm
	loadGorm := GetStringOption("load-gorm", m2)
	//
	//log.Println("INFO 数据库连接参数获取完成..")
	// db
	var db *sqlx.DB
	// 数据源url
	// var vds = "%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=true&loc=Local"
	var vds = "%s:%s@tcp(%s:%s)/%s?%s"
	// 根据env获取host 根据env获取host
	var dataSource = fmt.Sprintf(vds, user, password, host, port, database, url)
	//
	var err error
	//
	//log.Println("INFO 正在连接数据库..")
	// driverName: 驱动
	db, err = sqlx.Connect("mysql", dataSource)
	//
	if err != nil {
		log.Fatal("ERROR 数据master库连接失败： ", err)
	}
	err = db.Ping()
	if err != nil {
		log.Fatal("ERROR 数据master库连接失败1： ", err)
	}
	// 设置最大空闲连接数
	if interfaceMaxIdle != nil {
		db.SetMaxIdleConns(interfaceMaxIdle.(int))
	}
	// 设置连接池大小
	if interfacePoolSize != nil {
		db.SetMaxOpenConns(interfacePoolSize.(int))
	}
	// 设置连接最大空闲时间
	if interfaceIdleTimeout != nil {
		db.SetConnMaxIdleTime(time.Duration(interfaceIdleTimeout.(int)))
	}
	// 设置连接最大生存时间
	if interfaceIdleTimeout != nil {
		db.SetConnMaxLifetime(time.Duration(interfaceMaxLifetime.(int)))
	}
	log.Printf("INFO MySQL connection %s successful：%s:%s/%s \n", datasourceName, host, port, database)
	// 进行gorm连接创建
	if loadGorm != nil && loadGorm.(bool) {
		initGormDb := InitGormDb(db, datasourceName)
		datasourceList[datasourceName] = DbStruct{Db: db, Gorm: initGormDb}
	} else {
		datasourceList[datasourceName] = DbStruct{Db: db}
	}
}

/**
 * @Author yNsLuHan
 * @Description:
 * @Time 2021-08-23 14:42:57
 * @param db
 * @param gormDb
 * @param datasourceName
 */
func InitGormDb(db *sqlx.DB, datasourceName string) *gorm.DB {
	//
	var err error
	// 创建gorm, 使用现有连接
	gormDb, err := gorm.Open(mysql.New(mysql.Config{Conn: db}), &gorm.Config{})
	//
	if err != nil {
		log.Fatal("ERROR gorm", datasourceName, "create fail:", err)
	}
	log.Println("INFO Mysql Gorm", datasourceName, "init success.")
	//
	return gormDb
}

/**
 * @Author yNsLuHan
 * @Description:
 * @Time 2021-08-23 14:18:20
 * @return map[string]DbStruct
 */
func GetDataSource() map[string]DbStruct {
	return datasourceList
}

/**
 * @Author yNsLuHan
 * @Description:  返回实体类
 */
type Res struct {
	AlterRow int
	Error    error
}

/**
 * @Author yNsLuHan
 * @Description: 公共方法： 预处理，插入多条数据
 * @Time 2021-08-23 17:15:46
 * @param db
 * @param sql
 * @param args
 * @return Res
 */
func PrepareMany(db *sqlx.DB, sql string, args []interface{}) Res {
	prepare, err := db.Prepare(sql)
	//
	if err != nil {
		log.Println("ERROR sql failed:", err)
		return Res{Error: err}
	}
	exec, err := prepare.Exec(args...)
	//
	if err != nil {
		log.Println("ERROR select failed:", err)
		return Res{Error: err}
	}
	// 获取id
	id, err := exec.LastInsertId()
	//
	if err != nil {
		log.Println("ERROR GetId failed:", err)
		return Res{Error: err}
	}
	return Res{AlterRow: int(id), Error: nil}
}

/**
 * @Author yNsLuHan
 * @Description: 公共方法：获取一个数据
 * @Time 2021-08-23 17:19:29
 * @param db
 * @param sql
 * @param o
 * @param args
 * @return Res
 */
func GetOne(db *sqlx.DB, sql string, o interface{}, args ...interface{}) Res {
	if args != nil {
		err := db.Get(o, sql, args...)
		if err != nil {
			log.Println("ERROR sql failed:", err)
			return Res{Error: err}
		}
		return Res{}
	} else {
		err := db.Get(o, sql)
		if err != nil {
			log.Println("ERROR select failed:", err)
			return Res{Error: err}
		}
		return Res{}
	}
}

/**
 * @Author yNsLuHan
 * @Description: 公共方法：获取一个或多个对象
 * @Time 2021-08-23 17:19:48
 * @param db
 * @param sql
 * @param o
 * @param args
 */
func GetStruct(db *sqlx.DB, sql string, o interface{}, args ...interface{}) Res {
	if args != nil {
		err := db.Select(o, sql, args...)
		if err != nil {
			log.Println("ERROR sql failed:", err)
			return Res{Error: err}
		}
	} else {
		err := db.Select(o, sql)
		if err != nil {
			log.Println("ERROR select failed:", err)
			return Res{Error: err}
		}
	}
	return Res{}
}

/**
 * @Author yNsLuHan
 * @Description: 公共方法：插入数据
 * @Time 2021-08-23 17:23:11
 * @param db
 * @param sql
 * @param args
 * @return Res
 */
func InsertStruct(db *sqlx.DB, sql string, args ...interface{}) Res {
	exec, err := db.Exec(sql, args...)
	if err != nil {
		log.Println("ERROR insert failed:", err)
		return Res{Error: err}
	}
	id, err := exec.LastInsertId()
	if err != nil {
		log.Println("ERROR Get id failed:", err)
		return Res{Error: err}
	}
	return Res{AlterRow: int(id)}
}

/**
 * @Author yNsLuHan
 * @Description: 公共方法： 修改
 * @Time 2021-06-08 15:14:19
 * @param db
 * @param sql
 * @param args
 * @return interface{}
 */
func UpdateStruct(db *sqlx.DB, sql string, args ...interface{}) Res {
	exec, err := db.Exec(sql, args...)
	if err != nil {
		log.Println("ERROR update failed:", err)
		return Res{Error: err}
	}
	affected, err := exec.RowsAffected()
	if err != nil {
		log.Println("ERROR get Affected failed:", err)
		return Res{Error: err}
	}
	// 返回影响行数
	return Res{AlterRow: int(affected)}
}

/**
 * @Author yNsLuHan
 * @Description: 公共方法： 删除
 * @Time 2021-08-23 17:25:22
 * @param db
 * @param sql
 * @param args
 * @return Res
 */
func DeleteStruct(db *sqlx.DB, sql string, args ...interface{}) Res {
	exec, err := db.Exec(sql, args...)
	if err != nil {
		log.Println("ERROR update failed:", err)
		return Res{Error: err}
	}
	affected, err := exec.RowsAffected()
	if err != nil {
		log.Println("ERROR get Affected failed:", err)
		return Res{Error: err}
	}
	// 返回影响行数
	return Res{AlterRow: int(affected)}
}

/**
 * @Author yNsLuHan
 * @Description:
 * @Time 2021-08-23 12:12:08
 * @param optionName
 * @param data
 * @return string
 */
func GetStringMustOption(optionName string, data map[string]interface{}) interface{} {
	h := data[optionName]

	if h == nil {
		log.Fatal("ERROR 数据库：", optionName, " 字段为空")
	}
	return h.(interface{})
}

/**
 * @Author yNsLuHan
 * @Description:
 * @Time 2021-08-23 12:12:08
 * @param optionName
 * @param data
 * @return string
 */
func GetStringOption(optionName string, data map[string]interface{}) interface{} {
	h := data[optionName]

	if h == nil {
		//log.Println("数据库：", optionName, "为空")
		return nil
	}

	return h.(interface{})
}

/**
 * @Author yNsLuHan
 * @Description:
 * @Time 2021-08-23 12:12:08
 * @param optionName
 * @param data
 * @return string
 */
func GetIntOption(optionName string, data map[string]interface{}) interface{} {
	h := data[optionName]

	if h == nil {
		//log.Println("数据库：", optionName, "为空")
		return nil
	}

	return h.(interface{})
}

/**
 * @Author yNsLuHan
 * @Description:
 * @Time 2021-08-23 12:12:08
 * @param optionName
 * @param data
 * @return string
 */
func GetIntMustOption(optionName string, data map[string]interface{}) interface{} {
	h := data[optionName]

	if h == nil {
		log.Fatal("数据库：", optionName, "为空")
	}

	return h.(interface{})
}
