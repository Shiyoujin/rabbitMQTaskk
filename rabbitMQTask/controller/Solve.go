package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql-master"
	"github.com/streadway/amqp"
	"log"
)

type Order struct {
	UserId string
	GoodId string
	ShopId string
	Number int
	Time   string
}

func orderHistory(userId string, goodId string, shopId string, number int, time string) {

	db, err := sql.Open("mysql", "root:@tcp(localhost:3306)/gotest?charset=utf8")
	check(err)

	stmt, err := db.Prepare(`INSERT into orderhistroy (userId,goodId,shopId,num,now) VALUES (?, ?, ?, ?, ?)`)
	check(err)

	res, err := stmt.Exec(userId, goodId, shopId, number, time)
	check(err)

	id, err := res.LastInsertId()
	check(err)

	fmt.Println(id)
	stmt.Close()
}

func queryShopNum() int {

	var shopNumber int

	db, err := sql.Open("mysql", "root:@tcp(localhost:3306)/gotest?charset=utf8")
	check(err)

	//"SELECT id,imgUrl,createDate,state FROM antable
	rows, err := db.Query("SELECT shopNumber FROM shopreserve")
	check(err)

	for rows.Next() {

		//注意这里的Scan括号中的参数顺序，和 SELECT 的字段顺序要保持一致。
		if err := rows.Scan(&shopNumber); err != nil {
			log.Fatal(err)
		}
		/*
			每一次循环的时候，我们都可以按照我们想要的顺序，取得所有的字段值。唯一需要注意的是，rows.Scan的参数顺序，
			需要和select语句的字段保持顺序一致。这里主要指的是数据类型。参数名可以不同。
		*/
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	rows.Close()
	return shopNumber
}

func update(shopNumber int, shopId string, goodId string) {
	db, err := sql.Open("mysql", "root:@tcp(localhost:3306)/gotest?charset=utf8")
	check(err)

	stmt, err := db.Prepare("UPDATE shopreserve set shopNumber=? WHERE shopId = ? and goodId = ?")
	check(err)

	res, err := stmt.Exec(shopNumber, shopId, goodId)
	check(err)

	num, err := res.RowsAffected()
	check(err)

	fmt.Println(num)
	stmt.Close()
}

func main() {

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	handleError(err, "Can't connect a amqpChannel")
	defer conn.Close()

	amqpChannel, err := conn.Channel()
	handleError(err, "Can't create a amqpChannel")
	defer amqpChannel.Close()

	//创建队列
	queue, err := amqpChannel.QueueDeclare("order", true, false, false, false, nil)
	handleError(err, "Could not declare `add` queue")

	//注意：这个在推送模式下非常重要，通过设置Qos用来防止消息堆积。
	err = amqpChannel.Qos(1, 0, false)
	handleError(err, "could not configure QoS")

	/*
		接受消息-推模式
		RMQ Server主动把消息推给消费者
	*/
	messageChannel, err := amqpChannel.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil)

	handleError(err, "Could not register consumer")

	stopChan := make(chan bool)

	go func() {
		log.Printf("接受订单开始处理")
		for d := range messageChannel {
			log.Printf("Received a order json: %s", string(d.Body))
			str := string(d.Body)
			good := &Order{}
			err := json.Unmarshal(d.Body, good)
			if err != nil {
				log.Printf("Error decoding JSON: %s", err)
			}

			fmt.Println("str", str)
			//这里就可以处理。json 解析到结构体
			//https://www.cnblogs.com/ycyoes/p/5398796.html
			type Orderhistory struct {
				UserId string `json:"user-id"`
				GoodId string `json:"good-id"`
				ShopId string `json:"shop-id"`
				Number int
				Time   string
			}

			var o Orderhistory

			json.Unmarshal([]byte(str), &o)

			fmt.Println(o.UserId, o.GoodId, o.ShopId,
				o.Number, o.Time)
			orderhistory := Orderhistory{UserId: o.UserId, GoodId: o.GoodId, ShopId: o.ShopId, Number: o.Number, Time: o.Time}

			//插入订单数据库中
			log.Printf("执行订单记录")
			fmt.Println(orderhistory)
			orderHistory(orderhistory.UserId, orderhistory.GoodId, orderhistory.ShopId, orderhistory.Number, orderhistory.Time)

			shopNumberBefor := queryShopNum()
			shopNumberAfter := shopNumberBefor - orderhistory.Number
			log.Println("修改商铺库存表")
			update(shopNumberAfter, orderhistory.ShopId, orderhistory.GoodId)

			//手动回复消息
			//multiple参数。true表示回复当前信道所有未回复的ack，用于批量确认。false表示回复当前条目。
			if err := d.Ack(false); err != nil {
				log.Printf("Error acknowledging message : %s", err)
			} else {
				//进行订单的处理. DB
			}
		}
	}()
	//终止当前进程
	<-stopChan
}

func handleError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)

	}
}

func check(err error) {
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
}
