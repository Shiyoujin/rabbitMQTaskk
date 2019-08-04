package main

import (
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql-master"
	"github.com/streadway/amqp"
	"log"
	"time"
)

/*
  参数绑定
  gin框架 model bind
  hold住更多类型参数的处理
*/
type Order struct {
	UserId string `user-id:"user-id" json:"user-id" binding:"required"`
	GoodId string `form:"good-id" json:"good-id" binding:"required"`
	ShopId string `form:"shop-id" json:"shop-id"`
	Number int    `form:"number" json:"number"`
	Time   string
}

func failError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {

	r := gin.Default()
	r.POST("/order", func(context *gin.Context) {

		var order Order
		err := context.Bind(&order)

		fmt.Println(order.UserId)
		//获取当前时间格式化 string
		currentTime := time.Now().Format("2006-01-02 15:04:05")

		order.Time = currentTime

		if err != nil {
			fmt.Println(err)
			log.Fatal(err)
		}

		//创建发布者
		conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
		failError(err, "Can't connect to MQ")
		defer conn.Close()

		amqpChannel, err := conn.Channel()
		failError(err, "Can't create a Channel")

		defer amqpChannel.Close()

		queue, err := amqpChannel.QueueDeclare("order", true, false, false, false, nil)
		failError(err, "Could not declare queue")

		//rand.Seed(time.Now().UnixNano())

		good := order
		body, err := json.Marshal(good)

		if err != nil {
			failError(err, "Error encoding JSON")
		}
		err = amqpChannel.Publish("", queue.Name, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent, //消息是否持久化，2表示持久化，0或1表示非持久化。
			ContentType:  "text/plain",    //消息的类型，通常为“text/plain”
			Body:         []byte(body),    //消息主体
		})
		if err != nil {
			log.Fatalf("Error publishing message: %s", err)
		}
		log.Printf("AddOrder: %s", string(body))

	})

	r.Run(":8000")

}
