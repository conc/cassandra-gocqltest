/*
create keyspace testwrite with replication = {'class':'SimpleStrategy','replication_factor': 2 };
create table testwrite.tweet(
	timeline text,
	id UUID,
	text text,
	PRIMARY KEY(id)
)with caching='KEYS_ONLY'
AND read_repair_chance = 0.000001;
*/

package main

import (
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"time"
	"strings"
)

const insertNum = 100000
const goruntime_num = 20

var passwdAuth gocql.PasswordAuthenticator

func main() {

	passwdAuth.Username = "username"
	passwdAuth.Password = "passwd"

	chs := make([]chan int, goruntime_num)

	for i := 0; i < goruntime_num; i++ {
		chs[i] = make(chan int)
		go doOnce(chs[i], i)
	}

	for _, ch := range chs {
		<-ch
	}

	return
}

func doOnce(ch chan int, i int) {
	defer func() {
		ch <- 1
	}()

	cluster := gocql.NewCluster("192.168.1.67", "192.168.1.68")
	cluster.Keyspace = "testwrite"
	cluster.Consistency = gocql.Quorum
	cluster.Authenticator = passwdAuth
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 10} //失败重试次数
	cluster.NumConns = 10

	session, err := cluster.CreateSession()
	if err != nil {
		log.Println(i, "err:", err, "we return")
		return
	}
	defer session.Close()

	i++
	startTime := currentTimeMillis()
	log.Println("第", i, "个开始", time.Now().String())

	for j := 0; j < insertNum; j++ {
		begin := "BEGIN BATCH"
		end := "APPLY BATCH"
		var query string
		
		for k:=0;k<50;k++{
			query += fmt.Sprintf("INSERT INTO tweet (timeline, id, text) VALUES ('mememmememme',"+
				"%s, '%d');", gocql.TimeUUID(), k)
		}
		fullQuery := strings.Join([]string{begin, query, end}, "\n")
		
		if err := session.Query(fullQuery).Exec(); err != nil {
			log.Println("[error:] 第",i,"err when write ","insert :",err)
		}
	}
	
	endTime := currentTimeMillis()
	log.Println("第", i, "个结束", time.Now().String(), ",time is:", endTime-startTime, "毫秒,", "数据量为:", insertNum, "条")

	return
}

func currentTimeMillis() int64 {
	return time.Now().UnixNano() / 1000000
}
