/*
create keyspace testwrite with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };
create table testwrite.tweet(timeline text, id UUID, text text, linenumber int, PRIMARY KEY(id));
create index on testwrite.tweet(timeline);
*/

package main

import (
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"time"
)

const insertNum = 5000000
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
  defer func(){
	   ch <- 1
  }()
    
  var cluster *gocql.ClusterConfig
  cluster = gocql.NewCluster("192.168.1.68", "192.168.1.67")
  cluster.Keyspace = "testwrite"
	cluster.Consistency = gocql.Quorum
	cluster.Authenticator = passwdAuth
  cluster.NumConns = 10

  session, err := cluster.CreateSession()
  if err != nil{
    log.Println(i,"err:",err,"we return")
    return
  }
	defer session.Close()
	
	i++
	startTime := currentTimeMillis()
	log.Println("第", i, "个开始", time.Now().String())
	for j := 0; j < insertNum; j++ {
		if err := session.Query(`INSERT INTO tweet (timeline, id, text) VALUES (?, ?, ?)`,
			"me", gocql.TimeUUID(),fmt.Sprintf("111111111100000000002222222222%d", j)).Exec(); err != nil {
			log.Println("第",i,"err when write ","insert :",err)
		}
	}
	endTime := currentTimeMillis()
	log.Println("第", i, "个结束", time.Now().String(), ",time is:", endTime-startTime, "毫秒,", "数据量为:", insertNum,  "条")

	return
}

func currentTimeMillis() int64 {
	return time.Now().UnixNano() / 1000000
}
