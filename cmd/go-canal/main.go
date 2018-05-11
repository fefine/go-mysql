package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go-mysql/canal"
	"go-mysql/mysql"
	"go-mysql/replication"
	log "github.com/sirupsen/logrus"
	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
	"context"
	"encoding/json"
	"github.com/fefine/fqueue/producer"
)

var (
	positionNotExist = errors.New("not found binlog position on etcd")
)

var strEndpoints = flag.String("etcd.endpoints", "127.0.0.1:2379", "etcd endpoints, must provide")
var topic = flag.String("topic", "127.0.0.1:2379", "etcd endpoints, must provide")
//var partioner = flag.String("mq.partioner", "", "partitioner")
var configFilePath = flag.String("c", "", "configuration file path")
var overwritePos = flag.Bool("override", false, "provide position override saved position")

var host = flag.String("host", "127.0.0.1", "MySQL host")
var port = flag.Int("port", 3306, "MySQL port")
var user = flag.String("user", "root", "MySQL user, must have replication privilege")
var password = flag.String("password", "Fsh950905", "MySQL password")

var flavor = flag.String("flavor", "mysql", "Flavor: mysql or mariadb")

var serverID = flag.Int("server-id", 101, "Unique Server ID")
var mysqldump = flag.String("mysqldump", "mysqldump", "mysqldump execution path")

var dbs = flag.String("dbs", "springdemo", "dump databases, seperated by comma")
var tables = flag.String("tables", "person", "dump tables, seperated by comma, will overwrite dbs")
var tableDB = flag.String("table_db", "test", "database for dump tables")
var ignoreTables = flag.String("ignore_tables", "", "ignore tables, must be database.table format, separated by comma")

var startName = flag.String("bin_name", "mysql-bin.000001", "start sync from binlog name")
var startPos = flag.Uint("bin_pos", 4, "start sync from binlog position of")

var heartbeatPeriod = flag.Duration("heartbeat", 60*time.Second, "master heartbeat period")
var readTimeout = flag.Duration("read_timeout", 90*time.Second, "connection read timeout")


func main() {
	flag.Parse()
	// TODO 0，从配置文件中读取配置
	cfg := canal.NewDefaultConfig()
	if *configFilePath == "" {
		cfg.Addr = fmt.Sprintf("%s:%d", *host, *port)
		cfg.User = *user
		cfg.Password = *password
		cfg.Flavor = *flavor

		cfg.ReadTimeout = *readTimeout
		cfg.HeartbeatPeriod = *heartbeatPeriod
		cfg.ServerID = uint32(*serverID)
		cfg.Dump.ExecutionPath = *mysqldump
		cfg.Dump.DiscardErr = false
	} else {
		// parse config from file
	}
	c, err := canal.NewCanal(cfg)
	if err != nil {
		fmt.Printf("create canal err %v", err)
		os.Exit(1)
	}

	if len(*ignoreTables) == 0 {
		subs := strings.Split(*ignoreTables, ",")
		for _, sub := range subs {
			if seps := strings.Split(sub, "."); len(seps) == 2 {
				c.AddDumpIgnoreTables(seps[0], seps[1])
			}
		}
	}

	if len(*tables) > 0 && len(*tableDB) > 0 {
		subs := strings.Split(*tables, ",")
		c.AddDumpTables(*tableDB, subs...)
	} else if len(*dbs) > 0 {
		subs := strings.Split(*dbs, ",")
		c.AddDumpDatabases(subs...)
	}

	handler, err := NewBinlogEventHandler(*strEndpoints, *topic, "")
	if err != nil {
		log.Fatal("create binlog handler failed, err: ", err)
	}

	c.SetEventHandler(handler)

	var position *mysql.Position
	noPosition := false

	if *overwritePos {
		if *startName == "" {
			log.Fatal("must provide binlog position")
		}
		position = &mysql.Position{
			Name: *startName,
			Pos:  uint32(*startPos),
		}

	} else {
		// 从etcd中获取position
		pos, err := handler.GetLatestPosition()
		if err != nil {
			if err == positionNotExist {
				noPosition = true
			} else {
				log.Fatal(err)
			}
		}
		position = pos
	}

	go func() {
		if noPosition {
			err = c.Run()
		} else {
			err = c.RunFrom(*position)
		}
		if err != nil {
			fmt.Printf("start canal err %v", err)
		}
	}()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	<-sc

	c.Close()
}



type BinlogEventHandler struct {
	canal.DummyEventHandler

	client    *clientv3.Client
	Endpoints []string
	topic     string
	Position  mysql.Position
	Producer  *producer.Producer
}

func NewBinlogEventHandler(strEndpoints, topic, partitioner string) (handler *BinlogEventHandler, err error) {
	if strEndpoints == "" || topic == "" {
		if strEndpoints == "" {
			return nil, errors.New("must provide strEndpoints")
		} else {
			return nil, errors.New("must provide topic")
		}
	}
	handler = new(BinlogEventHandler)
	// connect etcd
	handler.client, err = handler.connectEtcd()
	if err != nil {
		log.Error("connect to etcd failed")
		return
	}
	log.Info("connect to etcd success")
	// TODO 创建producer
	producerConfig := &producer.ProducerConfig{EtcdEndpoints: handler.Endpoints, Debug:true}
	msgProducer, err := producer.NewProducer(producerConfig)
	if err != nil {
		return
	}
	handler.Producer = msgProducer
	return
}

// 从
func (handler *BinlogEventHandler) GetLatestPosition() (pos *mysql.Position, err error) {
	// save: /position = {}
	resp, err := handler.client.Get(context.Background(), "/positions/pos")
	if err != nil {
		log.Errorf("get position error, err: %v", err)
		return
	}
	if resp.Count == 0 {
		// 说明之前并不存在
		return pos, positionNotExist
	} else {
		pos = new(mysql.Position)
		err = json.Unmarshal(resp.Kvs[0].Value, pos)
		if err != nil {
			log.Error(err)
		}
		return
	}
}

func (handler *BinlogEventHandler) savePosition(position mysql.Position) {
	value, err := json.Marshal(position)
	if err != nil {
		log.Error("save position failed, marshal position failed, err: ", err)
		return
	}
	_, err = handler.client.Put(context.Background(), "/position/pos", string(value))
	if err != nil {
		log.Error("save position failed, put failed, err: ", err)
	}
}

// 连接etcd
func (handler *BinlogEventHandler) connectEtcd() (client *clientv3.Client, er error) {
	client, er = clientv3.New(clientv3.Config{Endpoints: handler.Endpoints})
	if er != nil {
		log.Errorln("consumer connect etcd failed, err:", er)
	}
	return
}

// binlog位置改变
func (h *BinlogEventHandler) OnRotate(event *replication.RotateEvent) error {
	log.Infof("[rotate] position: %d, next: %d", event.Position, string(event.NextLogName))
	return nil
}

func (h *BinlogEventHandler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	value, err := json.Marshal(queryEvent)
	if err != nil {
		log.Errorf("marshal row error, err: ", err)
		return err
	}
	key := fmt.Sprintf("%s_ddl", queryEvent.Schema, queryEvent.Schema)
	log.Debugf("push: key: %s, value: %s", key, string(value))
	return h.Producer.Push(context.Background(), h.topic, []byte(key), value, nil)
}

func (h *BinlogEventHandler) OnRow(event *canal.RowsEvent) error                 {
	value, err := json.Marshal(event)
	if err != nil {
		log.Errorf("marshal row error, err: ", err)
		return err
	}
	key := fmt.Sprintf("%s_%s_%d", event.Table.Schema, event.Table.Name, event.Table.Columns[0])
	log.Debugf("push: key: %s, value: %s", key, string(value))
	return h.Producer.Push(context.Background(), h.topic, []byte(key), value, nil)
}
func (h *BinlogEventHandler) OnXID(mysql.Position) error             { return nil }
func (h *BinlogEventHandler) OnGTID(mysql.GTIDSet) error             { return nil }

func (h *BinlogEventHandler) OnPosSynced(position mysql.Position, b bool) error {
	h.Position = position
	h.savePosition(position)
	return nil
}

func (h *BinlogEventHandler) String() string {
	return "BinlogEventHandler"
}
