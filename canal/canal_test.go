package canal

import (
	"bytes"
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"go-mysql/mysql"
	"go-mysql/replication"
	log "github.com/sirupsen/logrus"
)

var testHost = flag.String("host", "127.0.0.1", "MySQL host")

func Test(t *testing.T) {
	TestingT(t)
}

type canalTestSuite struct {
	c *Canal
}

var _ = Suite(&canalTestSuite{})

func (s *canalTestSuite) SetUpSuite(c *C) {
	cfg := NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:3306", *testHost)
	cfg.User = "root"
	cfg.Password = "Fsh950905"
	cfg.HeartbeatPeriod = 200 * time.Millisecond
	cfg.ReadTimeout = 300 * time.Millisecond
	//cfg.Dump.ExecutionPath = "mysqldump"
	cfg.Dump.TableDB = "canal_test"
	//cfg.Dump.Tables = []string{"t1"}
	cfg.Dump.Where = "id>0"

	// include & exclude config
	//cfg.IncludeTableRegex = make([]string, 1)
	//cfg.IncludeTableRegex[0] = ".*\\.canal_test"
	//cfg.ExcludeTableRegex = make([]string, 2)
	//cfg.ExcludeTableRegex[0] = "mysql\\..*"
	//cfg.ExcludeTableRegex[1] = ".*\\..*_inner"

	var err error
	s.c, err = NewCanal(cfg)
	c.Assert(err, IsNil)
	/*
	s.execute(c, "DROP TABLE IF EXISTS test.canal_test")
	sql := `
        CREATE TABLE IF NOT EXISTS test.canal_test (
            id int AUTO_INCREMENT,
            name varchar(100),
            PRIMARY KEY(id)
            )ENGINE=innodb;
    `

	s.execute(c, sql)

	s.execute(c, "DELETE FROM test.canal_test")
	s.execute(c, "INSERT INTO test.canal_test (name) VALUES (?), (?), (?)", "a", "b", "c")

	s.execute(c, "SET GLOBAL binlog_format = 'ROW'")
	*/

	s.c.SetEventHandler(&testEventHandler{c: c})
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.c.Run()
	}()
	c.Assert(<-errCh, IsNil)
}

func (s *canalTestSuite) TearDownSuite(c *C) {
	// To test the heartbeat and read timeout,so need to sleep 1 seconds without data transmission
	log.Info("Start testing the heartbeat and read timeout")
	time.Sleep(time.Second)

	if s.c != nil {
		s.c.Close()
		s.c = nil
	}
}

func (s *canalTestSuite) execute(c *C, query string, args ...interface{}) *mysql.Result {
	log.Infof("exe sql: %s", query)
	r, err := s.c.Execute(query, args...)
	c.Assert(err, IsNil)
	return r
}

type testEventHandler struct {
	DummyEventHandler
	c *C
}

func (h *testEventHandler) OnRow(e *RowsEvent) error {
	for i, r := range e.Rows {
		log.Infof("index %d, row %v", i, r)
	}
	log.Infof("%s %v", e.Action, e.Rows)
	return nil
}

func (h *testEventHandler) OnRotate(e *replication.RotateEvent) error {
	log.Infof("rotate- %v %d", string(e.NextLogName), e.Position)
	return nil
}
func (h *testEventHandler) OnTableChanged(schema string, table string) error {
	log.Infof("tablechanged schema: %s talbe: %s", schema, table)
	return nil
}
func (h *testEventHandler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	log.Infof("ddl- %v", string(queryEvent.Query))
	return nil
}

func (h *testEventHandler) OnXID(e mysql.Position) error             {
	log.Infof("xid %v", e)
	return nil
}
func (h *testEventHandler) OnGTID(e mysql.GTIDSet) error             {
	log.Infof("gtid %v", e.String())
	return nil
}
func (h *testEventHandler) OnPosSynced(e mysql.Position, b bool) error {
	log.Infof("sync %v", e)
	return nil
}

func (h *testEventHandler) String() string {
	return "testEventHandler"
}

func (s *canalTestSuite) TestCanal(c *C) {
	<-s.c.WaitDumpDone()

	for i := 1; i < 10; i++ {
		s.execute(c, "INSERT INTO test.canal_test (name) VALUES (?)", fmt.Sprintf("%d", i))
	}
	s.execute(c, "ALTER TABLE test.canal_test ADD `age` INT(5) NOT NULL AFTER `name`")
	s.execute(c, "INSERT INTO test.canal_test (name,age) VALUES (?,?)", "d", "18")

	err := s.c.CatchMasterPos(10 * time.Second)
	c.Assert(err, IsNil)
}

func (s *canalTestSuite) TestCanalFilter(c *C) {
	// included
	sch, err := s.c.GetTable("test", "canal_test")
	c.Assert(err, IsNil)
	c.Assert(sch, NotNil)
	_, err = s.c.GetTable("not_exist_db", "canal_test")
	c.Assert(errors.Trace(err), Not(Equals), ErrExcludedTable)
	// excluded
	sch, err = s.c.GetTable("test", "canal_test_inner")
	c.Assert(errors.Cause(err), Equals, ErrExcludedTable)
	c.Assert(sch, IsNil)
	sch, err = s.c.GetTable("mysql", "canal_test")
	c.Assert(errors.Cause(err), Equals, ErrExcludedTable)
	c.Assert(sch, IsNil)
	sch, err = s.c.GetTable("not_exist_db", "not_canal_test")
	c.Assert(errors.Cause(err), Equals, ErrExcludedTable)
	c.Assert(sch, IsNil)
}

func TestCreateTableExp(t *testing.T) {
	cases := []string{
		"CREATE TABLE `mydb.mytable` (`id` int(10)) ENGINE=InnoDB",
		"CREATE TABLE `mytable` (`id` int(10)) ENGINE=InnoDB",
		"CREATE TABLE IF NOT EXISTS `mytable` (`id` int(10)) ENGINE=InnoDB",
		"CREATE TABLE IF NOT EXISTS mytable (`id` int(10)) ENGINE=InnoDB",
	}
	table := []byte("mytable")
	db := []byte("mydb")
	for _, s := range cases {
		m := expCreateTable.FindSubmatch([]byte(s))
		mLen := len(m)
		if m == nil || !bytes.Equal(m[mLen-1], table) || (len(m[mLen-2]) > 0 && !bytes.Equal(m[mLen-2], db)) {
			t.Fatalf("TestCreateTableExp: case %s failed", s)
		}
	}
}

func TestAlterTableExp(t *testing.T) {
	cases := []string{
		"ALTER TABLE `mydb`.`mytable` ADD `field2` DATE  NULL  AFTER `field1`;",
		"ALTER TABLE `mytable` ADD `field2` DATE  NULL  AFTER `field1`;",
		"ALTER TABLE mydb.mytable ADD `field2` DATE  NULL  AFTER `field1`;",
		"ALTER TABLE mytable ADD `field2` DATE  NULL  AFTER `field1`;",
		"ALTER TABLE mydb.mytable ADD field2 DATE  NULL  AFTER `field1`;",
	}

	table := []byte("mytable")
	db := []byte("mydb")
	for _, s := range cases {
		m := expAlterTable.FindSubmatch([]byte(s))
		mLen := len(m)
		if m == nil || !bytes.Equal(m[mLen-1], table) || (len(m[mLen-2]) > 0 && !bytes.Equal(m[mLen-2], db)) {
			t.Fatalf("TestAlterTableExp: case %s failed", s)
		}
	}
}

func TestRenameTableExp(t *testing.T) {
	cases := []string{
		"rename table `mydb`.`mytable` to `mydb`.`mytable1`",
		"rename table `mytable` to `mytable1`",
		"rename table mydb.mytable to mydb.mytable1",
		"rename table mytable to mytable1",

		"rename table `mydb`.`mytable` to `mydb`.`mytable2`, `mydb`.`mytable3` to `mydb`.`mytable1`",
		"rename table `mytable` to `mytable2`, `mytable3` to `mytable1`",
		"rename table mydb.mytable to mydb.mytable2, mydb.mytable3 to mydb.mytable1",
		"rename table mytable to mytable2, mytable3 to mytable1",
	}
	table := []byte("mytable")
	db := []byte("mydb")
	for _, s := range cases {
		m := expRenameTable.FindSubmatch([]byte(s))
		mLen := len(m)
		if m == nil || !bytes.Equal(m[mLen-1], table) || (len(m[mLen-2]) > 0 && !bytes.Equal(m[mLen-2], db)) {
			t.Fatalf("TestRenameTableExp: case %s failed", s)
		}
	}
}

func TestDropTableExp(t *testing.T) {
	cases := []string{
		"drop table test1",
		"DROP TABLE test1",
		"DROP TABLE test1",
		"DROP table IF EXISTS test.test1",
		"drop table `test1`",
		"DROP TABLE `test1`",
		"DROP table IF EXISTS `test`.`test1`",
		"DROP TABLE `test1` /* generated by server */",
		"DROP table if exists test1",
		"DROP table if exists `test1`",
		"DROP table if exists test.test1",
		"DROP table if exists `test`.test1",
		"DROP table if exists `test`.`test1`",
		"DROP table if exists test.`test1`",
		"DROP table if exists test.`test1`",
	}

	table := []byte("test1")
	for _, s := range cases {
		m := expDropTable.FindSubmatch([]byte(s))
		mLen := len(m)
		if m == nil {
			t.Fatalf("TestDropTableExp: case %s failed", s)
			return
		}
		if mLen < 4 {
			t.Fatalf("TestDropTableExp: case %s failed", s)
			return
		}
		if !bytes.Equal(m[mLen-1], table) {
			t.Fatalf("TestDropTableExp: case %s failed", s)
		}
	}
}
