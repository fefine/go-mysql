package canal

import (
	"sync"

	log "github.com/sirupsen/logrus"
	"go-mysql/mysql"
)

type masterInfo struct {
	sync.RWMutex

	pos mysql.Position

	gtid mysql.GTIDSet
}

func (m *masterInfo) Update(pos mysql.Position) {
	log.Debugf("update master position %s", pos)

	m.Lock()
	m.pos = pos
	// todo save postion
	m.Unlock()
}

func (m *masterInfo) UpdateGTID(gtid mysql.GTIDSet) {
	log.Debugf("update master gtid %s", gtid.String())

	m.Lock()
	m.gtid = gtid
	m.Unlock()
}

func (m *masterInfo) Position() mysql.Position {
	m.RLock()
	defer m.RUnlock()
	// todo get position from file on first start
	return m.pos
}

func (m *masterInfo) GTID() mysql.GTIDSet {
	m.RLock()
	defer m.RUnlock()

	return m.gtid
}
