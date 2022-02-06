package server

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/labstack/gommon/log"

	"github.com/dgraph-io/badger/v3"
	"github.com/echoface/elasticbs/pkg/bestore/fsm"

	badgerStore "github.com/BBVA/raft-badger"
	"github.com/echoface/be_indexer/util"
	"github.com/hashicorp/raft"
)

const (
	raftLogPath       = "./raft/"
	snapshotRetainMax = 2
)

func SetupRaft(db *badger.DB, c *BSConfig, bootstrap bool) (*raft.Raft, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", c.RaftBindAddr)
	util.PanicIfErr(err, "create transport fail, bind:%s", c.RaftBindAddr)
	transport, err := raft.NewTCPTransport(tcpAddr.String(), tcpAddr, 2, time.Second*5, os.Stdout)
	util.PanicIfErr(err, "create transport fail, bind:%s", c.RaftBindAddr)

	fmt.Println("raft will use address:", tcpAddr.String())

	raftPath := filepath.Join(c.DocStoreVolume, raftLogPath, "log")
	raftStore, err := badgerStore.NewBadgerStore(filepath.Join(raftPath, "log"))
	util.PanicIfErr(err, "create raft backend storage fail:%v, path:%s", err)

	// Wrap the store in a LogCache to improve performance.
	cacheLogStore, err := raft.NewLogCache(1024, raftStore)
	util.PanicIfErr(err, "create cache store fail")

	snapshotStore, err := raft.NewFileSnapshotStore(raftPath, snapshotRetainMax, os.Stdout)
	util.PanicIfErr(err, "create snapshotStore fail")

	raftConf := raft.DefaultConfig()
	raftConf.LogLevel = "trace"
	raftConf.SnapshotThreshold = 20
	raftConf.SnapshotInterval = time.Minute * 2
	raftConf.LocalID = raft.ServerID(tcpAddr.String())

	fsmImpl := fsm.NewBadger(db)
	raftServer, err := raft.NewRaft(raftConf, fsmImpl, cacheLogStore, raftStore, snapshotStore, transport)
	util.PanicIfErr(err, "initialize raft machine fail")

	go func() {
		ch := raftServer.LeaderCh()
		for {
			leader, ok := <-ch
			log.Infof("leadership change:%t, ok:%t, current:%s", leader, ok, raftServer.Leader())
		}
	}()

	raftServer.LeadershipTransfer()
	if bootstrap {
		// TODO: pull last configuration from remote and bootstrap
		// always start single server as a leader
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftConf.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		f := raftServer.BootstrapCluster(configuration)
		log.Infof("raft machine bootstrap err:", f.Error())
	}
	return raftServer, nil
}
