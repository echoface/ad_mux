package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/v3"
	"github.com/echoface/be_indexer/util"
	"github.com/echoface/elasticbs/server"
)

var flagConfig string
var flagbootstrap bool

func init() {
	flag.StringVar(&flagConfig, "config", "", "--config=config.yaml")
	flag.BoolVar(&flagbootstrap, "bootstrap", false, "--bootstrap=true|false")
}

func main() {
	flag.Parse()

	config, err := server.NewConfig(flagConfig)
	util.PanicIfErr(err, "fail open config file:%s", flagConfig)

	// Preparing badgerDB
	badgerOpt := badger.DefaultOptions(filepath.Join(config.DocStoreVolume, "db"))
	badgerDB, err := badger.Open(badgerOpt)
	util.PanicIfErr(err, "fail open storage from:%s", config.DocStoreVolume)
	defer func() {
		if err := badgerDB.Close(); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "error close badgerDB: %s\n", err.Error())
		}
	}()
	raftImpl, err := server.SetupRaft(badgerDB, config, flagbootstrap)
	fmt.Printf("init raft stats:%s", util.JSONPretty(raftImpl.Stats()))

	srv := server.New(config.ServerBind, badgerDB, raftImpl)
	err = srv.Start()
	fmt.Printf("elasticbs server end with err:%v", err)
}
