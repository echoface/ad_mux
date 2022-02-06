package server

import (
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/echoface/elasticbs/server/rafthandler"
	"github.com/echoface/elasticbs/server/storehandler"
	"github.com/hashicorp/raft"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

// srv struct handling server
type srv struct {
	listenAddress string
	raft          *raft.Raft
	echo          *echo.Echo
}

// Start boot up server
func (s srv) Start() error {
	return s.echo.StartServer(&http.Server{
		Addr:         s.listenAddress,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})
}

// New return new server
func New(listenAddr string, badgerDB *badger.DB, r *raft.Raft) *srv {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.Pre(middleware.RemoveTrailingSlash())
	e.GET("/debug/pprof/*", echo.WrapHandler(http.DefaultServeMux))

	// Raft server
	raftHandler := rafthandler.New(r)
	raftRouter := e.Group("/raft")
	raftRouter.POST("/join", raftHandler.JoinRaftHandler)
	raftRouter.POST("/remove", raftHandler.RemoveRaftHandler)
	raftRouter.GET("/stats", raftHandler.StatsRaftHandler)

	// Store server
	storeHandler := storehandler.New(r, badgerDB)
	e.POST("/store", storeHandler.Store)
	e.GET("/store/:key", storeHandler.Get)
	e.DELETE("/store/:key", storeHandler.Delete)

	return &srv{
		listenAddress: listenAddr,
		echo:          e,
		raft:          r,
	}
}
