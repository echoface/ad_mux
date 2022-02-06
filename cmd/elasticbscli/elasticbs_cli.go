package main

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/echoface/be_indexer/util"
	"github.com/urfave/cli/v2"
	"golang.org/x/time/rate"
)

type (
	PutReq struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
)

const (
	contentTypeJSON = "application/json"
)

func putURL(addr string) string {
	return fmt.Sprintf("http://%s/store", addr)
}

func main() {
	app := &cli.App{
		Name:    "bs_cli",
		Usage:   "bs_cli cmd args...",
		Version: "v0.0.1",
		Commands: []*cli.Command{
			{
				Name:        "rset",
				Usage:       "bs_cli rset ...",
				Description: "a tool rand set value and then access it value",
				Action: func(context *cli.Context) error {
					interval := time.Millisecond * time.Duration(context.Int("interval"))
					limiter := rate.NewLimiter(rate.Every(interval), int(rate.Every(interval)))

					apiPath := putURL(context.String("server"))
					for i := 0; i < context.Int("count"); i++ {
						_ = limiter.Wait(context.Context)

						payload := []byte(util.JSONString(PutReq{
							Key:   fmt.Sprintf("key%d", i),
							Value: fmt.Sprintf("value%d", i),
						}))

						resp, err := http.Post(apiPath, contentTypeJSON, bytes.NewReader(payload))
						if err != nil {
							return err
						}
						_ = resp.Body.Close()
					}
					return nil
				},
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "server",
						Usage:   "--server=ip:port",
						Aliases: []string{"s"},
						Value:   "127.0.0.1:5000",
					},
					&cli.IntFlag{
						Name:    "count",
						Usage:   "--count=1 how many keys total set",
						Aliases: []string{"c"},
						Value:   1024,
					},
					&cli.IntFlag{
						Name:    "interval",
						Usage:   "--interval=1 interval set every key in ms",
						Aliases: []string{"i"},
						Value:   1000,
					},
				},
			},
		},
	}
	err := app.Run(os.Args)
	util.PanicIfErr(err, "program end with:%v", err)
}
