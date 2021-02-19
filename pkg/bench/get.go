/*
 * Warp (C) 2019-2020 MinIO, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package bench

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio/pkg/console"
	"github.com/minio/warp/pkg/generator"
)

// Get benchmarks download speed.
type Get struct {
	CreateObjects int
	RandomRanges  bool
	Collector     *Collector
	objects       generator.Objects
	LogPath       string // add by guo.hao
	PutLogPath    string // add by guo.hao
	// Default Get options.
	GetOpts minio.GetObjectOptions
	Common
}

func (c *Common) readAccessLog(content map[string]interface{}) error {

	return nil
}

// Prepare will create an empty bucket or delete any content already there
// and upload a number of objects.
func (g *Get) Prepare(ctx context.Context) error {

	e := g.createAccessLog(ctx, g.LogPath)
	if e != nil {
		return e
	}

	if err := g.createEmptyBucket(ctx); err != nil {
		return err
	}
	src := g.Source()
	console.Info("\rUploading ", g.CreateObjects, " objects of ", src.String())
	var wg sync.WaitGroup
	wg.Add(g.Concurrency)
	g.Collector = NewCollector()
	obj := make(chan struct{}, g.CreateObjects)
	for i := 0; i < g.CreateObjects; i++ {
		obj <- struct{}{}
	}
	close(obj)
	var groupErr error
	var mu sync.Mutex

	// 两种造数据的方式
	// 1. 读取日志文件
	// 2. 自己上传
	if len(g.PutLogPath) > 0 {
		res_file, put_log_err := os.Open(g.PutLogPath)
		if put_log_err != nil {
			return put_log_err
		}

		rd := bufio.NewReader(res_file)
		for {
			line, err := rd.ReadString('\n') //以'\n'为结束符读入一行
			if err != nil || io.EOF == err {
				break
			}
			client, cldone := g.Client()
			//{"bucket":"test01","cost":946923.896776,"etag":"","msg":"","object":"lE7rQRnWDCmVKOXk.csv","size":97537030,"status":"succ"}
			//{"bucket":"","cost":1200742.249267,"etag":"","msg":"Put \"http://ozone.s3gtest.qihoo.net/test01/M2QHUPjT5P%29NpQiX.csv\": net/http: timeout awaiting response headers","object":"","size":0,"status":"err"}
			alog := make(map[string]interface{})
			err1 := json.Unmarshal([]byte(strings.TrimSpace(line)), &alog)
			if err1 != nil {
				continue
			}
			s, ok := alog["status"]
			if ok {
				ss := s.(string)
				if ss == "succ" {
					Start := time.Now()
					op := Operation{
						OpType:   http.MethodPut,
						Thread:   uint16(rand.Intn(g.Concurrency - 1)),
						Size:     alog["size"].(int64),
						File:     alog["object"].(string),
						ObjPerOp: 1,
						Endpoint: client.EndpointURL().String(),
						Start:    Start,
						End:      Start,
					}
					rcv := g.Collector.Receiver()
					rcv <- op
				}
			}
			cldone()
		}
	} else {
		for i := 0; i < g.Concurrency; i++ {
			go func(i int) {
				defer wg.Done()
				src := g.Source()
				for range obj {
					opts := g.PutOpts
					rcv := g.Collector.Receiver()
					done := ctx.Done()

					select {
					case <-done:
						return
					default:
					}
					obj := src.Object()
					client, cldone := g.Client()
					op := Operation{
						OpType:   http.MethodPut,
						Thread:   uint16(i),
						Size:     obj.Size,
						File:     obj.Name,
						ObjPerOp: 1,
						Endpoint: client.EndpointURL().String(),
					}
					opts.ContentType = obj.ContentType
					op.Start = time.Now()
					res, err := client.PutObject(ctx, g.Bucket, obj.Name, obj.Reader, obj.Size, opts)
					op.End = time.Now()
					writeLog := false
					latency := op.End.Sub(op.Start).Seconds() * 1000

					slow := op.End.Sub(op.Start).Seconds() < float64(obj.Size/1024/1024)

					if err != nil {
						err := fmt.Errorf("upload error: %w", err)
						g.Error(err)
						mu.Lock()
						if groupErr == nil {
							groupErr = err
						}
						mu.Unlock()

						writeLog = true
						m := make(map[string]interface{})
						m["status"] = "err"
						m["action"] = "put"
						m["bucket"] = g.Bucket
						m["object"] = obj.Name
						m["cost"] = latency
						m["etag"] = res.ETag
						m["size"] = res.Size
						m["msg"] = op.Err
						m["start"] = op.Start.Format("2006-01-02T15:04:05")
						m["slow"] = slow
						g.writeAccessLog(m)

						return
					}
					obj.VersionID = res.VersionID
					if res.Size != obj.Size {
						err := fmt.Errorf("short upload. want: %d, got %d", obj.Size, res.Size)
						g.Error(err)
						mu.Lock()
						if groupErr == nil {
							groupErr = err
						}
						mu.Unlock()
						return
					}
					if !writeLog {
						m := make(map[string]interface{})
						m["status"] = "succ"
						m["action"] = "put"
						m["bucket"] = res.Bucket
						m["object"] = res.Key
						m["cost"] = latency
						m["etag"] = res.ETag
						m["size"] = res.Size
						m["msg"] = op.Err
						m["start"] = op.Start.Format("2006-01-02T15:04:05")
						m["slow"] = slow
						g.writeAccessLog(m)
					}

					cldone()
					mu.Lock()
					obj.Reader = nil
					g.objects = append(g.objects, *obj)
					g.prepareProgress(float64(len(g.objects)) / float64(g.CreateObjects))
					mu.Unlock()
					rcv <- op
				}
			}(i)
		}
	}

	wg.Wait()
	return groupErr
}

type firstByteRecorder struct {
	t *time.Time
	r io.Reader
}

func (f *firstByteRecorder) Read(p []byte) (n int, err error) {
	if f.t != nil || len(p) == 0 {
		return f.r.Read(p)
	}
	// Read a single byte.
	n, err = f.r.Read(p[:1])
	if n > 0 {
		t := time.Now()
		f.t = &t
	}
	return n, err
}

// Start will execute the main benchmark.
// Operations should begin executing when the start channel is closed.
func (g *Get) Start(ctx context.Context, wait chan struct{}) (Operations, error) {
	var wg sync.WaitGroup
	wg.Add(g.Concurrency)
	c := g.Collector
	if g.AutoTermDur > 0 {
		ctx = c.AutoTerm(ctx, http.MethodGet, g.AutoTermScale, autoTermCheck, autoTermSamples, g.AutoTermDur)
	}

	// Non-terminating context.
	nonTerm := context.Background()

	for i := 0; i < g.Concurrency; i++ {
		go func(i int) {
			rng := rand.New(rand.NewSource(int64(i)))
			rcv := c.Receiver()
			defer wg.Done()
			opts := g.GetOpts
			done := ctx.Done()

			<-wait
			for {
				select {
				case <-done:
					return
				default:
				}
				fbr := firstByteRecorder{}
				obj := g.objects[rng.Intn(len(g.objects))]
				client, cldone := g.Client()
				op := Operation{
					OpType:   http.MethodGet,
					Thread:   uint16(i),
					Size:     obj.Size,
					File:     obj.Name,
					ObjPerOp: 1,
					Endpoint: client.EndpointURL().String(),
				}
				if g.RandomRanges && op.Size > 2 {
					// Randomize length similar to --obj.randsize
					size := generator.GetExpRandSize(rng, op.Size-2)
					start := rng.Int63n(op.Size - size)
					end := start + size
					op.Size = end - start + 1
					opts.SetRange(start, end)
				}
				op.Start = time.Now()
				var err error
				opts.VersionID = obj.VersionID
				writeLog := false
				o, err := client.GetObject(nonTerm, g.Bucket, obj.Name, opts)
				if err != nil {
					g.Error("download error:", err)
					op.Err = err.Error()
					op.End = time.Now()

					latency := op.End.Sub(op.Start).Seconds() * 1000
					slow := op.End.Sub(op.Start).Seconds() < float64(obj.Size/1024/1024)
					writeLog = true
					m := make(map[string]interface{})
					m["status"] = "err"
					m["action"] = "get"
					m["bucket"] = g.Bucket
					m["object"] = obj.Name
					m["cost"] = latency
					m["etag"] = ""
					m["size"] = op.Size
					m["msg"] = op.Err
					m["start"] = op.Start.Format("2006-01-02T15:04:05")
					m["slow"] = slow
					g.writeAccessLog(m)

					rcv <- op
					cldone()
					continue
				}
				fbr.r = o
				n, err := io.Copy(ioutil.Discard, &fbr)
				if err != nil {
					g.Error("download error:", err)
					op.Err = err.Error()
				}
				op.FirstByte = fbr.t
				op.End = time.Now()
				if n != op.Size && op.Err == "" {
					op.Err = fmt.Sprint("unexpected download size. want:", op.Size, ", got:", n)
					g.Error(op.Err)
				}

				if !writeLog {
					latency := op.End.Sub(op.Start).Seconds() * 1000
					slow := op.End.Sub(op.Start).Seconds() < float64(obj.Size/1024/1024)
					m := make(map[string]interface{})
					m["status"] = "succ"
					m["action"] = "get"
					m["bucket"] = g.Bucket
					m["object"] = obj.Name
					m["cost"] = latency
					m["etag"] = ""
					m["size"] = op.Size
					m["msg"] = op.Err
					m["start"] = op.Start.Format("2006-01-02T15:04:05")
					m["slow"] = slow
					g.writeAccessLog(m)
				}

				rcv <- op
				cldone()
				o.Close()
			}
		}(i)
	}
	wg.Wait()
	return c.Close(), nil
}

// Cleanup deletes everything uploaded to the bucket.
func (g *Get) Cleanup(ctx context.Context) {
	g.deleteAllInBucket(ctx, g.objects.Prefixes()...)
}
