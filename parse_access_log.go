package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {

	o := flag.String("input", "", "Use -input <file>")

	flag.Parse()
	input_file := *o

	if len(input_file) == 0 {
		log.Panic("请输入文件名")
	}

	res_file, put_log_err := os.Open(input_file)
	if put_log_err != nil {
		log.Panic(put_log_err)
	}

	rd := bufio.NewReader(res_file)
	i := 0
	for {
		line, err := rd.ReadString('\n') //以'\n'为结束符读入一行
		if err != nil || io.EOF == err {
			break
		}
		alog := make(map[string]interface{})
		err1 := json.Unmarshal([]byte(strings.TrimSpace(line)), &alog)
		if err1 != nil {
			continue
		}

		//idx     thread  op      client_id       n_objects       bytes   endpoint        file    error   start   first_byte      end     duration_ns
		//0       222     PUT     FZz0y2  1       4194304 http://d47.xstore.shbt.qihoo.net:9878   nrWIbdO)E)CESCrc.csv            2021-03-26T14:52:39.628851009+08:00             2021-03-26T14:52:39.84383554+08:00      214984531

		end, _ := time.ParseInLocation("2006-01-02T15:04:05", alog["datetime"].(string), time.Local)

		var i2 int32

		j, _ := strconv.ParseInt(alog["cost"].(string), 10, 32)
		i2 = int32(j) * -1

		start := end.Add(time.Duration(i2) * time.Millisecond)

		fmt.Println(i, rand.Intn(200), alog["method"].(string), GetRandomString(6), 1, int64(alog["object_size"].(float64)), "http://test-ozone", alog["key"].(string),
			"", "", GetGMT(start), GetGMT(end), alog["cost"].(int32)*1000000)

		i++
	}

}

var VERSION_BASE_CODE = []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func GetRandomString(leng int) string {
	result := []byte{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < leng; i++ {
		result = append(result, VERSION_BASE_CODE[r.Intn(len(VERSION_BASE_CODE))])
	}
	return string(result)
}

func GetGMT(update_time time.Time) string {

	location, _ := time.LoadLocation("GMT")

	return update_time.In(location).Format(time.RFC3339Nano)

}

/*
	{
		"hostname": "cn-test-1.xstore.qihoo.net",
		"datetime": "2021-06-09T18:15:59",
		"request_id": "70677584-7b75-4024-afb4-a1bdb542ea5c",
		"requester_id": -1,
		"owner_id": -1,
		"code": 200,
		"cost": "3029",
		"client_ip": "10.18.22.17",
		"bucket": "test01",
		"origin_bucket": "",
		"cluster_id": "",
		"operation": "PutObject",
		"method": "PUT",
		"key": "chinadaily1.txt",
		"path": "test01/chinadaily1.txt",
		"request_uri": "/test01/chinadaily1.txt",
		"user_agent": "aws-sdk-java/1.11.543 Windows_10/10.0 Java_HotSpot(TM)_64-Bit_Server_VM/25.171-b11 java/1.8.0_171 vendor/Oracle_Corporation",
		"length": 270,
		"real_length": 270,
		"object_size": 270,
		"res_bytes_sent": 0,
		"req_bytes_sent": 270,
		"error_code": "",
		"error_detail": "",
		"version_id": "",
		"host_id": "0ffd2e453a29156bc48c38e613de527e",
		"tls_version": "",
		"proto_version": "HTTP/1.1",
		"range": "",
		"copy_source": "",
		"copy_source_bucket": "",
		"copy_source_key": "",
		"copy_range": "",
		"etag": "",
		"is_over_write": 0,
		"old_object_size": 0
	}
*/
