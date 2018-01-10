package main

import (
	"github.com/eawsy/aws-lambda-go-core/service/lambda/runtime"
	"github.com/eawsy/aws-lambda-go-net/service/lambda/runtime/net/apigatewayproxy"
	"net/http"
	"github.com/eawsy/aws-lambda-go-net/service/lambda/runtime/net"
	"os"
	"io"
	"log"
	"strings"
	"encoding/json"
	"io/ioutil"
	"github.com/go-redis/redis"
	"fmt"
)


func HandleExtract(evt interface{}, ctx *runtime.Context) (string, error) {
	results := loadResults()
	putEndpoint := os.Getenv("apiBaseUrl")+"/recommendations"
	bytes, _  := json.Marshal(results)
	putRequest(putEndpoint, strings.NewReader(string(bytes)))
	return "Finished successfully", nil
}

// Handle is the exported handler called by AWS Lambda.
var Handle apigatewayproxy.Handler

func init() {
	ln := net.Listen()

	// Amazon API Gateway binary media types are supported out of the box.
	// If you don't send or receive binary data, you can safely set it to nil.
	Handle = apigatewayproxy.New(ln, []string{"image/png"}).Handle

	// Any Go framework complying with the Go http.Handler interface can be used.
	// This includes, but is not limited to, Vanilla Go, Gin, Echo, Gorrila, Goa, etc.
	go http.Serve(ln, http.HandlerFunc(handle))
}

func handle(w http.ResponseWriter, r *http.Request) {

	client := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("redisEndpoint")+":"+os.Getenv("redisPort"),
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	pong, err := client.Ping().Result()
	fmt.Println(pong, err)

	switch r.Method {
	case "GET":
		id := strings.TrimPrefix(r.URL.Path, "/recommendations/")
		val, err := client.Get(id).Result()
		if err != nil {
			panic(err)
		}
		w.Write([]byte(val))
	case "PUT":
		body, _ := ioutil.ReadAll(r.Body)
		results := make(map[string][]string)
		json.Unmarshal(body, &results)
		for k,v := range results {
			bytes,_ := json.Marshal(v)
			err := client.Set(k, string(bytes), 0).Err()
			if err != nil {
				println(err)
			}
		}

		w.Write([]byte(string(body)))
	}
}

func putRequest(url string, data io.Reader)  {
	client := &http.Client{}
	req, err := http.NewRequest(http.MethodPut, url, data)
	if err != nil {
		// handle error
		log.Fatal(err)
	}
	_, err = client.Do(req)
	if err != nil {
		// handle error
		log.Fatal(err)
	}


}