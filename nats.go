package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nats-io/go-nats"
	"github.com/untillpro/godif"
	"github.com/untillpro/igoonce/iconfig"
	"github.com/untillpro/igoonce/iqueues"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
)

func Declare() {
	godif.Provide(&iqueues.InvokeFromHTTPRequest, invokeFromHTTPRequest)
	godif.Provide(&iqueues.PartitionHandlerFactories, partitionHandlerFactories)
	godif.Provide(&iqueues.NonPartyHandlers, nonPartyHandlers)

	godif.Require(&iconfig.GetCurrentAppConfig)
	//TODO maybe we need PutCurrentAppConfig
}

var partitionHandlerFactories map[string]iqueues.PartitionHandlerFactory
var nonPartyHandlers map[string]iqueues.NonPartyHandler

type NatsConfig struct {
	Servers string
}

type natsWorker struct {
	conf *NatsConfig
	conn *nats.Conn
}

type natsQueueKey int

const (
	natsConnection natsQueueKey = 0
)

func Init(ctx context.Context) (context.Context, error) {
	var config NatsConfig
	err := iconfig.GetCurrentAppConfig(ctx, &config)
	if err != nil {
		return nil, err
	}
	opts := []nats.Option{nats.Name("NatsQueues")}
	natsConn, err := nats.Connect(config.Servers, opts...)
	if err != nil {
		return nil, err
	}
	worker := natsWorker{&config, natsConn}
	return context.WithValue(ctx, natsConnection, worker), nil
}

//TODO recreate http.Request to our Request with Args=*http.Request or pass partitionKey to method or read mux.Vars() (gorilla-mux specific)
// or parse URL here (bad) or pass map[string]string with params from router (not library-specific), but we need to know key names
func invokeFromHTTPRequest(ctx context.Context, request *iqueues.Request, response http.ResponseWriter, timeout time.Duration) {
	worker := ctx.Value(natsConnection).(natsWorker)
	HTTPRequest := request.Args.(*http.Request)
	body, err := ioutil.ReadAll(HTTPRequest.Body)
	if err != nil {
		http.Error(response, "can't read request body: "+string(body), http.StatusBadRequest)
		return
	}
	var resp *iqueues.Response
	if request.PartitionDividend == 0 {
		resp = worker.reqRespNats(body, request.QueueID, timeout)
	} else {
		resp = worker.reqRespNats(body, request.QueueID+strconv.Itoa(request.PartitionNumber), timeout)
	}
	data := resp.Data.([]byte)
	_, err = fmt.Fprint(response, data)
	if err != nil {
		http.Error(response, "can't write response", http.StatusBadRequest)
	}
}

func (w *natsWorker) reqRespNats(data interface{}, partitionKey string, timeout time.Duration) *iqueues.Response {
	conn := w.conn
	var resp iqueues.Response
	var body []byte
	var err error
	switch data.(type) {
	case []byte:
		body = data.([]byte)
	default:
		body, err = json.Marshal(data)
		if err != nil {
			resp.Status = http.StatusText(http.StatusBadRequest)
			resp.StatusCode = http.StatusBadRequest
			resp.Data = "Can't marshal request body!"
			return &resp
		}
	}
	msg, err := conn.Request(partitionKey, body, timeout)
	if err != nil {
		resp.Status = http.StatusText(http.StatusRequestTimeout)
		resp.StatusCode = http.StatusRequestTimeout
		if conn.LastError() != nil {
			resp.Data = conn.LastError().Error()
		}
		return &resp
	}
	resp.Status = http.StatusText(http.StatusOK)
	resp.StatusCode = http.StatusOK
	resp.Data = msg.Data
	return &resp
}

var airBoView iqueues.NonPartyHandler = func(ctx context.Context, queueID string, request *iqueues.Request) *iqueues.Response {

}

//TODO nats reducer stuff with this maps
