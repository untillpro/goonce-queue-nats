package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nats-io/go-nats"
	"github.com/untillpro/godif"
	"github.com/untillpro/igoonce/iconfig"
	"github.com/untillpro/igoonce/iqueues"
	"hash/fnv"
	"io/ioutil"
	"log"
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
	opts := setupConnOptions([]nats.Option{})
	natsConn, err := nats.Connect(config.Servers, opts...)
	if err != nil {
		return nil, err
	}
	worker := natsWorker{&config, natsConn}
	return context.WithValue(ctx, natsConnection, worker), nil
}

//TODO close connection to nats
// should we stop the context?
func Finit(ctx context.Context) error {
	return nil
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
		request.PartitionNumber, err = calculatePartitionNumber(request.QueueID, request.PartitionDividend)
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
	var body []byte
	var err error
	switch data.(type) {
	case []byte:
		body = data.([]byte)
	default:
		body, err = json.Marshal(data)
		if err != nil {
			return createResponse(http.StatusBadRequest, "can't marshal request body")
		}
	}
	msg, err := conn.Request(partitionKey, body, timeout)
	if err != nil {
		resp := createResponse(http.StatusRequestTimeout, "request timeout")
		if conn.LastError() != nil {
			resp.Data = conn.LastError().Error()
		}
		return resp
	}
	return createResponse(http.StatusOK, msg.Data)
}

func InitConnection(ctx context.Context, partitionName string) *nats.Conn {

}

//TODO no need to return *Response and pass *Request
// need to create one conn for each handler and reuse it
// we need Handler interface and structs with *nats.Conn
var airBoView iqueues.NonPartyHandler = func(ctx context.Context, queueID string, request *iqueues.Request) *iqueues.Response {
	worker := ctx.Value(natsConnection).(natsWorker)
	conn := worker.conn
	_, err := conn.Subscribe(queueID, func(msg *nats.Msg) {
		//no need to check errors, we just got timeout request in publisher
		// maybe we need it later
		conn.Publish(msg.Reply, []byte("airBoView"))
	})
	err = conn.Flush()

	if err = conn.LastError(); err != nil {
		//TODO need to log it and maybe try to reconnect
		log.Fatal(err)
	}

	log.Printf("Subscriber for subject %s created and waits for request", queueID)
	return nil
}

//TODO here we can create new connection for PartitionHandler, try to connect, if success return handler, if failure
// return error *Response
// i should start connections here? Handler structs for partitionDivident num?
var factory iqueues.PartitionHandlerFactory = func(ctx context.Context, queueID string, partitionDividend int,
	partitionNumber int) (handler *iqueues.PartitionHandler, err *iqueues.Response) {

	return nil, &iqueues.Response{}
}

func calculatePartitionNumber(queueID string, partitionDividend int) (int, error) {
	h := fnv.New32a()
	_, err := h.Write([]byte(queueID))
	if err != nil {
		return 0, err
	}
	return int(h.Sum32()) % partitionDividend, nil
}

func createResponse(HTTPStatusCode int, data interface{}) *iqueues.Response {
	return &iqueues.Response{
		Status:     http.StatusText(HTTPStatusCode),
		StatusCode: HTTPStatusCode,
		Data:       data,
	}
}

//TODO refactor
func setupConnOptions(opts []nats.Option) []nats.Option {
	totalWait := 10 * time.Minute
	reconnectDelay := time.Second

	opts = append(opts, nats.ReconnectWait(reconnectDelay))
	opts = append(opts, nats.MaxReconnects(int(totalWait/reconnectDelay)))
	opts = append(opts, nats.DisconnectHandler(func(nc *nats.Conn) {
		log.Printf("Disconnected: will attempt reconnects for %.0fm", totalWait.Minutes())
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		log.Printf("Reconnected [%s]", nc.ConnectedUrl())
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		log.Fatal("Exiting, no servers available")
	}))
	return opts
}
