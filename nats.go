package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nats-io/go-nats"
	"github.com/untillpro/gochips"
	"github.com/untillpro/godif"
	"github.com/untillpro/igoonce/iconfig"
	"github.com/untillpro/igoonce/iqueues"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func Declare() {
	godif.Provide(&iqueues.InvokeFromHTTPRequest, invokeFromHTTPRequest)
	godif.Provide(&iqueues.PartitionHandlerFactories, partitionHandlerFactories)
	godif.Provide(&iqueues.NonPartyHandlers, nonPartyHandlers)

	godif.Require(&iconfig.GetCurrentAppConfig)
	//TODO maybe we need PutCurrentAppConfig
}

var partitionHandlerFactories = make(map[string]iqueues.PartitionHandlerFactory)
var nonPartyHandlers = make(map[string]iqueues.NonPartyHandler)

//TODO sync
var partitionHandlers map[string]iqueues.PartitionHandler

type NatsConfig struct {
	Servers string
}

type natsWorker struct {
	conf *NatsConfig
	conn *nats.Conn
}

type natsSubscriber struct {
	queueID          string
	numOfPartition   int
	partitionsNumber int
	worker           *natsWorker
}

type natsQueueKey int

const (
	natsPublish   natsQueueKey = 0
	natsSubscribe natsQueueKey = 1
)

func connectPublisher(conf *NatsConfig) (worker *natsWorker, err error) {
	return connectToNats(conf, "NatsPublisher")
}

func connectSubscribers(conf *NatsConfig) (subscribers []*natsSubscriber, err error) {
	subscribers = make([]*natsSubscriber, 0)
	for k := range nonPartyHandlers {
		queueAndPartition := strings.Split(k, ":")
		queueName := queueAndPartition[0]
		worker, err := connectToNats(conf, queueName)
		log.Println("sub", queueName, "connected to nats")
		if err != nil {
			return nil, err
		}
		subscriber := &natsSubscriber{k, 0, 0, worker}
		subscribers = append(subscribers, subscriber)
	}
	for k := range partitionHandlerFactories {
		queueAndPartition := strings.Split(k, ":")
		queueName := queueAndPartition[0]
		numOfPartitions, err := strconv.Atoi(queueAndPartition[1])
		if err != nil {
			return nil, err
		}
		for i := 0; i < numOfPartitions; i++ {
			worker, err := connectToNats(conf, queueName+strconv.Itoa(i))
			if err != nil {
				return nil, err
			}
			subscribers = append(subscribers, &natsSubscriber{k, i, numOfPartitions, worker})
		}
	}
	return subscribers, nil
}

func connectToNats(conf *NatsConfig, connectionName string) (worker *natsWorker, err error) {
	opts := setupConnOptions([]nats.Option{nats.Name(connectionName)})
	opts = setupConnOptions(opts)
	natsConn, err := nats.Connect(conf.Servers, opts...)
	if err != nil {
		return nil, err
	}
	worker = &natsWorker{conf, natsConn}
	return worker, nil
}

func Init(ctx context.Context) (context.Context, error) {
	var config NatsConfig
	err := iconfig.GetCurrentAppConfig(ctx, &config)
	if err != nil {
		return nil, err
	}
	workers, err := connectSubscribers(&config)
	if err != nil {
		//TODO don't forget to release resources
		return nil, err
	}
	ctx = context.WithValue(ctx, natsSubscribe, workers)
	worker, err := connectPublisher(&config)
	if err != nil {
		//TODO don't forget to release resources
		return nil, err
	}
	return context.WithValue(ctx, natsPublish, worker), nil
}

func (v *natsSubscriber) createNatsNonPartyHandler(ctx context.Context,
	handler func(ctx context.Context, req *iqueues.Request) *iqueues.Response) nats.MsgHandler {
	nc := v.worker.conn
	return func(msg *nats.Msg) {
		var req iqueues.Request
		err := json.Unmarshal(msg.Data, &req)
		if err != nil {
			//do smth with error
			log.Fatal(err)
		}
		resp := handler(ctx, &req)
		data, err := json.Marshal(resp)
		if err != nil {
			//do smth with error
			log.Fatal(err)
		}
		nc.Publish(msg.Reply, data)
	}
}

func (v *natsSubscriber) createNatsPartitionedHandler(ctx context.Context, handlerFactory iqueues.PartitionHandlerFactory) nats.MsgHandler {
	nc := v.worker.conn
	return func(msg *nats.Msg) {
		var req iqueues.Request
		err := json.Unmarshal(msg.Data, &req)
		if err != nil {
			//do smth with error
			log.Fatal(err)
		}
		var handler iqueues.PartitionHandler
		var errResp *iqueues.Response
		handler, ok := partitionHandlers[v.queueID+strconv.Itoa(v.numOfPartition)]
		if !ok {
			handler, errResp = handlerFactory(ctx, req.QueueID, req.PartitionDividend, req.PartitionNumber)
		}
		if errResp != nil {
			//do smth with error
			log.Fatal(errResp)
		}
		resp := handler.Handle(ctx, &req)
		data, err := json.Marshal(resp)
		if err != nil {
			//do smth with error
			log.Fatal(err)
		}
		nc.Publish(msg.Reply, data)
	}
}

func Start(ctx context.Context) {
	subscribers := ctx.Value(natsSubscribe).([]*natsSubscriber)
	for _, v := range subscribers {
		var natsHandler nats.MsgHandler
		if v.partitionsNumber == 0 {
			handler := nonPartyHandlers[v.queueID]
			natsHandler = v.createNatsNonPartyHandler(ctx, handler)
		} else {
			factory := partitionHandlerFactories[v.queueID+strconv.Itoa(v.numOfPartition)]
			natsHandler = v.createNatsPartitionedHandler(ctx, factory)
		}
		err := v.worker.subscribe(natsHandler)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (w *natsWorker) subscribe(handler nats.MsgHandler) error {
	conn := w.conn
	_, err := conn.Subscribe(conn.Opts.Name, handler)
	if err != nil {
		//TODO no need to stop all, just one subscribtion
		log.Println("problem in subs")
		return err
	}
	err = conn.Flush()
	if err != nil {
		//TODO no need to stop all, just one subscribtion
		log.Println("problem in subs with flush")
		return err
	}

	if err = conn.LastError(); err != nil {
		//TODO no need to stop all, just one subscribtion
		return err
	}
	return nil
}

//TODO unsubscribe
func Stop(ctx context.Context) {

}

//TODO close connection to nats
// should we stop the context?
func Finit(ctx context.Context) error {
	return nil
}

func invokeFromHTTPRequest(ctx context.Context, request *iqueues.Request, response http.ResponseWriter, timeout time.Duration) {
	worker := ctx.Value(natsPublish).(*natsWorker)
	reqData, err := json.Marshal(request)
	if err != nil {
		http.Error(response, "can't marshal request body", http.StatusBadRequest)
		return
	}
	queueNameAndPartitionNumber := strings.Split(request.QueueID, ":")
	if len(queueNameAndPartitionNumber) == 0 {
		http.Error(response, "wrong QueueID", http.StatusBadRequest)
		return
	}
	numOfPartitions, err := strconv.Atoi(queueNameAndPartitionNumber[1])
	if err != nil {
		http.Error(response, "wrong QueueID", http.StatusBadRequest)
		return
	}
	gochips.Info(fmt.Sprintf("Queue name: %s; number of partitions: %d", queueNameAndPartitionNumber[0], numOfPartitions))
	var resp iqueues.Response
	if numOfPartitions == 0 {
		resp = worker.reqRespNats(reqData, request.QueueID, timeout)
	} else {
		resp = worker.reqRespNats(reqData, queueNameAndPartitionNumber[0]+strconv.Itoa(request.PartitionNumber), timeout)
	}
	if resp.StatusCode != 200 {
		http.Error(response, resp.Status, resp.StatusCode)
		return
	}
	respData, err := json.Marshal(resp)
	if err != nil {
		http.Error(response, "can't marshal request body", http.StatusBadRequest)
		return
	}
	_, err = fmt.Fprint(response, respData)
	if err != nil {
		http.Error(response, "can't write response", http.StatusBadRequest)
	}
	log.Println("resp sent")
}

func (w *natsWorker) reqRespNats(data []byte, partitionKey string, timeout time.Duration) iqueues.Response {
	conn := w.conn
	msg, err := conn.Request(partitionKey, data, timeout)
	if err != nil {
		log.Println(err)
		resp := createResponse(http.StatusRequestTimeout, "request timeout")
		if conn.LastError() != nil {
			resp.Data = conn.LastError().Error()
		}
		return resp
	}
	return createResponse(http.StatusOK, msg.Data)
}

var airBoView iqueues.NonPartyHandler = func(ctx context.Context, request *iqueues.Request) *iqueues.Response {
	return &iqueues.Response{
		Status:     http.StatusText(http.StatusOK),
		StatusCode: http.StatusOK,
		Data:       fmt.Sprintf("Received message from %s with args: %s", request.QueueID, request.Args),
	}
}

//TODO here we can create new connection for PartitionHandler, try to connect, if success return handler, if failure
// return error *Response
// i should start connections here? Handler structs for partitionDivident num?
var factory iqueues.PartitionHandlerFactory = func(ctx context.Context, queueID string, partitionDividend int,
	partitionNumber int) (handler iqueues.PartitionHandler, err *iqueues.Response) {
	return nil, &iqueues.Response{}
}

func createResponse(HTTPStatusCode int, data interface{}) iqueues.Response {
	return iqueues.Response{
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
