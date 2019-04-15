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
	"hash/fnv"
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

var partitionHandlerFactories map[string]iqueues.PartitionHandlerFactory
var nonPartyHandlers map[string]iqueues.NonPartyHandler
var partitionHandlers map[string]iqueues.PartitionHandler

type NatsConfig struct {
	Servers string
}

type natsWorker struct {
	conf *NatsConfig
	conn *nats.Conn
}

type natsSubscriber struct {
	queueID           string
	partitionDividend int
	numOfPartition    int
	partitionsNumber  int
	worker            *natsWorker
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
		if err != nil {
			return nil, err
		}
		subscriber := &natsSubscriber{k, 0, 0, 0, worker}
		subscribers = append(subscribers, subscriber)
	}
	for k := range partitionHandlerFactories {
		queueAndPartition := strings.Split(k, ":")
		queueName := queueAndPartition[0]
		numOfPartitions, err := strconv.Atoi(queueAndPartition[1])
		if err != nil {
			return nil, err
		}
		partitionDividend, _, err := calculatePartitionNumber(k, numOfPartitions)
		if err != nil {
			return nil, err
		}
		for i := 0; i < numOfPartitions; i++ {
			worker, err := connectToNats(conf, fmt.Sprintf("%s_%d", queueName, i))
			if err != nil {
				return nil, err
			}
			subscribers = append(subscribers, &natsSubscriber{k, partitionDividend, i, numOfPartitions, worker})
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

func (v *natsSubscriber) createNatsHandler(handler interface{}) nats.MsgHandler {
	nc := v.worker.conn
	return func(msg *nats.Msg) {
		req := iqueues.Request{
			QueueID:           msg.Subject,
			PartitionNumber:   v.numOfPartition,
			PartitionDividend: v.partitionDividend,
			Args:              msg.Data,
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

func Start(ctx context.Context) {
	subscribers := ctx.Value(natsSubscribe).([]*natsSubscriber)
	for _, v := range subscribers {
		var natsHandler nats.MsgHandler
		if v.partitionsNumber == 0 {
			nc := v.worker.conn
			handler := nonPartyHandlers[v.queueID]
			natsHandler = func(msg *nats.Msg) {
				req := iqueues.Request{
					QueueID: msg.Subject,
					Args:    msg.Data,
				}
				resp := handler(ctx, &req)
				data, err := json.Marshal(resp)
				if err != nil {
					//do smth with error
					log.Fatal(err)
				}
				nc.Publish(msg.Reply, data)
			}
		} else {
			if val, ok := partitionHandlers[fmt.Sprintf("%s_%d", v.queueID, v.numOfPartition)]; ok {
				val.Handle(ctx)
			} else {

			}
			handlerFactory := v.queueID
			natsHandler = func(msg *nats.Msg) {
				req := iqueues.Request{
					QueueID: msg.Subject,
					Args:    msg.Data,
				}
				resp := handlerFactory(ctx)
				data, err := json.Marshal(resp)
				if err != nil {
					//do smth with error
					log.Fatal(err)
				}
				worker.conn.Publish(msg.Reply, data)
			}
		}
	}
}

func (w *natsWorker) subscribe(handler nats.MsgHandler) error {
	conn := w.conn
	_, err := conn.Subscribe(conn.Opts.Name, handler)
	err = conn.Flush()
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
	worker := ctx.Value(natsPublish).(natsWorker)
	HTTPRequestBody := request.Args.([]byte)
	queueNameAndPartitionNumber := strings.Split(request.QueueID, ":")
	if len(queueNameAndPartitionNumber) == 0 {
		http.Error(response, "wrong QueueID", http.StatusBadRequest)
	}
	numOfPartitions, err := strconv.Atoi(queueNameAndPartitionNumber[1])
	if err != nil {
		http.Error(response, "wrong QueueID", http.StatusBadRequest)
	}
	gochips.Info(fmt.Sprintf("Queue name: %s; number of partitions: %d", queueNameAndPartitionNumber[0], numOfPartitions))
	var resp *iqueues.Response
	if numOfPartitions == 0 {
		resp = worker.reqRespNats(HTTPRequestBody, request.QueueID, timeout)
	} else {
		request.PartitionNumber, request.PartitionDividend, err = calculatePartitionNumber(request.QueueID, numOfPartitions)
		resp = worker.reqRespNats(HTTPRequestBody, queueNameAndPartitionNumber[0]+strconv.Itoa(request.PartitionNumber), timeout)
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
	partitionNumber int) (handler *iqueues.PartitionHandler, err *iqueues.Response) {
	return nil, &iqueues.Response{}
}

func calculatePartitionNumber(queueID string, numOfPartitions int) (partitionDividend int, partitionNumber int, err error) {
	h := fnv.New32a()
	_, err = h.Write([]byte(queueID))
	if err != nil {
		return 0, 0, err
	}
	partitionDividend = int(h.Sum32())
	return partitionDividend, partitionDividend % numOfPartitions, nil
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
