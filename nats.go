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
	"sync"
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
var partitionHandlers sync.Map

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
	subscription     *nats.Subscription
}

type natsQueueKey int

const (
	natsPublish   natsQueueKey = 0
	natsSubscribe natsQueueKey = 1
)

func connectPublisher(conf *NatsConfig) (worker *natsWorker, err error) {
	return connectToNats(conf, "NatsPublisher")
}

//TODO do we have to return from method if one subscriber fails?
func connectSubscribers(conf *NatsConfig) (subscribers []*natsSubscriber, err error) {
	subscribers = make([]*natsSubscriber, 0)
	for k := range nonPartyHandlers {
		queueAndPartition := strings.Split(k, ":")
		queueName := queueAndPartition[0]
		worker, err := connectToNats(conf, queueName)
		if err != nil {
			return subscribers, err
		}
		subscribers = append(subscribers, &natsSubscriber{k, 0, 0, worker, nil})
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
				return subscribers, err
			}
			subscribers = append(subscribers, &natsSubscriber{k, i, numOfPartitions, worker, nil})
		}
	}
	return subscribers, nil
}

func unsubscribe(subscribers []*natsSubscriber) {
	for _, s := range subscribers {
		s.subscription.Unsubscribe()
	}
}

func disconnectSubscribers(subscribers []*natsSubscriber) {
	for _, s := range subscribers {
		s.worker.conn.Close()
	}
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

//TODO do we have to return nil ctx?
func Init(ctx context.Context) (context.Context, error) {
	var config NatsConfig
	err := iconfig.GetCurrentAppConfig(ctx, &config)
	if err != nil {
		return nil, err
	}
	subs, err := connectSubscribers(&config)
	if err != nil {
		disconnectSubscribers(subs)
		return nil, err
	}
	ctx = context.WithValue(ctx, natsSubscribe, subs)
	pub, err := connectPublisher(&config)
	if err != nil {
		disconnectSubscribers(subs)
		return nil, err
	}
	return context.WithValue(ctx, natsPublish, pub), nil
}

//TODO do smth with errors
func (ns *natsSubscriber) createNatsNonPartyHandler(ctx context.Context,
	handler func(ctx context.Context, req *iqueues.Request) *iqueues.Response) nats.MsgHandler {
	nc := ns.worker.conn
	return func(msg *nats.Msg) {
		var req iqueues.Request
		err := json.Unmarshal(msg.Data, &req)
		if err != nil {
			gochips.Info(err)
			return
		}
		resp := handler(ctx, &req)
		data, err := json.Marshal(resp)
		if err != nil {
			gochips.Info(err)
			return
		}
		nc.Publish(msg.Reply, data)
	}
}

//TODO do smth with errors
func (ns *natsSubscriber) createNatsPartitionedHandler(ctx context.Context, handlerFactory iqueues.PartitionHandlerFactory) nats.MsgHandler {
	nc := ns.worker.conn
	return func(msg *nats.Msg) {
		var req iqueues.Request
		err := json.Unmarshal(msg.Data, &req)
		if err != nil {
			gochips.Info(err)
			return
		}
		var errResp *iqueues.Response
		key := ns.queueID + strconv.Itoa(ns.numOfPartition)
		handler, ok := partitionHandlers.Load(key)
		if !ok {
			log.Println("Create handler for partition number", ns.numOfPartition)
			handler, errResp = handlerFactory(ctx, req.QueueID, req.PartitionDividend, req.PartitionNumber)
			partitionHandlers.LoadOrStore(key, handler)
		}
		if errResp != nil {
			gochips.Info(err)
			return
		}
		resp := handler.(iqueues.PartitionHandler).Handle(ctx, &req)
		data, err := json.Marshal(resp)
		if err != nil {
			gochips.Info(err)
			return
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
			factory := partitionHandlerFactories[v.queueID]
			natsHandler = v.createNatsPartitionedHandler(ctx, factory)
		}
		err := v.subscribe(natsHandler)
		if err != nil {
			v.worker.conn.Close()
			gochips.Info(err)
		}
	}
}

func (ns *natsSubscriber) subscribe(handler nats.MsgHandler) (err error) {
	conn := ns.worker.conn
	ns.subscription, err = conn.Subscribe(conn.Opts.Name, handler)
	err = conn.Flush()
	if err = conn.LastError(); err != nil {
		return err
	}
	log.Println("Subscribe for subj", conn.Opts.Name)
	return nil
}

func Stop(ctx context.Context) {
	subscribers := ctx.Value(natsSubscribe).([]*natsSubscriber)
	unsubscribe(subscribers)
}

//TODO close connection to nats
// should we stop the context?
func Finit(ctx context.Context) {
	subscribers := ctx.Value(natsSubscribe).([]*natsSubscriber)
	disconnectSubscribers(subscribers)
	publisher := ctx.Value(natsPublish).(*natsWorker)
	publisher.conn.Close()
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
	var respFromNats []byte
	if numOfPartitions == 0 {
		respFromNats, err = worker.reqRespNats(reqData, queueNameAndPartitionNumber[0], timeout)
	} else {
		respFromNats, err = worker.reqRespNats(reqData, queueNameAndPartitionNumber[0]+strconv.Itoa(request.PartitionNumber), timeout)
	}
	if err != nil {
		http.Error(response, "error from nats resp", http.StatusBadRequest)
	}
	_, err = fmt.Fprint(response, string(respFromNats))
	if err != nil {
		http.Error(response, "can't write response", http.StatusBadRequest)
	}
}

func (w *natsWorker) reqRespNats(data []byte, partitionKey string, timeout time.Duration) (resp []byte, err error) {
	conn := w.conn
	msg, err := conn.Request(partitionKey, data, timeout)
	if err != nil {
		if conn.LastError() != nil {
			err = conn.LastError()
		}
		return nil, err
	}
	return msg.Data, nil
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
