module github.com/untillpro/goonce-queues-nats

go 1.12

require (
	github.com/nats-io/go-nats v1.7.0
	github.com/nats-io/nkeys v0.0.2 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/untillpro/gochips v1.6.0
	github.com/untillpro/godif v0.2.0
	github.com/untillpro/igoonce v0.7.0
)

replace github.com/untillpro/igoonce => ../fork/igoonce
