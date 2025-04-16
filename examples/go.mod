module github.com/gubsky90/spubtream/examples

go 1.22.2

replace github.com/gubsky90/spubtream => ../

require (
	github.com/gubsky90/spubtream v0.0.0-00010101000000-000000000000
	github.com/rabbitmq/amqp091-go v1.10.0
	github.com/redis/go-redis/v9 v9.7.3
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
)
