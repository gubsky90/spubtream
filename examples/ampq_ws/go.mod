module amqp_ws

go 1.21

replace github.com/gubsky90/spubtream => ../../

require (
	github.com/gobwas/ws v1.4.0
	github.com/gubsky90/spubtream v0.0.0-00010101000000-000000000000
	github.com/mailru/easygo v0.0.0-20190618140210-3c14a0dc985f
	github.com/rabbitmq/amqp091-go v1.10.0
)

require (
	github.com/gobwas/httphead v0.1.0 // indirect
	github.com/gobwas/pool v0.2.1 // indirect
	github.com/golang-jwt/jwt/v5 v5.2.2 // indirect
	golang.org/x/sys v0.6.0 // indirect
)
