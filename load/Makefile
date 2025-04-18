.PHONY: run

run:
	go build -o spubtream . && ./spubtream

pprof:
	go tool pprof -raw spubtream http://127.0.0.1:9100/debug/pprof/profile?seconds=5 > pprof.log

web:
	go tool pprof -http http://127.0.0.1:9100