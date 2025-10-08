.PHONY: run

run:
	go build -o spubtream . && ./spubtream

pprof:
	go tool pprof -raw spubtream http://127.0.0.1:9100/debug/pprof/profile?seconds=5 > pprof.log

heap:
	curl -o heap.out http://127.0.0.1:9100/debug/pprof/heap?debug=1

allocs:
	curl -o allocs.out http://127.0.0.1:9100/debug/pprof/allocs?debug=1

web:
	go tool pprof -http 127.0.0.1:8080 spubtream allocs.out

some:
	go tool pprof -seconds 10 http://127.0.0.1:9100/debug/pprof/profile
	go tool pprof -seconds 10 http://127.0.0.1:9100/debug/pprof/block
	go tool pprof -seconds 5 http://127.0.0.1:9100/debug/pprof/allocs
	go tool pprof http://127.0.0.1:9100/debug/pprof/mutex

trace:
	curl -o trace.out http://127.0.0.1:9100/debug/pprof/trace?seconds=1
	go tool trace trace.out