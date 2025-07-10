package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

func ListenAndServe(ctx context.Context, wg *sync.WaitGroup, addr string, handler http.Handler) error {
	logger := slog.With("addr", addr)

	logger.Info("Listen HTTP")
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	wg.Add(2)
	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		defer wg.Done()
		<-ctx.Done()
		logger.Info("Wait requests")
		_ = srv.Shutdown(context.Background())
		logger.Info("Shutdown HTTP")
	}()
	go func() {
		defer wg.Done()
		_ = srv.Serve(l)
	}()

	return nil
}
