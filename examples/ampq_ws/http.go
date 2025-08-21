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

	//cert, err := tls.LoadX509KeyPair("server.crt", "server.key")
	//if err != nil {
	//	return err
	//}
	//
	//tlsConfig := &tls.Config{
	//	Certificates: []tls.Certificate{cert},
	//}

	logger.Info("Listen HTTP")
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	wg.Add(2)
	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		// TLSConfig:         tlsConfig,
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

		// _ = srv.ServeTLS(l, "", "")
	}()

	return nil
}
