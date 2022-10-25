package main

import (
	"github.com/gorilla/mux"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
	"net/http"
)

type Manager struct {
	*shutter.Shutter

	listenAddr       string
	shouldResetState bool
}

func NewManager(listenAddr string) *Manager {
	return &Manager{
		Shutter:    shutter.New(),
		listenAddr: listenAddr,
	}
}

func (m *Manager) resetState(w http.ResponseWriter, r *http.Request) {
	zlog.Info("state will reset on next restart")
	m.shouldResetState = true

}

func (m *Manager) cancelResetState(w http.ResponseWriter, r *http.Request) {
	zlog.Info("state will *NOT* reset on next restart")
	m.shouldResetState = false
}

func (m *Manager) shutdown(w http.ResponseWriter, r *http.Request) {
	zlog.Info("shutting down consumer")
	m.Shutdown(nil)
}

func (s *Manager) Launch() {
	router := mux.NewRouter()

	coreRouter := router.PathPrefix("/").Subrouter()
	coreRouter.HandleFunc("/healthz", func(writer http.ResponseWriter, request *http.Request) {
		writer.Write([]byte("ok"))
	})
	coreRouter.HandleFunc("/resetstate", s.resetState).Methods("POST")
	coreRouter.HandleFunc("/cancelresetstate", s.cancelResetState).Methods("POST")
	coreRouter.HandleFunc("/shutdown", s.shutdown).Methods("POST")

	httpServer := &http.Server{
		Addr:    s.listenAddr,
		Handler: coreRouter,
	}
	zlog.Info("starting management api", zap.String("listing_addr", s.listenAddr))
	if err := httpServer.ListenAndServe(); err != nil {
		zlog.Warn("management server shutdown with error", zap.Error(err))
	}

}
