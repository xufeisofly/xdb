package main

import (
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/sirupsen/logrus"
	"github.com/xufeisofly/xdb/memtable"
	"github.com/xufeisofly/xdb/server"
)

var mtable = memtable.New()

func main() {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)

	r.Get("/get", server.Get)
	r.Post("/set", server.Set)

	logrus.Infof("starting server")
	http.ListenAndServe(":8080", r)
}
