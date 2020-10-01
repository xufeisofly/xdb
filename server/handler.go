package server

import (
	"net/http"
	"time"

	"github.com/go-chi/render"
	"github.com/xufeisofly/xdb/memtable"
)

var memT *memtable.Memtable

func init() {
	memT = memtable.New()
}

func Get(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		return
	}
	v, _ := memT.Find(key)
	w.Write([]byte(v))
}

type SetParam struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (p *SetParam) Bind(r *http.Request) error {
	return nil
}

type SetResponse struct {
	Success bool `json:"success"`
}

func (res *SetResponse) Render(w http.ResponseWriter, r *http.Request) error {
	return nil
}

func Set(w http.ResponseWriter, r *http.Request) {
	data := &SetParam{}
	if err := render.Bind(r, data); err != nil {
		render.Render(w, r, &SetResponse{Success: false})
		return
	}

	memT.Insert(data.Key, data.Value, time.Now().Unix())
	render.Render(w, r, &SetResponse{Success: true})
	return
}
