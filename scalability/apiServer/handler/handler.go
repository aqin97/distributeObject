package handler

import "net/http"

func Handler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		get(w, r)
		return
	}
	if r.Method == http.MethodPut {
		put(w, r)
		return
	}
	if r.Method == http.MethodDelete {
		del(w, r)
		return
	}
	w.WriteHeader(http.StatusInternalServerError)
}
