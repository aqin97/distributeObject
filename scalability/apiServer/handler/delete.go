package handler

import (
	"distributeObject/scalability/apiServer/es"
	"log"
	"net/http"
	"strings"
)

func del(w http.ResponseWriter, r *http.Request) {
	name := strings.Split(r.URL.EscapedPath(), "/")[2]
	meta, err := es.SearchLatestVersion(name)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	//大小为0 哈希为空 表示这是一个删除标记
	err = es.PutMetaData(name, meta.Version+1, 0, "")
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
