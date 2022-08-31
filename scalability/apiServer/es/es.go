package es

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
)

type MetaData struct {
	Name    string `json:"name"`
	Version int    `json:"version"`
	Size    int64  `json:"size"`
	Hash    string `json:"hash"`
}

type hit struct {
	Source MetaData `json:"_source"`
}

type searchResult struct {
	Hits struct {
		Total struct {
			Value    int
			Relation string
		}
		Hits []hit
	}
}

func getMetaData(name string, versionId int) (meta MetaData, err error) {
	//老版本es的url
	//url := fmt.Sprintf("http://%s/metadata/objects/%s_%d/_source", os.Getenv("ES_SERVER"), name, versionId)
	//新版本的url metadata指用户创建的索引（数据库）， _doc是7.X.X版本系统默认的，用户将name和versionId拼接成es中的id，可以通过这个id获取到对应的数据;_source去除系统元数据
	url := fmt.Sprintf("http://%s/metadata/_doc/%s_%d/_source", os.Getenv("ES_SERVER"), name, versionId)
	r, err := http.Get(url)
	if err != nil {
		log.Println(err)
		return
	}
	if r.StatusCode != http.StatusOK {
		err = fmt.Errorf("fail to get %s_%d: %d", name, versionId, r.StatusCode)
		return
	}
	res, _ := ioutil.ReadAll(r.Body)
	json.Unmarshal(res, &meta)
	return
}

func SearchLatestVersion(name string) (meta MetaData, err error) {
	//es中text类型的数据没法sort，按需要可以改为keyword类型
	url := fmt.Sprintf("http://%s/metadata/_search?q=name:%s&size=1&sort=version:desc", os.Getenv("ES_SERVER"), url.PathEscape(name))
	r, err := http.Get(url)
	if err != nil {
		log.Println(err)
		return
	}
	if r.StatusCode != http.StatusOK {
		err = fmt.Errorf("fail to search latest metadata: %d", r.StatusCode)
		return
	}
	res, _ := ioutil.ReadAll(r.Body)
	var sr searchResult
	json.Unmarshal(res, &sr)
	if len(sr.Hits.Hits) != 0 {
		meta = sr.Hits.Hits[0].Source
	}
	return
}

func GetMetaData(name string, version int) (MetaData, error) {
	if version == 0 {
		return SearchLatestVersion(name)
	}
	return getMetaData(name, version)
}

func PutMetaData(name string, version int, size int64, hash string) error {
	doc := fmt.Sprintf(`{"name":"%s","version":%d,"size":%d,"hash":"%s"}`, name, version, size, hash)
	//
	log.Println("es.PutMetaData doc: ", doc)
	client := http.Client{}
	url := fmt.Sprintf("http://%s/metadata/_doc/%s_%d?op_type=create", os.Getenv("ES_SERVER"), name, version)
	log.Println("es.PutMetaData url: ", url)
	request, err := http.NewRequest("PUT", url, strings.NewReader(doc))
	if err != nil {
		log.Println("es.PutMetaData failed, err:", err)
	}
	//手动设置Content-Type=application/json，否则es会返回406 not acceptable 错误
	request.Header["Content-Type"] = []string{"application/json"}
	r, err := client.Do(request)
	if err != nil {
		return err
	}
	if r.StatusCode == http.StatusConflict {
		return PutMetaData(name, version+1, size, hash)
	}
	/*
		if r.StatusCode == http.StatusCreated {
			res, _ := ioutil.ReadAll(r.Body)
			return fmt.Errorf("fail to put metadata: %d %s", r.StatusCode, res)
		}
	*/

	return nil
}

func AddVersion(name, hash string, size int64) error {
	meta, err := SearchLatestVersion(name)
	if err != nil {
		return err
	}
	return PutMetaData(name, meta.Version+1, size, hash)
}

func SearchAllVersion(name string, from, size int) ([]MetaData, error) {
	url := fmt.Sprintf("http://%s/metadata/_search?sort=version&from=%d&size=%d", os.Getenv("ES_SERVER"), from, size)
	if name != "" {
		url = url + "&q=name:" + name
	}
	r, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	metas := make([]MetaData, 0)
	res, _ := ioutil.ReadAll(r.Body)
	log.Println("es.SearchAllVersion res: ", res)
	var sr searchResult
	json.Unmarshal(res, &sr)
	for i := range sr.Hits.Hits {
		metas = append(metas, sr.Hits.Hits[i].Source)
	}
	return metas, nil
}
