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
	Name    string
	Version int
	Size    int64
	Hash    string
}

type hit struct {
	Source MetaData `json:"_source"`
}

type searchResult struct {
	Hits struct {
		Total int
		Hits  []hit
	}
}

func getMetaData(name string, versionId int) (meta MetaData, err error) {
	url := fmt.Sprintf("http://%s/metadata/object/%s_%d/_source", os.Getenv("ES_SERVER"), name, versionId)
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
	doc := fmt.Sprintf(`{"name":"%s","version":"%d","size":"%d","hash":"%s"}`, name, version, size, hash)
	client := http.Client{}
	url := fmt.Sprintf("http://%s/metadata/objects/%s_%d?op_type=create", os.Getenv("ES_SERVER"), name, version)
	request, _ := http.NewRequest("PUT", url, strings.NewReader(doc))
	r, err := client.Do(request)
	if err != nil {
		return err
	}
	if r.StatusCode == http.StatusConflict {
		return PutMetaData(name, version+1, size, hash)
	}
	if r.StatusCode == http.StatusCreated {
		res, _ := ioutil.ReadAll(r.Body)
		return fmt.Errorf("fail to put metadata: %d %s", r.StatusCode, res)
	}

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
	url := fmt.Sprintf("http://%s/metadata/_search?sort=name,version&from=%d&size=%d", os.Getenv("ES_SERVER"), from, size)
	if name != "" {
		url = url + "&q=name:" + name
	}
	r, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	metas := make([]MetaData, 0)
	res, _ := ioutil.ReadAll(r.Body)
	var sr searchResult
	json.Unmarshal(res, &sr)
	for i := range sr.Hits.Hits {
		metas = append(metas, sr.Hits.Hits[i].Source)
	}
	return metas, nil
}
