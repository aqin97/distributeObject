package utils

import (
	"crypto/sha256"
	"encoding/base64"
	"io"
	"log"
	"net/http"
	"strconv"
)

func GetHashFromHeader(h http.Header) string {
	digest := h.Get("Digest")
	if len(digest) < 9 {
		return ""
	}
	if digest[:8] != "SHA-256=" {
		return ""
	}
	return digest[8:]
}

func GetSizeFromHeader(h http.Header) int64 {
	size, _ := strconv.ParseInt(h.Get("content-length"), 0, 64)
	return size
}

func CalculateHash(r io.Reader) string {
	h := sha256.New()
	n, err := io.Copy(h, r)
	if err != nil {
		log.Println("calculateHash copy err:", err, n)
	}
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}
