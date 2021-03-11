package controllers

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/k8-proxy/k8-go-api/models"
	"github.com/k8-proxy/k8-go-api/pkg/store"
	"github.com/k8-proxy/k8-go-api/utils"

	"github.com/k8-proxy/k8-go-api/pkg/message"
	"github.com/rs/zerolog"
)

// RebuildFile rebuilds a file using its binary data
func RebuildFile(w http.ResponseWriter, r *http.Request) {

	// max 6 MB file size
	r.ParseMultipartForm(6 << 20)

	cont := r.PostFormValue("contentManagementFlagJson")
	var mp map[string]json.RawMessage
	err := json.Unmarshal([]byte(cont), &mp)
	if err != nil {
		log.Println("unmarshal json:", err)
		utils.ResponseWithError(w, http.StatusBadRequest, "malformed json format")
		return
	}

	file, handler, err := r.FormFile("file")
	if err != nil {
		log.Println("formfile", err)
		utils.ResponseWithError(w, http.StatusBadRequest, "File is required")
		return
	}
	defer file.Close()

	buf, err := ioutil.ReadAll(file)
	if err != nil {
		log.Println("ioutilReadAll", err)
		utils.ResponseWithError(w, http.StatusBadRequest, "file not found")
		return
	}

	/////////////////////////////
	// this experemental  , it connect to a translating service process

	timer := time.Now()
	url, err := store.St(buf, "originalpdf")
	if err != nil {
		log.Println(err)
		utils.ResponseWithError(w, http.StatusInternalServerError, "StatusInternalServerError")
		return

	}

	stsince := time.Since(timer)

	reqid := r.Header.Get("Request-Id")

	timer = time.Now()
	miniourl, err := message.AmqpM(reqid, url)
	if err != nil {
		log.Println(err)
		utils.ResponseWithError(w, http.StatusInternalServerError, "StatusInternalServerError")
		return
	}
	mqsince := time.Since(timer)

	timer = time.Now()
	buf2, err := store.Getfile(miniourl)
	if err != nil {
		log.Println(err)
		return
	}
	gfsince := time.Since(timer)

	/////////////////////////
	//GW custom header
	utils.AddGWHeader(w, models.Temp)

	_, e := w.Write(buf2)
	if e != nil {
		log.Println(e)
		return
	}

	logf := zerolog.Ctx(r.Context())
	logf.UpdateContext(func(c zerolog.Context) zerolog.Context {
		return c.Str("Filename", handler.Filename).
			Int64("Filesize", handler.Size).
			Str("Content-Type", handler.Header.Get("Content-Type")).
			Dur("mqduration", mqsince).Dur("minio duration", stsince).Dur("getfil duration", gfsince)

	})
}
