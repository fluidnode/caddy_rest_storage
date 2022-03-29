package rest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
)

type RestStorage struct {
	Endpoint string `json:"endpoint"`
	Token    string `json:"token"`
}

func init() {
	caddy.RegisterModule(new(RestStorage))
}

func (RestStorage) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "caddy.storage.rest",
		New: func() caddy.Module { return new(RestStorage) },
	}
}

func (r RestStorage) Validate() error {
	if r.Endpoint == "" {
		return errors.New("endpoint must be specified")
	}

	if r.Token == "" {
		return errors.New("token must be specified")
	}

	return nil
}

func (r *RestStorage) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		var value string

		key := d.Val()

		if !d.Args(&value) {
			continue
		}

		switch key {
		case "endpoint":
			r.Endpoint = value
		case "token":
			r.Token = value
		}
	}

	return nil
}

func (r *RestStorage) CertMagicStorage() (certmagic.Storage, error) {
	return r, nil
}

type LockRequest struct {
	Key   string `json:"key"`
	Token string `json:"token"`
}

func (r *RestStorage) Lock(ctx context.Context, key string) error {
	lockReq, err := json.Marshal(LockRequest{
		Key:   key,
		Token: r.Token,
	})

	if err != nil {
		return err
	}

	for {
		resp, err := http.Post(r.Endpoint+"lock", "application/json", bytes.NewBuffer(lockReq))

		if err != nil {
			return err
		}

		if resp.StatusCode == 201 {
			return nil
		}

		if resp.StatusCode != 412 {
			return errors.New("Unknown status code received")
		}
	}
}

type UnlockRequest struct {
	Key   string `json:"key"`
	Token string `json:"token"`
}

func (r *RestStorage) Unlock(key string) error {
	unlockReq, err := json.Marshal(UnlockRequest{
		Key:   key,
		Token: r.Token,
	})

	if err != nil {
		return err
	}

	resp, err := http.Post(r.Endpoint+"unlock", "application/json", bytes.NewBuffer(unlockReq))

	if err != nil {
		return err
	}

	if resp.StatusCode != 204 {
		return errors.New("Unknown status code received")
	}

	return nil
}

type StoreRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Token string `json:"token"`
}

func (r *RestStorage) Store(key string, value []byte) error {
	valueEnc := base64.StdEncoding.EncodeToString(value)

	storeReq, err := json.Marshal(StoreRequest{
		Key:   key,
		Value: valueEnc,
		Token: r.Token,
	})

	if err != nil {
		return err
	}

	resp, err := http.Post(r.Endpoint+"store", "application/json", bytes.NewBuffer(storeReq))

	if err != nil {
		return err
	}

	if resp.StatusCode != 201 {
		return errors.New("Unknown status code received")
	}

	return nil
}

type LoadRequest struct {
	Key   string `json:"key"`
	Token string `json:"token"`
}

type LoadResponse struct {
	Value string `json:"value"`
}

func (r *RestStorage) Load(key string) ([]byte, error) {
	loadReq, err := json.Marshal(LoadRequest{
		Key:   key,
		Token: r.Token,
	})

	if err != nil {
		return nil, err
	}

	resp, err := http.Post(r.Endpoint+"load", "application/json", bytes.NewBuffer(loadReq))

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, errors.New("Unknown status code received")
	}

	var loadResp LoadResponse

	err = json.NewDecoder(resp.Body).Decode(&loadResp)

	if err != nil {
		return nil, err
	}

	valueDec, err := base64.StdEncoding.DecodeString(loadResp.Value)

	if err != nil {
		return nil, err
	}

	return valueDec, nil
}

type DeleteRequest struct {
	Key   string `json:"key"`
	Token string `json:"token"`
}

func (r *RestStorage) Delete(key string) error {
	deleteReq, err := json.Marshal(DeleteRequest{
		Key:   key,
		Token: r.Token,
	})

	if err != nil {
		return err
	}

	resp, err := http.Post(r.Endpoint+"delete", "application/json", bytes.NewBuffer(deleteReq))

	if err != nil {
		return err
	}

	if resp.StatusCode != 204 {
		return errors.New("Unknown status code received")
	}

	return nil
}

type ExistsRequest struct {
	Key   string `json:"key"`
	Token string `json:"token"`
}

type ExistsResponse struct {
	Exists bool `json:"exists"`
}

func (r *RestStorage) Exists(key string) bool {
	existsReq, err := json.Marshal(ExistsRequest{
		Key:   key,
		Token: r.Token,
	})

	if err != nil {
		return false
	}

	resp, err := http.Post(r.Endpoint+"exists", "application/json", bytes.NewBuffer(existsReq))

	if err != nil {
		return false
	}

	if resp.StatusCode != 200 {
		return false
	}

	var existsResp ExistsResponse

	err = json.NewDecoder(resp.Body).Decode(&existsResp)

	if err != nil {
		return false
	}

	return existsResp.Exists
}

type ListRequest struct {
	Prefix    string `json:"prefix"`
	Recursive bool   `json:"recursive"`
	Token     string `json:"token"`
}

type ListResponse struct {
	Keys []string `json:"keys"`
}

func (r *RestStorage) List(prefix string, recursive bool) ([]string, error) {
	listReq, err := json.Marshal(ListRequest{
		Prefix:    prefix,
		Recursive: recursive,
		Token:     r.Token,
	})

	if err != nil {
		return nil, err
	}

	resp, err := http.Post(r.Endpoint+"list", "application/json", bytes.NewBuffer(listReq))

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, errors.New("Unknown status code received")
	}

	var listResp ListResponse

	err = json.NewDecoder(resp.Body).Decode(&listResp)

	if err != nil {
		return nil, err
	}

	return listResp.Keys, nil
}

type StatRequest struct {
	Key   string `json:"key"`
	Token string `json:"token"`
}

type StatResponse struct {
	Key        string `json:"key"`
	Modified   string `json:"modified"`
	Size       int64  `json:"size"`
	IsTerminal bool   `json:"isTerminal"`
}

func (r *RestStorage) Stat(key string) (certmagic.KeyInfo, error) {
	statReq, err := json.Marshal(StatRequest{
		Key:   key,
		Token: r.Token,
	})

	if err != nil {
		return certmagic.KeyInfo{}, err
	}

	resp, err := http.Post(r.Endpoint+"stat", "application/json", bytes.NewBuffer(statReq))

	if err != nil {
		return certmagic.KeyInfo{}, err
	}

	if resp.StatusCode != 200 {
		return certmagic.KeyInfo{}, errors.New("Unknown status code received")
	}

	var statResp StatResponse

	err = json.NewDecoder(resp.Body).Decode(&statResp)

	if err != nil {
		return certmagic.KeyInfo{}, err
	}

	parsedTime, err := time.Parse(time.RFC3339, statResp.Modified)

	if err != nil {
		return certmagic.KeyInfo{}, err
	}

	return certmagic.KeyInfo{
		Key:        statResp.Key,
		Modified:   parsedTime,
		Size:       statResp.Size,
		IsTerminal: statResp.IsTerminal,
	}, nil
}
