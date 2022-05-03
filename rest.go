package rest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
)

type RestStorage struct {
	Endpoint string `json:"endpoint"`
	Token    string `json:"token"`
	client   *http.Client
}

func init() {
	caddy.RegisterModule(RestStorage{
		client: &http.Client{},
	})
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
		req, err := http.NewRequestWithContext(ctx, "POST", r.Endpoint+"lock", bytes.NewBuffer(lockReq))

		if err != nil {
			return err
		}

		req.Header.Add("Content-Type", "application/json")
		resp, err := r.client.Do(req)

		if err != nil {
			return err
		}

		resp.Body.Close()

		if resp.StatusCode == 201 {
			return nil
		}

		if resp.StatusCode != 412 {
			return fmt.Errorf("Unknown status code received: {}", resp.StatusCode)
		}
	}
}

type UnlockRequest struct {
	Key   string `json:"key"`
	Token string `json:"token"`
}

func (r *RestStorage) Unlock(ctx context.Context, key string) error {
	unlockReq, err := json.Marshal(UnlockRequest{
		Key:   key,
		Token: r.Token,
	})

	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", r.Endpoint+"unlock", bytes.NewBuffer(unlockReq))

	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")
	resp, err := r.client.Do(req)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 204 {
		return fmt.Errorf("Unknown status code received: {}", resp.StatusCode)
	}

	return nil
}

type StoreRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Token string `json:"token"`
}

func (r *RestStorage) Store(ctx context.Context, key string, value []byte) error {
	valueEnc := base64.StdEncoding.EncodeToString(value)

	storeReq, err := json.Marshal(StoreRequest{
		Key:   key,
		Value: valueEnc,
		Token: r.Token,
	})

	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", r.Endpoint+"store", bytes.NewBuffer(storeReq))

	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")
	resp, err := r.client.Do(req)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		return fmt.Errorf("Unknown status code received: {}", resp.StatusCode)
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

func (r *RestStorage) Load(ctx context.Context, key string) ([]byte, error) {
	loadReq, err := json.Marshal(LoadRequest{
		Key:   key,
		Token: r.Token,
	})

	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", r.Endpoint+"load", bytes.NewBuffer(loadReq))

	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/json")
	resp, err := r.client.Do(req)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return nil, fs.ErrNotExist
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Unknown status code received: {}", resp.StatusCode)
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

func (r *RestStorage) Delete(ctx context.Context, key string) error {
	deleteReq, err := json.Marshal(DeleteRequest{
		Key:   key,
		Token: r.Token,
	})

	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", r.Endpoint+"delete", bytes.NewBuffer(deleteReq))

	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")
	resp, err := r.client.Do(req)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return fs.ErrNotExist
	}

	if resp.StatusCode != 204 {
		return fmt.Errorf("Unknown status code received: {}", resp.StatusCode)
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

func (r *RestStorage) Exists(ctx context.Context, key string) bool {
	existsReq, err := json.Marshal(ExistsRequest{
		Key:   key,
		Token: r.Token,
	})

	if err != nil {
		return false
	}

	req, err := http.NewRequestWithContext(ctx, "POST", r.Endpoint+"exists", bytes.NewBuffer(existsReq))

	if err != nil {
		return false
	}

	req.Header.Add("Content-Type", "application/json")
	resp, err := r.client.Do(req)

	if err != nil {
		return false
	}

	defer resp.Body.Close()

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

func (r *RestStorage) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	listReq, err := json.Marshal(ListRequest{
		Prefix:    prefix,
		Recursive: recursive,
		Token:     r.Token,
	})

	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", r.Endpoint+"list", bytes.NewBuffer(listReq))

	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/json")
	resp, err := r.client.Do(req)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return nil, fs.ErrNotExist
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Unknown status code received: {}", resp.StatusCode)
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

func (r *RestStorage) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	statReq, err := json.Marshal(StatRequest{
		Key:   key,
		Token: r.Token,
	})

	if err != nil {
		return certmagic.KeyInfo{}, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", r.Endpoint+"stat", bytes.NewBuffer(statReq))

	if err != nil {
		return certmagic.KeyInfo{}, err
	}

	req.Header.Add("Content-Type", "application/json")
	resp, err := r.client.Do(req)

	if err != nil {
		return certmagic.KeyInfo{}, err
	}

	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return certmagic.KeyInfo{}, fs.ErrNotExist
	}

	if resp.StatusCode != 200 {
		return certmagic.KeyInfo{}, fmt.Errorf("Unknown status code received: {}", resp.StatusCode)
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
