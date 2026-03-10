package jsonrpc2client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/valyala/fasthttp"
)

type RPCRequests []*RpcRequest

type RpcRequest struct {
	JsonRpc string      `json:"jsonrpc"`
	Id      int         `json:"id"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
}

type RpcResponses []*RpcResponse

type RpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RpcError       `json:"error,omitempty"`
	ID      int             `json:"id"`
}

type RpcError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

type rpcClient struct {
	endpoint       string
	httpClient     *fasthttp.Client
	MaxConnections int
	MaxBatchSize   int
}

func (client *rpcClient) newRequest(req interface{}) (*fasthttp.Request, error) {

	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	request := fasthttp.AcquireRequest()
	request.SetBody(body)
	request.Header.SetMethod("POST")
	request.Header.SetContentType("application/json")
	request.SetRequestURI(client.endpoint)
	return request, nil
}

func (client *rpcClient) CallBatch(requests RPCRequests) (RpcResponses, error) {
	if len(requests) == 0 {
		return nil, errors.New("empty request list")
	}

	for i, req := range requests {
		req.Id = i
		req.JsonRpc = "2.0"
	}
	return client.doBatchCall(requests)
}

func (client *rpcClient) CallBatchRaw(requests RPCRequests) (RpcResponses, error) {
	if len(requests) == 0 {
		return nil, errors.New("empty request list")
	}
	return client.doBatchCall(requests)
}

func (client *rpcClient) CallBatchFast(requests RPCRequests) ([][]byte, error) {
	if len(requests) == 0 {
		return nil, errors.New("empty request list")
	}
	return client.doFastBatchCall(requests)
}

func NewClient(endpoint string) *rpcClient {
	return NewClientWithOpts(endpoint, 1, 4)
}

func NewClientWithOpts(endpoint string, maxConn int, maxBatch int) *rpcClient {
	return &rpcClient{
		endpoint:       endpoint,
		httpClient:     &fasthttp.Client{DialDualStack: true},
		MaxConnections: maxConn,
		MaxBatchSize:   maxBatch,
	}
}

func (client *rpcClient) CallRaw(request *RpcRequest) (*RpcResponse, error) {
	return client.doCall(request)
}

func (client *rpcClient) doBatchCall(rpcRequests []*RpcRequest) ([]*RpcResponse, error) {
	reqs := (len(rpcRequests) / client.MaxBatchSize) + 1
	pendingRpcReqs := make(chan RPCRequests, reqs)

	batch := RPCRequests{}

	for i, p := range rpcRequests {
		batch = append(batch, p)

		if i%client.MaxBatchSize == 0 && i > 0 {
			var pendingBatch RPCRequests
			pendingBatch = batch
			pendingRpcReqs <- pendingBatch
			batch = nil
		}
	}

	if len(batch) > 0 {
		var pendingBatch RPCRequests
		pendingBatch = batch
		pendingRpcReqs <- pendingBatch
		batch = nil
	}

	close(pendingRpcReqs)

	numWorkers := client.MaxConnections
	if reqs < numWorkers {
		numWorkers = reqs
	}

	var wait sync.WaitGroup
	wait.Add(numWorkers)

	var mu sync.Mutex
	rpcResponses := RpcResponses{}

	work := func(rpcRequest RPCRequests) {
		httpRequest, err := client.newRequest(rpcRequest)
		if err != nil {
			log.Printf("%v", err)
			mu.Lock()
			rpcResponses = append(rpcResponses, &RpcResponse{Error: &RpcError{Message: err.Error()}})
			mu.Unlock()
			return
		}
		httpRequest.Header.Set("Accept-Encoding", "gzip")
		res := fasthttp.AcquireResponse()

		if err2 := fasthttp.Do(httpRequest, res); err2 != nil {
			fasthttp.ReleaseRequest(httpRequest)
			fasthttp.ReleaseResponse(res)
			log.Printf("%v", err2)
			mu.Lock()
			rpcResponses = append(rpcResponses, &RpcResponse{Error: &RpcError{Message: err2.Error()}})
			mu.Unlock()
			return
		}
		fasthttp.ReleaseRequest(httpRequest)

		contentEncoding := res.Header.Peek("Content-Encoding")
		var body []byte
		if bytes.EqualFold(contentEncoding, []byte("gzip")) {
			body, _ = res.BodyGunzip()
		} else {
			body = res.Body()
		}

		var batchResponse RpcResponses
		if err3 := json.Unmarshal(body, &batchResponse); err3 != nil {
			fasthttp.ReleaseResponse(res)
			log.Printf("%v", err3)
			mu.Lock()
			rpcResponses = append(rpcResponses, &RpcResponse{Error: &RpcError{Message: err3.Error()}})
			mu.Unlock()
			return
		}
		fasthttp.ReleaseResponse(res)

		if len(batchResponse) > 0 {
			mu.Lock()
			rpcResponses = append(rpcResponses, batchResponse...)
			mu.Unlock()
		}
	}

	worker := func(ch <-chan RPCRequests) {
		defer wait.Done()

		for j := range ch {
			work(j)
		}
	}
	for i := 0; i < numWorkers; i++ {
		go worker(pendingRpcReqs)
	}

	wait.Wait()
	return rpcResponses, nil
}

func (client *rpcClient) doFastBatchCall(rpcRequests []*RpcRequest) ([][]byte, error) {
	reqs := (len(rpcRequests) / client.MaxBatchSize) + 1
	pendingRpcReqs := make(chan RPCRequests, reqs)

	var batch RPCRequests
	for i, p := range rpcRequests {
		batch = append(batch, p)
		if i%client.MaxBatchSize == 0 && i > 0 {
			var pendingBatch RPCRequests
			pendingBatch = batch
			pendingRpcReqs <- pendingBatch
			batch = nil
		}
	}

	if len(batch) > 0 {
		var pendingBatch RPCRequests
		pendingBatch = batch
		pendingRpcReqs <- pendingBatch
	}
	close(pendingRpcReqs)

	numWorkers := client.MaxConnections
	if reqs < numWorkers {
		numWorkers = reqs
	}

	var wait sync.WaitGroup
	wait.Add(numWorkers)

	var mu sync.Mutex
	var rpcResponses [][]byte

	work := func(rpcRequest RPCRequests) {
		httpRequest, err := client.newRequest(rpcRequest)
		if err != nil {
			log.Printf("%v", err)
			return
		}

		res := fasthttp.AcquireResponse()
		httpRequest.Header.Set("Accept-Encoding", "gzip")

		if err2 := fasthttp.Do(httpRequest, res); err2 != nil {
			fasthttp.ReleaseRequest(httpRequest)
			fasthttp.ReleaseResponse(res)
			log.Printf("%v", err2)
			return
		}
		fasthttp.ReleaseRequest(httpRequest)

		contentEncoding := res.Header.Peek("Content-Encoding")
		var body []byte
		if bytes.EqualFold(contentEncoding, []byte("gzip")) {
			// BodyGunzip allocates a new slice, safe to use after ReleaseResponse.
			body, _ = res.BodyGunzip()
		} else {
			// res.Body() returns a reference into the response's internal buffer.
			// Copy before releasing so the caller gets stable memory.
			src := res.Body()
			body = make([]byte, len(src))
			copy(body, src)
		}
		fasthttp.ReleaseResponse(res)

		if len(body) > 0 {
			mu.Lock()
			rpcResponses = append(rpcResponses, body)
			mu.Unlock()
		}
	}

	worker := func(ch <-chan RPCRequests) {
		defer wait.Done()
		for j := range ch {
			work(j)
		}
	}
	for i := 0; i < numWorkers; i++ {
		go worker(pendingRpcReqs)
	}

	wait.Wait()
	return rpcResponses, nil
}

func (client *rpcClient) doCall(RPCRequest *RpcRequest) (*RpcResponse, error) {
	httpRequest, err := client.newRequest(RPCRequest)
	if err != nil {
		return nil, fmt.Errorf("rpc batch call on %v: %v", client.endpoint, err.Error())
	}
	httpRequest.Header.Set("Accept-Encoding", "gzip")
	res := fasthttp.AcquireResponse()

	if err := fasthttp.Do(httpRequest, res); err != nil {
		return nil, err
	}
	fasthttp.ReleaseRequest(httpRequest)

	contentEncoding := res.Header.Peek("Content-Encoding")
	var body []byte
	if bytes.EqualFold(contentEncoding, []byte("gzip")) {
		body, _ = res.BodyGunzip()
	} else {
		body = res.Body()
	}

	rpcResponse := &RpcResponse{}
	if err := json.Unmarshal(body, &rpcResponse); err != nil {
		log.Printf("%v", err)
		return nil, err
	}

	fasthttp.ReleaseResponse(res)
	return rpcResponse, nil
}

func (client *rpcClient) doFastCall(RPCRequest *RpcRequest) (*RpcResponse, error) {
	httpRequest, err := client.newRequest(RPCRequest)
	if err != nil {
		return nil, fmt.Errorf("rpc batch call on %v: %v", client.endpoint, err.Error())
	}
	httpRequest.Header.Set("Accept-Encoding", "gzip")
	res := fasthttp.AcquireResponse()

	if err := fasthttp.Do(httpRequest, res); err != nil {
		return nil, err
	}
	fasthttp.ReleaseRequest(httpRequest)

	contentEncoding := res.Header.Peek("Content-Encoding")
	var body []byte
	if bytes.EqualFold(contentEncoding, []byte("gzip")) {
		body, _ = res.BodyGunzip()
	} else {
		body = res.Body()
	}

	rpcResponse := &RpcResponse{}
	if err := json.Unmarshal(body, &rpcResponse); err != nil {
		log.Printf("%v", err)
		return nil, err
	}

	fasthttp.ReleaseResponse(res)
	return rpcResponse, nil
}
