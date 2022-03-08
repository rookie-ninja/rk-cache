package rkcache

type CacheReq struct {
	Key   interface{}
	Value interface{}
}

type CacheResp struct {
	Success bool
	Error   error
}
