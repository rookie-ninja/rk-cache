package rkcache

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/cache/v9"
	"github.com/rookie-ninja/rk-db/redis"
	"github.com/rookie-ninja/rk-entry/v2/entry"
	"github.com/rs/xid"
	"go.uber.org/zap"
	"time"
)

// This must be declared in order to register registration function into rk context
// otherwise, rk-boot won't able to bootstrap echo entry automatically from boot config file
func init() {
	rkentry.RegisterPluginRegFunc(RegisterCacheEntryYAML)
}

const CacheRedisEntry = "CacheRedisEntry"

// GetCache get cache.Cache with entryName
func GetCache(entryName string) *cache.Cache {
	if v := rkentry.GlobalAppCtx.GetEntry(CacheRedisEntry, entryName); v != nil {
		if raw, ok := v.(*CacheEntry); ok {
			return raw.GetCache()
		}
	}

	return nil
}

func GetCacheEntry(entryName string) *CacheEntry {
	if v := rkentry.GlobalAppCtx.GetEntry(CacheRedisEntry, entryName); v != nil {
		if raw, ok := v.(*CacheEntry); ok {
			return raw
		}
	}

	return nil
}

// BootCache bootstrap entry from config
type BootCache struct {
	Cache []*BootCacheE `yaml:"cache" json:"cache"`
}

type BootCacheE struct {
	Name        string `yaml:"name" json:"name"`
	Domain      string `yaml:"domain" json:"domain"`
	Description string `yaml:"description" json:"description"`
	Enabled     bool   `yaml:"enabled" json:"enabled"`
	Local       struct {
		Enabled bool `yaml:"enabled" json:"enabled"`
		Size    int  `yaml:"size" json:"size"`
		TtlMin  int  `yaml:"ttlMin" json:"ttlMin"`
	} `yaml:"local" json:"local"`
	Redis       rkredis.BootRedisE `yaml:"redis" json:"redis"`
	LoggerEntry string             `yaml:"loggerEntry" json:"loggerEntry"`
	CertEntry   string             `yaml:"certEntry" json:"certEntry"`
}

// RegisterCacheEntryYAML create entry from config file
func RegisterCacheEntryYAML(raw []byte) map[string]rkentry.Entry {
	res := make(map[string]rkentry.Entry)

	// 1: unmarshal user provided config into boot config struct
	config := &BootCache{}
	rkentry.UnmarshalBootYAML(raw, config)

	// filter out based domain
	configMap := make(map[string]*BootCacheE)
	for _, e := range config.Cache {
		if !e.Enabled || len(e.Name) < 1 {
			continue
		}

		if !rkentry.IsValidDomain(e.Domain) {
			continue
		}

		// * or matching domain
		// 1: add it to map if missing
		if _, ok := configMap[e.Name]; !ok {
			configMap[e.Name] = e
			continue
		}

		// 2: already has an entry, then compare domain,
		//    only one case would occur, previous one is already the correct one, continue
		if e.Domain == "" || e.Domain == "*" {
			continue
		}

		configMap[e.Name] = e
	}

	for _, element := range configMap {
		var localCache cache.LocalCache
		if element.Local.Enabled {
			// assign default value
			if element.Local.Size < 2 {
				element.Local.Size = 10000
			}

			if element.Local.TtlMin < 1 {
				element.Local.TtlMin = 60
			}

			localCache = cache.NewTinyLFU(element.Local.Size, time.Duration(element.Local.TtlMin)*time.Minute)
		}

		certEntry := rkentry.GlobalAppCtx.GetCertEntry(element.CertEntry)
		loggerEntry := rkentry.GlobalAppCtx.GetLoggerEntry(element.LoggerEntry)

		var redisCache *rkredis.RedisEntry
		if element.Redis.Enabled {
			redisOpt := rkredis.ToRedisUniversalOptions(&element.Redis)
			redisCache = rkredis.RegisterRedisEntry(
				rkredis.WithName(element.Name),
				rkredis.WithDescription(element.Description),
				rkredis.WithUniversalOption(redisOpt),
				rkredis.WithCertEntry(certEntry),
				rkredis.WithLoggerEntry(loggerEntry))

			rkentry.GlobalAppCtx.RemoveEntry(redisCache)
		}

		entry := RegisterCacheEntry(
			WithName(element.Name),
			WithDescription(element.Description),
			WithRedisCache(redisCache),
			WithLocalCache(localCache),
			WithLoggerEntry(loggerEntry))

		res[entry.GetName()] = entry
	}

	return res
}

// RegisterCacheEntry register with Option
func RegisterCacheEntry(opts ...Option) *CacheEntry {
	entry := &CacheEntry{
		entryName:        "CacheRedis",
		entryType:        CacheRedisEntry,
		entryDescription: "CacheRedisEntry with rk-db/redis",
		logger:           rkentry.GlobalAppCtx.GetLoggerEntryDefault().Logger,
	}

	for i := range opts {
		opts[i](entry)
	}

	if len(entry.entryName) < 1 {
		entry.entryName = "cache-" + xid.New().String()
	}

	if len(entry.entryDescription) < 1 {
		entry.entryDescription = fmt.Sprintf("%s entry with name of %s with localCache:%v, redisCache:%v",
			entry.entryType,
			entry.entryName,
			entry.IsLocalCacheEnabled(),
			entry.IsRedisCacheEnabled())
	}

	rkentry.GlobalAppCtx.AddEntry(entry)

	return entry
}

// CacheEntry implementation of rkentry.Entry
type CacheEntry struct {
	entryName        string
	entryType        string
	entryDescription string
	localCache       cache.LocalCache
	redisCache       *rkredis.RedisEntry
	cacheClient      *cache.Cache
	logger           *zap.Logger
}

// Bootstrap entry
func (entry *CacheEntry) Bootstrap(ctx context.Context) {
	entry.logger.Info("Bootstrap CacheRedisEntry",
		zap.String("entryName", entry.entryName),
		zap.Bool("localCache", entry.IsLocalCacheEnabled()),
		zap.Bool("redisCache", entry.IsRedisCacheEnabled()))

	cacheOpt := &cache.Options{
		LocalCache:   entry.localCache,
		StatsEnabled: true,
	}

	// bootstrap redis cache
	if entry.IsRedisCacheEnabled() {
		entry.redisCache.Bootstrap(ctx)
		if rdb, ok := entry.redisCache.GetClient(); ok {
			cacheOpt.Redis = rdb
		}
	}

	entry.cacheClient = cache.New(cacheOpt)
}

// Interrupt entry
func (entry *CacheEntry) Interrupt(ctx context.Context) {
	entry.logger.Info("Interrupt CacheRedisEntry",
		zap.String("entryName", entry.entryName),
		zap.Bool("localCache", entry.IsLocalCacheEnabled()),
		zap.Bool("redisCache", entry.IsRedisCacheEnabled()))

	if entry.IsRedisCacheEnabled() {
		entry.redisCache.Interrupt(ctx)
	}
}

// GetName returns name of entry
func (entry *CacheEntry) GetName() string {
	return entry.entryName
}

// GetType returns type of entry
func (entry *CacheEntry) GetType() string {
	return entry.entryType
}

// GetDescription returns description of entry
func (entry *CacheEntry) GetDescription() string {
	return entry.entryDescription
}

// String to string
func (entry *CacheEntry) String() string {
	bytes, err := json.Marshal(entry)
	if err != nil || len(bytes) < 1 {
		return "{}"
	}

	return string(bytes)
}

// IsLocalCacheEnabled is local cache enabled
func (entry *CacheEntry) IsLocalCacheEnabled() bool {
	return entry.localCache != nil
}

// IsRedisCacheEnabled is redis cache enabled
func (entry *CacheEntry) IsRedisCacheEnabled() bool {
	return entry.redisCache != nil
}

// GetCache returns cache instance
func (entry *CacheEntry) GetCache() *cache.Cache {
	return entry.cacheClient
}

// *************** Service ***************

func (entry *CacheEntry) GetFromCache(req *CacheReq) *CacheResp {
	if entry.cacheClient == nil {
		return &CacheResp{
			Success: false,
			Error:   errors.New("cache client is nil"),
		}
	}

	if req == nil {
		return &CacheResp{
			Success: false,
			Error:   errors.New("CacheReq is nil"),
		}
	}

	// 1: convert key, marshal and calculate MD5
	// 1.1: marshal
	jsonBytesKey, err := json.Marshal(req.Key)
	if err != nil {
		return &CacheResp{
			Success: false,
			Error:   err,
		}
	}

	// 1.2: md5
	md5BytesKey := md5.Sum(jsonBytesKey)

	// 1.3: convert to string
	convertedKey := fmt.Sprintf("%x", md5BytesKey)

	// 2: get from cache
	// 2.1 base64 encoded bytes
	encodeStr := ""

	// 2.2: get from cache
	err = entry.cacheClient.Get(context.Background(), convertedKey, &encodeStr)
	if err != nil {
		return &CacheResp{
			Success: false,
			Error:   err,
		}
	}

	// 2.3: base64 decode
	encoder := base64.StdEncoding
	decodedBytes, err := encoder.DecodeString(encodeStr)
	if err != nil {
		return &CacheResp{
			Success: false,
			Error:   err,
		}
	}

	// 2.4: unmarshal
	err = json.Unmarshal(decodedBytes, req.Value)
	if err != nil {
		return &CacheResp{
			Success: false,
			Error:   err,
		}
	}

	return &CacheResp{
		Success: true,
	}
}

func (entry *CacheEntry) AddToCache(req *CacheReq) *CacheResp {
	if entry.cacheClient == nil {
		return &CacheResp{
			Success: false,
			Error:   errors.New("cache client is nil"),
		}
	}

	if req == nil {
		return &CacheResp{
			Success: false,
			Error:   errors.New("CacheReq is nil"),
		}
	}

	// 1: convert key, marshal and calculate MD5
	// 1.1: marshal
	jsonBytesKey, err := json.Marshal(req.Key)
	if err != nil {
		return &CacheResp{
			Success: false,
			Error:   err,
		}
	}

	// 1.2: md5
	md5BytesKey := md5.Sum(jsonBytesKey)

	// 1.3: convert to string
	convertedKey := fmt.Sprintf("%x", md5BytesKey)

	// 2: convert value, marshal and base64 encode
	// 2.1: marshal
	jsonBytesValue, err := json.Marshal(req.Value)
	if err != nil {
		return &CacheResp{
			Success: false,
			Error:   err,
		}
	}

	// 2.2: base64 encode
	encoder := base64.StdEncoding
	convertedVal := encoder.EncodeToString(jsonBytesValue)

	// 3: set to cache
	err = entry.cacheClient.Set(&cache.Item{
		Key:   convertedKey,
		Value: convertedVal,
	})
	if err != nil {
		return &CacheResp{
			Success: false,
			Error:   err,
		}
	}

	return &CacheResp{
		Success: true,
	}
}

// *************** Option ***************

// Option entry options
type Option func(e *CacheEntry)

// WithName provide name.
func WithName(name string) Option {
	return func(entry *CacheEntry) {
		entry.entryName = name
	}
}

// WithDescription provide name.
func WithDescription(description string) Option {
	return func(entry *CacheEntry) {
		entry.entryDescription = description
	}
}

// WithRedisCache provide RedisEntry
func WithRedisCache(in *rkredis.RedisEntry) Option {
	return func(entry *CacheEntry) {
		if in != nil {
			entry.redisCache = in
		}
	}
}

// WithLocalCache provide LocalCache
func WithLocalCache(in cache.LocalCache) Option {
	return func(entry *CacheEntry) {
		if in != nil {
			entry.localCache = in
		}
	}
}

// WithLoggerEntry provide rkentry.LoggerEntry entry name
func WithLoggerEntry(entry *rkentry.LoggerEntry) Option {
	return func(m *CacheEntry) {
		if entry != nil {
			m.logger = entry.Logger
		} else {
			m.logger = rkentry.GlobalAppCtx.GetLoggerEntryDefault().Logger
		}
	}
}
