package redis_autopipeline

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

type operationPrefix byte

const (
	// List of supported redis commands
	HDel operationPrefix = iota + 1
	Expire
	HGet
	HGetAll
	Get
	Del
	SMembers
	MGet

	defaultCacheTTL         = time.Microsecond * 1000
	defaultCacheSize   uint = 100
	defaultRunInterval      = 50 * time.Microsecond

	resultChannelBufferSize = 1
)

var (
	ErrChannelClosed = errors.New("unexpected error: channel closed")
	ErrRedisIsNil    = errors.New("redis client is nil")
	ErrHashNotFound  = errors.New("hash  not found")
	ErrCacheStopped  = errors.New("cache is stopped")
)

type Client interface {
	HDel(ctx context.Context, key string, fields ...string) *redis.IntCmd
	HDelAsync(ctx context.Context, key string, fields ...string) chan interface{}
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	ExpireAsync(ctx context.Context, key string, expiration time.Duration) chan interface{}
	HGet(ctx context.Context, key, field string) *redis.StringCmd
	HGetAsync(ctx context.Context, key, field string) chan interface{}
	HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd
	HGetAllAsync(ctx context.Context, key string) chan interface{}
	Get(ctx context.Context, key string) *redis.StringCmd
	GetAsync(ctx context.Context, key string) chan interface{}
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	DelAsync(ctx context.Context, keys ...string) chan interface{}
	SMembers(ctx context.Context, key string) *redis.StringSliceCmd
	SMembersAsync(ctx context.Context, key string) chan interface{}
	MGet(ctx context.Context, keys ...string) *redis.SliceCmd
	MGetAsync(ctx context.Context, keys ...string) chan interface{}
}

type Logger interface {
	Error(args ...interface{})
}

type defaultLogger struct{}

func (l *defaultLogger) Error(args ...interface{}) {
	fmt.Println(args...)
}

// config contains configuration parameters for Autopipeline
type config struct {
	// ctx is a context
	ctx context.Context
	// ttl is time to live of cached redis queries
	// on end of ttl this package will perform a pipelined call to a redis
	// which will contain all queries from the cache
	ttl time.Duration
	// maxSize is a number of queries to keep in cache
	// once cache storageThresholdSize is bigger than maxSize this package will perform a pipelined call to a redis
	// which will contain all queries from the cache
	maxSize uint
	// runInterval is an interval between runs of main runtime
	// where we check certain conditions (ttl or maxSize) and run redis pipeline
	runInterval time.Duration
	// Basic logger interface
	logger Logger
}

type Autopipeline struct {
	redisClient *redis.Client
	cache       *cache
	cnf         *config
}

func NewAutoPipeline(redisClient *redis.Client, options ...func(a *Autopipeline)) (Client, error) {
	if redisClient == nil {
		return nil, ErrRedisIsNil
	}
	var logger Logger = &defaultLogger{}
	a := &Autopipeline{
		redisClient: redisClient,
		cnf: &config{
			ctx:         context.TODO(),
			ttl:         defaultCacheTTL,
			maxSize:     defaultCacheSize,
			runInterval: defaultRunInterval,
			logger:      logger,
		},
	}
	for _, o := range options {
		o(a)
	}
	a.cache = newCache(a.redisClient, a.cnf.ctx, a.cnf.ttl, a.cnf.runInterval, a.cnf.maxSize, a.cnf.logger)
	return a, nil
}

func WithContext(ctx context.Context) func(a *Autopipeline) {
	return func(a *Autopipeline) {
		a.cnf.ctx = ctx
	}
}

func WithCacheTTL(timeout time.Duration) func(a *Autopipeline) {
	return func(a *Autopipeline) {
		a.cnf.ttl = timeout
	}
}

func WithRunInterval(timeout time.Duration) func(a *Autopipeline) {
	return func(a *Autopipeline) {
		a.cnf.runInterval = timeout
	}
}

func WithMaxSize(size uint) func(a *Autopipeline) {
	return func(a *Autopipeline) {
		a.cnf.maxSize = size
	}
}

func WithLogger(l Logger) func(a *Autopipeline) {
	return func(a *Autopipeline) {
		a.cnf.logger = l
	}
}

func (a Autopipeline) HDel(ctx context.Context, key string, fields ...string) *redis.IntCmd {
	resCh := a.HDelAsync(ctx, key, fields...)
	res, ok := <-resCh
	if !ok {
		resp := redis.IntCmd{}
		resp.SetErr(ErrChannelClosed)
		return &resp
	}
	defer close(resCh)
	return res.(*redis.IntCmd)
}

func (a Autopipeline) HDelAsync(ctx context.Context, key string, fields ...string) chan interface{} {
	args := transformHDel(key, fields...)
	return a.cache.enqueue(HDel, args)
}

func (a Autopipeline) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	resCh := a.ExpireAsync(ctx, key, expiration)
	res, ok := <-resCh
	if !ok {
		resp := redis.BoolCmd{}
		resp.SetErr(ErrChannelClosed)
		return &resp
	}
	defer close(resCh)
	return res.(*redis.BoolCmd)
}

func (a Autopipeline) ExpireAsync(ctx context.Context, key string, expiration time.Duration) chan interface{} {
	args := transformExpire(key, expiration)
	return a.cache.enqueue(Expire, args)
}

func (a Autopipeline) HGet(ctx context.Context, key, field string) *redis.StringCmd {
	resCh := a.HGetAsync(ctx, key, field)
	res, ok := <-resCh
	if !ok {
		resp := redis.StringCmd{}
		resp.SetErr(ErrChannelClosed)
		return &resp
	}
	defer close(resCh)
	return res.(*redis.StringCmd)
}

func (a Autopipeline) HGetAsync(ctx context.Context, key, field string) chan interface{} {
	args := transformHGet(key, field)
	return a.cache.enqueue(HGet, args)
}

func (a Autopipeline) HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd {
	resCh := a.HGetAllAsync(ctx, key)
	res, ok := <-resCh
	if !ok {
		resp := redis.MapStringStringCmd{}
		resp.SetErr(ErrChannelClosed)
		return &resp
	}
	defer close(resCh)
	return res.(*redis.MapStringStringCmd)
}

func (a Autopipeline) HGetAllAsync(ctx context.Context, key string) chan interface{} {
	args := transformHGetAll(key)
	return a.cache.enqueue(HGetAll, args)
}

func (a Autopipeline) Get(ctx context.Context, key string) *redis.StringCmd {
	resCh := a.GetAsync(ctx, key)
	res, ok := <-resCh
	if !ok {
		resp := redis.StringCmd{}
		resp.SetErr(ErrChannelClosed)
		return &resp
	}
	defer close(resCh)
	return res.(*redis.StringCmd)
}
func (a Autopipeline) GetAsync(ctx context.Context, key string) chan interface{} {
	args := transformGet(key)
	return a.cache.enqueue(Get, args)
}

func (a Autopipeline) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	resCh := a.DelAsync(ctx, keys...)
	res, ok := <-resCh
	if !ok {
		resp := redis.IntCmd{}
		resp.SetErr(ErrChannelClosed)
		return &resp
	}
	defer close(resCh)
	return res.(*redis.IntCmd)
}

func (a Autopipeline) DelAsync(ctx context.Context, keys ...string) chan interface{} {
	args := transformDel(keys...)
	return a.cache.enqueue(Del, args)
}

func (a Autopipeline) SMembers(ctx context.Context, key string) *redis.StringSliceCmd {
	resCh := a.SMembersAsync(ctx, key)
	res, ok := <-resCh
	if !ok {
		resp := redis.StringSliceCmd{}
		resp.SetErr(ErrChannelClosed)
		return &resp
	}
	defer close(resCh)
	return res.(*redis.StringSliceCmd)
}

func (a Autopipeline) SMembersAsync(ctx context.Context, key string) chan interface{} {
	args := transformSMembers(key)
	return a.cache.enqueue(SMembers, args)
}

func (a Autopipeline) MGet(ctx context.Context, keys ...string) *redis.SliceCmd {
	resCh := a.MGetAsync(ctx, keys...)
	res, ok := <-resCh
	if !ok {
		resp := redis.SliceCmd{}
		resp.SetErr(ErrChannelClosed)
		return &resp
	}
	defer close(resCh)
	return res.(*redis.SliceCmd)
}

func (a Autopipeline) MGetAsync(ctx context.Context, keys ...string) chan interface{} {
	args := transformMGet(keys...)
	return a.cache.enqueue(MGet, args)
}
