package redis_autopipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// redisOperation is a main and only one member of cache storage
// it contains type of redis command (kind)
// arguments of redis command (args)
// and list of receivers of redis command (listeners)
type redisOperation struct {
	args      []string           // arguments of redis command, f.e. for HGetAll("prefix") argument is "prefix"
	listeners []chan interface{} // collection of listeners awaiting redis operation to be executed. Result goes here.
	kind      operationPrefix    // redis command, e.g. HSet, Del, HGet and so on
}

// cache is a core structure of this package
// it contains storage, cache params and methods to use them all
type cache struct {
	mx                   *sync.RWMutex              // shared mutex
	client               *redis.Client              // go-redis client
	storage              map[string]*redisOperation // storage of scheduled redis command to be pipelined
	activeListeners      atomic.Int32               // number of active listeners in storage
	storageThresholdSize int32                      // number of stored redis requests to run redis pipeline
	storageThresholdTime time.Duration              // time interval to run redis pipeline
	runInterval          time.Duration              // sleep time between checks to run pipeline
	lastPipeline         atomic.Int64               // execution time of last redis pipeline, in microseconds
	log                  Logger                     // logger interface
	done                 atomic.Bool                // marks this cache instance as stopped
}

// newCache returns a pointer to a new cache storage
// and runs it in background
func newCache(c *redis.Client, ctx context.Context, ttl, runInterval time.Duration, size uint, l Logger) *cache {
	cc := cache{
		client:               c,
		storage:              make(map[string]*redisOperation),
		mx:                   &sync.RWMutex{},
		storageThresholdTime: ttl,
		storageThresholdSize: int32(size),
		runInterval:          runInterval,
		log:                  l,
	}
	cc.lastPipeline.Store(time.Now().UnixMicro()) // let's set imaginary pipeline run at startup
	go cc.run(ctx)
	return &cc
}

// run is a controller goroutine, which observe state of storage and triggers runPipeline function
// if certain conditions met
func (c *cache) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			// stop receiving new commands
			c.done.Store(true)
			// and run pipeline for a last time
			if c.activeListeners.Load() > 0 {
				c.runPipeline(ctx)
			}
			return
		default:
			// put this goroutine to a waiting state for short period of time
			// this will allow most (but not 100% all) incoming simultaneous async redis requests from client goroutines
			// to be executed in same pipeline
			time.Sleep(c.runInterval)
			// check number of listeners threshold
			if c.activeListeners.Load() > c.storageThresholdSize {
				c.runPipeline(ctx)
				continue
			}
			// check time threshold
			lastRun := time.UnixMicro(c.lastPipeline.Load())
			if lastRun.Add(c.storageThresholdTime).Before(time.Now()) && c.activeListeners.Load() > 0 {
				c.runPipeline(ctx)
			}
		}
	}
}

// runPipeline gather all existed redis commands from the storage,
// put all of them into single redis pipeline, executes it,
// and returns a results of execution to a respective listeners
// nolint:cyclop
func (c *cache) runPipeline(ctx context.Context) {
	pipe := c.client.Pipeline()
	// prepare pipe, do not cast a types, let's have a set of maps
	// more code, but more readable, slightly better compiling and performance
	intCmds := map[string]*redis.IntCmd{}
	sliceCmds := map[string]*redis.SliceCmd{}
	stringCmds := map[string]*redis.StringCmd{}
	stringStringMapCmds := map[string]*redis.MapStringStringCmd{}
	stringSliceCmds := map[string]*redis.StringSliceCmd{}
	boolCmds := map[string]*redis.BoolCmd{}
	// lock the mutex for read, and gather all commands to be pipelined in redis
	c.mx.RLock()
	for hash, op := range c.storage {
		switch op.kind {
		case HDel:
			key, fields := normalizeHDel(op.args)
			intCmds[hash] = pipe.HDel(ctx, key, fields...)
		case Del:
			keys := normalizeDel(op.args)
			intCmds[hash] = pipe.Del(ctx, keys...)
		case MGet:
			keys := normalizeMGet(op.args)
			sliceCmds[hash] = pipe.MGet(ctx, keys...)
		case HGet:
			key, field := normalizeHGet(op.args)
			stringCmds[hash] = pipe.HGet(ctx, key, field)
		case Get:
			key := normalizeGet(op.args)
			stringCmds[hash] = pipe.Get(ctx, key)
		case HGetAll:
			key := normalizeHGetAll(op.args)
			stringStringMapCmds[hash] = pipe.HGetAll(ctx, key)
		case SMembers:
			key := normalizeSMembers(op.args)
			stringSliceCmds[hash] = pipe.SMembers(ctx, key)
		case Expire:
			key, duration := normalizeExpire(op.args)
			boolCmds[hash] = pipe.Expire(ctx, key, duration)
		}
	}
	c.mx.RUnlock()

	// exec pipe, no need to lock mutex while we perform redis request, too long
	// if any upcoming request came in meantime, and if we already have this request in pipeline (duplicated)
	// we will return result from existed pipeline, so we'll save time and one request
	// and if this is a new request - it will be added to storage and served in next run of this function
	_, err := pipe.Exec(ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		c.log.Error(err)
		return
	}

	// store the time of last redis pipeline
	c.lastPipeline.Store(time.Now().UnixMicro())
	// send the results to listeners
	for hash, cmd := range intCmds {
		c.sendResult(hash, cmd)
	}
	for hash, cmd := range sliceCmds {
		c.sendResult(hash, cmd)
	}
	for hash, cmd := range stringCmds {
		c.sendResult(hash, cmd)
	}
	for hash, cmd := range stringStringMapCmds {
		c.sendResult(hash, cmd)
	}
	for hash, cmd := range stringSliceCmds {
		c.sendResult(hash, cmd)
	}
	for hash, cmd := range boolCmds {
		c.sendResult(hash, cmd)
	}
}

func (c *cache) sendResult(hash string, redisCmd interface{}) {
	// For case of unexpected write to a closed channel
	defer func() {
		if r := recover(); r != nil {
			c.log.Error("recovered:", r)
		}
	}()
	c.mx.Lock()
	o, ok := c.storage[hash]
	// should never happen, as only one pipe could be processed at the time
	if !ok {
		c.mx.Unlock()
		c.log.Error(fmt.Errorf("%w: %s", ErrHashNotFound, hash))
		return
	}

	delete(c.storage, hash)
	// TODO: recreate storage map, as map only grows and never shrink?
	c.mx.Unlock()

	// here we don't want to lock the mutex, as delivering of results may be time-consuming
	for _, r := range o.listeners {
		r <- redisCmd
		// decrement the listeners number
		c.activeListeners.Add(-1)
	}
}

// enqueue puts the request to the cache, to be executed in next runPipeline execution
func (c *cache) enqueue(kind operationPrefix, args []string) chan interface{} {
	resultCh := make(chan interface{}, resultChannelBufferSize)
	// don't schedule anything if cache is stopped
	if c.done.Load() {
		c.log.Error(ErrCacheStopped)
		close(resultCh)
		return resultCh
	}
	h := hashStringSlice(kind, args)
	c.mx.Lock()
	defer c.mx.Unlock()
	if _, ok := c.storage[h]; !ok {
		c.storage[h] = &redisOperation{
			kind:      kind,
			args:      args,
			listeners: []chan interface{}{resultCh},
		}
		c.activeListeners.Add(1)
		return resultCh
	}
	c.storage[h].listeners = append(c.storage[h].listeners, resultCh)
	c.activeListeners.Add(1)
	return resultCh
}
