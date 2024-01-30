package redis_autopipeline

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestClientTTLTriggered(t *testing.T) {
	var ctx = context.TODO()
	db, mock := redismock.NewClientMock()
	mock.ExpectHDel("key1").SetVal(1)
	mock.ExpectHDel("key2", "key3").SetVal(2)
	mock.MatchExpectationsInOrder(false)

	c, err := NewAutoPipeline(db,
		WithCacheTTL(time.Microsecond*500),
		WithMaxSize(200))
	assert.Nil(t, err)

	run1 := time.Now()
	result, err := c.HDel(ctx, "key1").Result()
	assert.Nil(t, err)
	assert.Equal(t, int64(1), result)

	result2, err := c.HDel(ctx, "key2", "key3").Result()
	assert.Nil(t, err)
	assert.Equal(t, int64(2), result2)
	run2 := time.Now()

	// ensure that commands were run in separate pipelines and pipelines were triggered by ttl param
	assert.True(t, run1.Add(449*time.Microsecond).Before(run2))
}

func TestClientSizeTriggered(t *testing.T) {
	var ctx = context.TODO()
	db, mock := redismock.NewClientMock()
	mock.ExpectHDel("key1").SetVal(1)
	mock.ExpectHDel("key2").SetVal(2)
	mock.ExpectHDel("key3").SetVal(3)
	mock.MatchExpectationsInOrder(false)

	c, err := NewAutoPipeline(db,
		WithCacheTTL(time.Microsecond*5000),
		WithMaxSize(1))
	assert.Nil(t, err)

	runTime := time.Now()
	resCh1 := c.HDelAsync(ctx, "key1")
	defer close(resCh1)
	resCh2 := c.HDelAsync(ctx, "key2")
	defer close(resCh2)
	resCh3 := c.HDelAsync(ctx, "key3")
	defer close(resCh3)
	// Ensure all 3 commands executed in same pipeline call
	assert.True(t, runTime.Add(200*time.Microsecond).After(time.Now()))

	res1, res2, res3 := <-resCh1, <-resCh2, <-resCh3

	cmd1, cmd2, cmd3 := res1.(*redis.IntCmd), res2.(*redis.IntCmd), res3.(*redis.IntCmd)
	r1, err := cmd1.Result()
	assert.Nil(t, err)
	assert.Equal(t, int64(1), r1)

	r2, err := cmd2.Result()
	assert.Nil(t, err)
	assert.Equal(t, int64(2), r2)

	r3, err := cmd3.Result()
	assert.Nil(t, err)
	assert.Equal(t, int64(3), r3)
}

func TestClientMultipleListenersForSameCommand(t *testing.T) {
	var ctx = context.TODO()
	db, mock := redismock.NewClientMock()
	mock.ExpectHDel("key1").SetVal(1)
	mock.ExpectHDel("key2").SetVal(2)
	mock.ExpectHDel("key3").SetVal(3)
	mock.MatchExpectationsInOrder(false)

	c, err := NewAutoPipeline(db,
		WithCacheTTL(time.Microsecond*5000),
		WithMaxSize(1))
	assert.Nil(t, err)

	runTime := time.Now()
	resCh1 := c.HDelAsync(ctx, "key1")
	defer close(resCh1)
	resCh2 := c.HDelAsync(ctx, "key2")
	defer close(resCh2)

	// one command in pipeline, but multiple listeners
	resCh3 := c.HDelAsync(ctx, "key3")
	defer close(resCh3)
	resCh4 := c.HDelAsync(ctx, "key3")
	defer close(resCh4)
	resCh5 := c.HDelAsync(ctx, "key3")
	defer close(resCh5)

	// Ensure all 3 commands executed in same pipeline call
	assert.True(t, runTime.Add(200*time.Microsecond).After(time.Now()))

	res1, res2, res3, res4, res5 := <-resCh1, <-resCh2, <-resCh3, <-resCh4, <-resCh5

	cmd1, cmd2, cmd3, cmd4, cmd5 := res1.(*redis.IntCmd), res2.(*redis.IntCmd), res3.(*redis.IntCmd), res4.(*redis.IntCmd), res5.(*redis.IntCmd)
	r1, err := cmd1.Result()
	assert.Nil(t, err)
	assert.Equal(t, int64(1), r1)

	r2, err := cmd2.Result()
	assert.Nil(t, err)
	assert.Equal(t, int64(2), r2)

	r3, err := cmd3.Result()
	assert.Nil(t, err)
	assert.Equal(t, int64(3), r3)
	r4, err := cmd4.Result()
	assert.Nil(t, err)
	assert.Equal(t, int64(3), r4)
	r5, err := cmd5.Result()
	assert.Nil(t, err)
	assert.Equal(t, int64(3), r5)
}

func TestClosedChannelRecovery(t *testing.T) {
	var ctx = context.TODO()
	db, mock := redismock.NewClientMock()
	mock.ExpectHDel("key", "f1", "f2").SetVal(2)
	mock.ExpectHGet("key1", "name").SetVal("john")
	mock.MatchExpectationsInOrder(false)

	c, err := NewAutoPipeline(db,
		WithCacheTTL(time.Microsecond*100),
		WithMaxSize(200))
	assert.Nil(t, err)
	resCh := c.HDelAsync(ctx, "key", "f1", "f2")
	close(resCh)
	resCh1 := c.HGetAsync(ctx, "key1", "name")
	defer close(resCh1)
	cmd := <-resCh1
	r1, err := cmd.(*redis.StringCmd).Result()
	assert.Nil(t, err)
	assert.Equal(t, "john", r1)
}

func TestIdleRecipients(t *testing.T) {
	var ctx = context.TODO()
	db, mock := redismock.NewClientMock()
	mock.ExpectHDel("key1").SetVal(1)
	mock.ExpectHDel("key2").SetVal(2)
	mock.ExpectHDel("key3").SetVal(3)
	mock.MatchExpectationsInOrder(false)

	c, err := NewAutoPipeline(db,
		WithCacheTTL(time.Microsecond*5000),
		WithMaxSize(1))
	assert.Nil(t, err)

	runTime := time.Now()
	c.HDelAsync(ctx, "key1")
	c.HDelAsync(ctx, "key2")
	resCh3 := c.HDelAsync(ctx, "key3")
	defer close(resCh3)
	// Ensure all 3 commands executed in same pipeline call
	assert.True(t, runTime.Add(200*time.Microsecond).After(time.Now()))

	res3 := <-resCh3
	cmd3 := res3.(*redis.IntCmd)
	r3, err := cmd3.Result()
	assert.Nil(t, err)
	assert.Equal(t, int64(3), r3)
}

func TestContextDone(t *testing.T) {
	var ctx = context.TODO()
	db, _ := redismock.NewClientMock()

	pipeCtx, cancel := context.WithCancel(context.Background())
	c, err := NewAutoPipeline(db,
		WithCacheTTL(time.Microsecond*500),
		WithRunInterval(time.Nanosecond),
		WithMaxSize(200),
		WithContext(pipeCtx))
	assert.Nil(t, err)
	cancel()
	time.Sleep(100 * time.Microsecond)

	_, err = c.HDel(ctx, "key2").Result()
	assert.NotNil(t, err)
}

func TestHDel(t *testing.T) {
	var ctx = context.TODO()
	db, mock := redismock.NewClientMock()
	mock.ExpectHDel("key", "f1", "f2").SetVal(2)
	mock.MatchExpectationsInOrder(false)

	c, err := NewAutoPipeline(db,
		WithCacheTTL(time.Microsecond*5),
		WithMaxSize(200))
	assert.Nil(t, err)
	result, err := c.HDel(ctx, "key", "f1", "f2").Result()
	assert.Nil(t, err)
	assert.Equal(t, int64(2), result)
}

func TestExpire(t *testing.T) {
	var ctx = context.TODO()
	db, mock := redismock.NewClientMock()
	mock.ExpectExpire("key", time.Second).SetVal(true)
	mock.MatchExpectationsInOrder(false)

	c, err := NewAutoPipeline(db,
		WithCacheTTL(time.Microsecond*5),
		WithMaxSize(200))
	assert.Nil(t, err)
	result, err := c.Expire(ctx, "key", time.Second).Result()
	assert.Nil(t, err)
	assert.Equal(t, true, result)
}

func TestHGet(t *testing.T) {
	var ctx = context.TODO()
	db, mock := redismock.NewClientMock()
	mock.ExpectHGet("key", "name").SetVal("john")
	mock.MatchExpectationsInOrder(false)

	c, err := NewAutoPipeline(db,
		WithCacheTTL(time.Microsecond*5),
		WithMaxSize(200))
	assert.Nil(t, err)
	result, err := c.HGet(ctx, "key", "name").Result()
	assert.Nil(t, err)
	assert.Equal(t, "john", result)
}

func TestHGetAll(t *testing.T) {
	var ctx = context.TODO()
	db, mock := redismock.NewClientMock()
	mock.ExpectHGetAll("key").SetVal(map[string]string{
		"name": "john",
		"age":  "20",
	})
	mock.MatchExpectationsInOrder(false)

	c, err := NewAutoPipeline(db,
		WithCacheTTL(time.Microsecond*5),
		WithMaxSize(200))
	assert.Nil(t, err)
	result, err := c.HGetAll(ctx, "key").Result()
	assert.Nil(t, err)
	assert.Equal(t, map[string]string{
		"name": "john",
		"age":  "20",
	}, result)
}

func TestGet(t *testing.T) {
	var ctx = context.TODO()
	db, mock := redismock.NewClientMock()
	mock.ExpectGet("key").SetVal("john")
	mock.MatchExpectationsInOrder(false)

	c, err := NewAutoPipeline(db,
		WithCacheTTL(time.Microsecond*5),
		WithMaxSize(200))
	assert.Nil(t, err)
	result, err := c.Get(ctx, "key").Result()
	assert.Nil(t, err)
	assert.Equal(t, "john", result)
}

func TestDel(t *testing.T) {
	var ctx = context.TODO()
	db, mock := redismock.NewClientMock()
	mock.ExpectDel("key", "key1").SetVal(2)
	mock.MatchExpectationsInOrder(false)

	c, err := NewAutoPipeline(db,
		WithCacheTTL(time.Microsecond*5),
		WithMaxSize(200))
	assert.Nil(t, err)
	result, err := c.Del(ctx, "key", "key1").Result()
	assert.Nil(t, err)
	assert.Equal(t, int64(2), result)
}

func TestSMembers(t *testing.T) {
	var ctx = context.TODO()
	db, mock := redismock.NewClientMock()
	mock.ExpectSMembers("key").SetVal([]string{"john", "mike"})
	mock.MatchExpectationsInOrder(false)

	c, err := NewAutoPipeline(db,
		WithCacheTTL(time.Microsecond*5),
		WithMaxSize(200))
	assert.Nil(t, err)
	result, err := c.SMembers(ctx, "key").Result()
	assert.Nil(t, err)
	assert.Equal(t, []string{"john", "mike"}, result)
}

func TestMGet(t *testing.T) {
	var ctx = context.TODO()
	db, mock := redismock.NewClientMock()
	mock.ExpectMGet("key", "key1").SetVal([]interface{}{"john", "jill"})
	mock.MatchExpectationsInOrder(false)

	c, err := NewAutoPipeline(db,
		WithCacheTTL(time.Microsecond*5),
		WithMaxSize(200))
	assert.Nil(t, err)
	result, err := c.MGet(ctx, "key", "key1").Result()
	assert.Nil(t, err)
	assert.Equal(t, []interface{}{"john", "jill"}, result)
}
