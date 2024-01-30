### General purpose of usage

In some cases it's hard to put simultaneous redis requests into one redis pipeline
using common syntax. F.e. if you have really high-loaded service with N (thousands+) connected clients
sending messages simultaneously and want to put messages in one pipeline, not N pipelines.

This package will automatically pack incoming
redis calls from different goroutines into one pipeline and return the results to the listeners after
execution.

If you don't have such high-load environment - this package will not help you much.

### Configuration

1. `TTL` - this is a time interval which triggers redis pipeline execution
2. `MaxSize` - number of active listeners which triggers redis pipeline execution
3. `Logger` - basic logger interface

### Example of usage

#### Synchronous call
If you don't need to execute multiple redis commands in same pipeline you may use
basic syntax, which is similar to `go-redis` package syntax:

```
cmd := c.HDel(ctx, "key", "f1", "f2")
```
#### Asynchronous call
If you want to get the results from many redis commands in same code block it's 
better to use asynchronous versions of commands, which has a suffix `Async`, f.e.
`HDelAsync`. Then this commands will be in same pipeline 
(excepting some really rare occasions) and results will be delivered simultaneously:

```
resCh0 := c.HDelAsync(ctx, "key", "f1", "f2")
defer close(resCh0)
resCh1 := c.HGetAsync(ctx, "key1", "name")
defer close(resCh1)
// wait for result
r0, r1 := <-resCh0, <-resCh1
// cast to proper type of redis command
cmd0 := r0.(*redis.IntCmd)
cmd1 := r1.(*redis.StringCmd)
```

Important notes:
* take care of closing result channel after reading from it (but not before)
* async functions returns `chan interface{}`, so cast it to proper redis command type
