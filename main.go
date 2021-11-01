package main

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

//USING CONTEXT AND IMPLEMENTING SOME GENERAL PATTERNS

//using a context
//when a service request is initialized either by an incoming request or trigerred by the main function
//the top level procces will use the background function to create a new context value
//possibly decorating it with one or more of the context.With* functions
//before passing it along to any subrequest

func slowoperation(ctx context.Context) (int, error) {
	time.Sleep(time.Second * 7)
	return 0, nil
}

func Stream(ctx context.Context, out chan<- interface{}) error {
	//create a derived context with a 10 s timeout;
	//will be cancelled upon timeout , ctx will not
	//cancel is a function that will explicitely cancel dctx
	dctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	var res int
	var err error
	go func() {
		res, err = slowoperation(dctx)
	}()
	if err != nil {
		log.Fatal(err)
	}
	for {
		select {
		case out <- res:
			fmt.Println("all are okey")
			//read from res; send to out
		case <-ctx.Done():
			//trigered ctx is cancelled
			return ctx.Err()
		}

	}
}

type Circuit func(context.Context) (string, error)

//the breaker function accepts any funvtion that conforms to the function
//Circuits. In return it provides another function, witch also conforms
//the circuit type definition, but with more logic

func Breaker(circuit Circuit, failureThreshold uint) Circuit {
	var consecutiveFails int = 0
	var lastAttemp = time.Now()
	var m sync.RWMutex

	return func(ctx context.Context) (string, error) {
		m.RLock() //establish a read lock
		d := consecutiveFails - int(failureThreshold)

		if d >= 0 {
			shouldRetryAt := lastAttemp.Add(time.Second * 2 << d)
			if !time.Now().After(shouldRetryAt) {
				m.RUnlock()
				return "", errors.New("service unreachable")
			}
		}
		m.RUnlock()
		response, err := circuit(ctx)
		m.Lock()

		defer m.Unlock()
		lastAttemp = time.Now()
		if err != nil {
			consecutiveFails++
			fmt.Println("unexpected error number :", consecutiveFails)
			return response, err
		}
		consecutiveFails = 0
		return response, nil
	}
}

//DEBOUNCE PATTERN
//debounce limits the frequency of a function invocation so thay only the first
//or last in a cluster of calls is actually performed

//implementation , weare going to use the circuit function type defined before
func DebounceFirst(circuit Circuit, d time.Duration) Circuit {
	var threshold time.Time
	var result string
	var err error
	var m sync.Mutex

	return func(ctx context.Context) (string, error) {
		m.Lock()

		defer func() {
			threshold = time.Now().Add(d)
			m.Unlock()
			fmt.Println(threshold, "unlocked undefined")
		}()
		if time.Now().Before(threshold) {
			return result, err
		}

		result, err = circuit(ctx)

		return result, err
	}
}

func DebounceLast(circuit Circuit, d time.Duration) Circuit {
	var threshold time.Time = time.Now()
	var ticker *time.Ticker
	var result string
	var err error
	var once sync.Once
	var m sync.Mutex

	return func(ctx context.Context) (string, error) {
		m.Lock()
		defer m.Unlock()

		threshold = time.Now().Add(d)
		once.Do(func() {
			ticker = time.NewTicker(time.Millisecond * 100)
			go func() {
				defer func() {
					m.Lock()
					ticker.Stop()
					once = sync.Once{}
					m.Unlock()
				}()
				for {
					select {
					case <-ticker.C:
						m.Lock()
						if time.Now().After(threshold) {
							result, err = circuit(ctx)
							m.Unlock()
							return
						}
						m.Unlock()
					case <-ctx.Done():
						m.Lock()
						result, err = "", ctx.Err()
						m.Unlock()
						return
					}
				}
			}()
		})
		return result, err
	}
}

//RETRY
//retry accounts for a possible transient fault in a distributed system by transparently
//retrying a failed operation

type Effector func(context.Context) (string, error)

func Retry(effector Effector, retries int, delay time.Duration) Effector {
	return func(ctx context.Context) (string, error) {
		for r := 0; ; r++ {
			response, err := effector(ctx)
			if err == nil || r >= retries {
				return response, err
			}

			log.Printf("attempt %d failed; retrying in %v", r+1, delay)

			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
	}
}

//THROTLE
//trotle limits the frequency of a function call to some maximum number of invocationsper unit of time
//a user may only be allowed 10 service request per second
//a xlient may restric itself to call a particular function once every 500 millisecond
//an account may only be allowed three failed login attempts in a 24 hour period

func Throttle(effector Effector, max uint, refill uint, d time.Duration) Effector {
	var tokens = max
	var once sync.Once

	return func(ctx context.Context) (string, error) {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		once.Do(func() {
			ticker := time.NewTicker(d)

			go func() {
				defer ticker.Stop()

				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						t := tokens + refill
						if t > max {
							t = max
						}
						tokens = t
					}
				}
			}()
		})

		if tokens <= 0 {
			return "", fmt.Errorf("too many calls")
		}

		tokens--
		return effector(ctx)
	}
}

//timeout
//timeout allows a procces to stop waiting for a new answer once its clear that an answer
//may not be coming

type SlowFunction func(string) (string, error)
type WithContext func(context.Context, string) (string, error)

func Timeout(f SlowFunction) WithContext {
	return func(ctx context.Context, arg string) (string, error) {
		chres := make(chan string)
		cherr := make(chan error)

		go func() {
			res, err := f(arg)
			chres <- res
			cherr <- err
		}()
		select {
		case res := <-chres:
			return res, <-cherr
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}

//CONCURRENCY PATTERNS
//fan in
//fan in multiplexes multiple input chanels onto one output chanel

func Funnel(sources ...<-chan int) <-chan int {
	dest := make(chan int)
	var wg sync.WaitGroup

	wg.Add(len(sources))

	for _, ch := range sources {
		go func(c <-chan int) {
			defer wg.Done()
			for n := range c {
				dest <- n
			}
		}(ch)
		go func() {
			wg.Wait()
			close(dest)
		}()
	}
	return dest
}

//FAN OUT
//fan out evenly distributes messages from an input channel to multiple output
//chanels

func Split(source <-chan int, n int) []<-chan int {
	dests := make([]<-chan int, 0)
	for i := 0; i < n; i++ {
		ch := make(chan int)
		dests = append(dests, ch)
		go func() {
			defer close(ch)
			for val := range source {
				ch <- val
			}
		}()
	}
	return dests
}

//FUTURE
//future provides a placeholder for a value thats not yet known
/*
func ConcurrentInverse(m Matrix) <-chan Matrix {
	out := make(chan Matrix)

	go func() {
		out <- BlockingInverse(m)
		close(out)
	}()

	return out
}
THINS IS A NON WORKING EXAMPLE, ONLY FOR SEEING
//using concurrentinverse, one could then build a function to calculate the inverse
//product of two matrices

func InverseProduct(a, b Matrix) Matrix {
	inva := ConcurrentInverse(a)
	invb := ConcurrentInverse(b)
	return Product(<-inva, <-invb)
}
*/

type Future interface {
	Result() (string, error)
}
type InnerFuture struct {
	once sync.Once
	wg   sync.WaitGroup

	res   string
	err   error
	resCH <-chan string
	errCH <-chan error
}

func (f *InnerFuture) Result() (string, error) {
	f.once.Do(func() {
		f.wg.Add(1)
		defer f.wg.Done()
		f.res = <-f.resCH
		f.err = <-f.errCH
	})
	f.wg.Wait()
	return f.res, f.err
}

func SlowFunction2(ctx context.Context) Future {
	resCH := make(chan string)
	errCH := make(chan error)

	go func() {
		select {
		case <-time.After(time.Second * 2):
			resCH <- "i sleept for 2 seconds"
			errCH <- nil
		case <-ctx.Done():
			resCH <- ""
			errCH <- ctx.Err()
		}
	}()
	return &InnerFuture{resCH: resCH, errCH: errCH}
}

//SHARDING
//sharding splits a large data sructrure into multiple partitions to localize the effects of read/write locks
var items = struct {
	sync.RWMutex
	m map[string]int
}{m: make(map[string]int)}

func ThreadSafeRead(key string) int {
	items.RLock()
	value := items.m[key]
	items.RUnlock()
	return value
}
func ThreadSafeWrite(key string, value int) {
	items.Lock()
	items.m[key] = value
	items.Unlock()
}

//this strategy generally works fine, however because locks allow acces only
//one procces at a time, the average amount of time spent waiting for locks to clear in a read write
//intensive application can increase dramatically with the numbver of concurrent processes
//virtual sharding reduces lock contention by splitting the underliying data structure into several individually lockable maps
type Shard struct {
	sync.RWMutex
	m map[string]interface{}
}

type ShardedMap []*Shard //a slice of shards

func NewShardedMap(nshards int) ShardedMap {
	shards := make([]*Shard, nshards)

	for i := 0; i < nshards; i++ {
		shard := make(map[string]interface{})
		shards[i] = &Shard{m: shard}
	}

	return shards
}

//sharded map has two unexported methods , used to calculate a key, and to retrive a key
func (m ShardedMap) getShardedKey(key string) int {
	checksum := sha1.Sum([]byte(key))
	hash := int(checksum[17]) //pick an arbitrary byte as the hash
	return hash % len(m)
}

func (m ShardedMap) getShard(key string) *Shard {
	index := m.getShardedKey(key)
	return m[index]
}

func (m ShardedMap) Get(key string) interface{} {
	shard := m.getShard(key)
	shard.Lock()
	defer shard.RUnlock()
	return shard.m[key]
}

func (m ShardedMap) Set(key string, value interface{}) {
	shard := m.getShard(key)
	shard.Lock()
	defer shard.RUnlock()
	shard.m[key] = value
}

func (m ShardedMap) Keys() []string {
	keys := make([]string, 0)

	mutex := sync.Mutex{}

	wg := sync.WaitGroup{}
	wg.Add(len(m))
	for _, shard := range m {
		go func(s *Shard) {
			s.RLock()

			for key := range s.m {
				mutex.Lock()
				keys = append(keys, key)
				mutex.Unlock()
			}
			s.RUnlock()
			wg.Done()
		}(shard)
	}
	wg.Wait()

	return keys
}

func main() {
	/*
			var v chan interface{}
			fmt.Println("init of function main")
			err := Stream(context.Background(), v)

			if err != nil {
				fmt.Println("error in function stream")
			}
		//this function have more functionality, if it fails 3 times the breaker breaks the function
		newfunc := Breaker(
			func(ctx context.Context) (string, error) {
				return "", errors.New("unexpected error")
			}, uint(5),
		)
		//only executes the function 5 times

			newfunc(context.TODO()) //for example
			newfunc(context.TODO()) //for example
			newfunc(context.TODO()) //for example
			newfunc(context.TODO()) //for example
			newfunc(context.TODO()) //for example
			newfunc(context.TODO()) //for example
			newfunc(context.TODO()) //for example
			newfunc(context.TODO()) //for example
			newfunc(context.TODO()) //for example
	*/
	/*
		wrapped := DebounceFirst(
			func(context.Context) (string, error) {
				return "all okey", nil
			}, time.Second*10,
		)
		fmt.Println(wrapped(context.TODO()))
		fmt.Println(wrapped(context.TODO()))
		fmt.Println(wrapped(context.TODO()))
		fmt.Println(wrapped(context.TODO()))
		fmt.Println(wrapped(context.TODO()))
		fmt.Println("whats gona happen")
	*/

	//retry test
	/*
		var count int = 4
		r := Retry(func(ctx context.Context) (string, error) {
			count--
			fmt.Println(count)
			if count >= 3 {
				return "intentianal fail", errors.New("error")
			} else {
				return "succes", nil
			}
		}, 3, time.Second)

		res, err := r(context.Background())
		fmt.Println(res, err)
	*/

	//throttle test
	/*
		fun := Throttle(func(context.Context) (string, error) {
			time.Sleep(time.Second)
			return "function terminated", nil
		}, 4, 2, time.Second*4)

		fmt.Println(fun(context.Background()))
		fmt.Println(fun(context.Background()))
		fmt.Println(fun(context.Background()))
		fmt.Println(fun(context.Background()))
		fmt.Println(fun(context.Background()))
		fmt.Println(fun(context.Background()))
	*/
	/*
		//timeout test
		ctx := context.Background()
		//put a timeout of 2 seconds, if the function spend more than two seconds the
		//the context will stop the execution
		ctxt, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		timeout := Timeout(func(string) (string, error) {
			time.Sleep(time.Second * 2)
			return "exit", nil
		})
		fmt.Println(timeout(ctxt, "useless"))
	*/
	//FUNNEL TEST
	/*
		sources := make([]<-chan int, 0) //create an empty channel slice
		for i := 0; i < 3; i++ {
			ch := make(chan int)
			sources = append(sources, ch) //create a channel and add to sources

			go func() {
				defer close(ch)
				for i := 1; i <= 5; i++ {
					ch <- i
					time.Sleep(time.Millisecond * 500)
				}
			}()
		}
		dest := Funnel(sources...)
		for d := range dest {
			fmt.Println(d)
		}
	*/
	//FAN OUT TEST
	/*
		source := make(chan int)
		dests := Split(source, 5)

		go func() {
			for i := 1; i <= 10; i++ {
				source <- i
			}
			close(source)
		}()
		var wg sync.WaitGroup
		wg.Add(len(dests))

		for i, ch := range dests {
			go func(i int, d <-chan int) {
				defer wg.Done()

				for val := range d {
					fmt.Printf("#%d got %d\n", i, val)
				}
			}(i, ch)
		}
		wg.Wait()
	*/
}
