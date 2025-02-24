package rxgo

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// Amb takes several Observables, emit all of the items from only the first of these Observables
// to emit an item or notification.
func Amb(observables []Observable, opts ...Option) Observable {
	option := parseOptions(opts...)
	ctx := option.buildContext(emptyContext)
	next := option.buildChannel()
	once := sync.Once{}

	f := func(o Observable) {
		it := o.Observe(opts...)

		select {
		case <-ctx.Done():
			return
		case item, ok := <-it:
			if !ok {
				return
			}
			once.Do(func() {
				defer close(next)
				if item.Error() {
					next <- item
					return
				}
				next <- item
				for {
					select {
					case <-ctx.Done():
						return
					case item, ok := <-it:
						if !ok {
							return
						}
						if item.Error() {
							next <- item
							return
						}
						next <- item
					}
				}
			})
		}
	}

	for _, o := range observables {
		go f(o)
	}

	return &ObservableImpl{
		iterable: newChannelIterable(next),
	}
}

// CombineLatest combines the latest item emitted by each Observable via a specified function
// and emit items based on the results of this function.
func CombineLatest(f FuncN, observables []Observable, opts ...Option) Observable {
	option := parseOptions(opts...)
	ctx := option.buildContext(emptyContext)
	path := ctx.Value("path")
	params := ctx.Value("params")
	next := option.buildChannel()

	go func() {
		size := uint32(len(observables))
		var counter uint32 = 0
		s := make([]interface{}, size)
		ready := make([]bool, size)
		mutex := sync.Mutex{}
		wg := sync.WaitGroup{}
		wg.Add(int(size))
		errCh := make(chan struct{})

		printNotReady := func() {
			notReady := []interface{}{}
			for i, f := range ready {
				if !f {
					if params != nil {
						notReady = append(notReady, params.([]string)[i])
					} else {
						notReady = append(notReady, i+1)
					}
				}
			}
			if len(notReady) > 0 {
				fmt.Printf("%v combinedLatest items not ready %d/%d (%v)\n", path, len(notReady), size, notReady)
			}
		}
		defer printNotReady()
		handler := func(ctx context.Context, it Observable, i int) {
			var param interface{}
			if params != nil {
				param = params.([]string)[i]
			}
			defer wg.Done()
			if option.toLogTracePath() {
				fmt.Printf("%v/%v combinedLatest handler %d/%d started\n", path, param, i+1, size)
			}
			//observe := it.Observe(append(opts, WithPublishStrategyAs(false))...)
			var observe <-chan Item
			if it == nil {
				c := make(chan Item, 1)
				c <- Of(nil)
				observe = c
			} else {
				if option.toLogTracePath() {
					fmt.Printf("%v/%v combinedLatest handler %d/%d creating observe channel\n", path, param, i+1, size)
				}
				observe = it.Observe(WithContext(context.WithValue(ctx, "path", fmt.Sprintf("%v/%v", path, param))), WithBufferedChannel(1),
					WithLogTracePath(option.toLogTracePath()))
				if option.toLogTracePath() {
					fmt.Printf("%v/%v combinedLatest handler %d/%d created observe channel\n", path, param, i+1, size)
				}
			}
			for {
				if option.toLogTracePath() {
					fmt.Printf("%v/%v combinedLatest handler %d/%d select\n", path, param, i+1, size)
				}
				select {
				case <-ctx.Done():
					if option.toLogTracePath() {
						fmt.Printf("%v/%v combinedLatest handler %d/%d exit\n", path, param, i+1, size)
					}
					return
				case item, ok := <-observe:
					if !ok {
						if option.toLogTracePath() {
							fmt.Printf("%v/%v combinedLatest handler %d/%d done\n", path, param, i+1, size)
						}
						return
					}
					if option.toLogTracePath() {
						fmt.Printf("%v/%v combinedLatest handler %d/%d (%d) received %+v\n", path, param, atomic.LoadUint32(&counter), size, i+1, item)
					}
					if item.Error() {
						next <- item
						errCh <- struct{}{}
						return
					}
					mutex.Lock()
					if !ready[i] {
						if atomic.AddUint32(&counter, 1) == size {
							fmt.Printf("%v combinedLatest is ready now\n", path)
						}
					}
					ready[i] = true
					/*if option.toLogTracePath() {
						fmt.Printf("%v combinedLatest handler %d/%d (%d) items ready\n", path, counter, size, i+1)
					}*/
					s[i] = item.V
					if atomic.LoadUint32(&counter) == size {
						newData := make([]interface{}, len(s))
						copy(newData, s)
						vs := Of(f(newData...))
						mutex.Unlock()
						if option.toLogTracePath() {
							fmt.Printf("%v combinedLatest handler %d/%d (%d) sending %+v\n", path, atomic.LoadUint32(&counter), size, i+1, vs)
						}
						next <- vs
						if option.toLogTracePath() {
							fmt.Printf("%v combinedLatest handler %d/%d (%d) sent %+v\n", path, atomic.LoadUint32(&counter), size, i+1, vs)
						}
					} else {
						if option.toLogTracePath() {
							fmt.Printf("%v combinedLatest not ready yet\n", path)
							printNotReady()
						}
						mutex.Unlock()
					}
				}
			}
		}

		ctx, cancel := context.WithCancel(ctx)
		for i, o := range observables {
			go handler(ctx, o, i)
		}

		go func() {
			for range errCh {
				cancel()
			}
		}()

		wg.Wait()
		if option.toLogTracePath() {
			fmt.Printf("%v combinedLatest exit\n", path)
		}
		close(next)
		close(errCh)
	}()

	return &ObservableImpl{
		parent:   ctx,
		iterable: newChannelIterable(next, opts...),
	}
}

// Concat emits the emissions from two or more Observables without interleaving them.
func Concat(observables []Observable, opts ...Option) Observable {
	option := parseOptions(opts...)
	ctx := option.buildContext(emptyContext)
	next := option.buildChannel()

	go func() {
		defer close(next)
		for _, obs := range observables {
			//fmt.Printf("Concat start loop on ob %d\n", idx)
			//observe := obs.Observe(append(opts, WithPublishStrategyAs(false))...)
			observe := obs.Observe()
		loop:
			for {
				select {
				case <-ctx.Done():
					//fmt.Printf("Concat exit\n")
					return
				case item, ok := <-observe:
					//fmt.Printf("Concat received %+v\n", item)
					if !ok {
						//fmt.Printf("Concat break loop\n")
						break loop
					}
					if item.Error() {
						next <- item
						return
					}
					next <- item
				}
			}
		}
	}()
	return &ObservableImpl{
		iterable: newChannelIterable(next, opts...),
	}
}

// Create creates an Observable from scratch by calling observer methods programmatically.
func Create(f []Producer, opts ...Option) Observable {
	return &ObservableImpl{
		iterable: newCreateIterable(f, opts...),
	}
}

// Defer does not create the Observable until the observer subscribes,
// and creates a fresh Observable for each observer.
func Defer(f []Producer, opts ...Option) Observable {
	return &ObservableImpl{
		iterable: newDeferIterable(f, opts...),
	}
}

// Empty creates an Observable with no item and terminate immediately.
func Empty() Observable {
	next := make(chan Item)
	close(next)
	return &ObservableImpl{
		iterable: newChannelIterable(next),
	}
}

// FromChannel creates a cold observable from a channel.
func FromChannel(next <-chan Item, opts ...Option) Observable {
	option := parseOptions(opts...)
	ctx := option.buildContext(emptyContext)
	return &ObservableImpl{
		parent:   ctx,
		iterable: newChannelIterable(next, opts...),
	}
}

// FromEventSource creates a hot observable from a channel.
func FromEventSource(next <-chan Item, opts ...Option) Observable {
	option := parseOptions(opts...)

	return &ObservableImpl{
		iterable: newEventSourceIterable(option.buildContext(emptyContext), next, option.getBackPressureStrategy(), opts...),
	}
}

// Interval creates an Observable emitting incremental integers infinitely between
// each given time interval.
func Interval(interval Duration, opts ...Option) Observable {
	option := parseOptions(opts...)
	next := option.buildChannel()
	ctx := option.buildContext(emptyContext)

	go func() {
		i := 0
		for {
			select {
			case <-time.After(interval.duration()):
				if !Of(i).SendContext(ctx, next) {
					return
				}
				i++
			case <-ctx.Done():
				close(next)
				return
			}
		}
	}()
	return &ObservableImpl{
		iterable: newEventSourceIterable(ctx, next, option.getBackPressureStrategy()),
	}
}

// Just creates an Observable with the provided items.
func Just(items ...interface{}) func(opts ...Option) Observable {
	return func(opts ...Option) Observable {
		return &ObservableImpl{
			iterable: newJustIterable(items...)(opts...),
		}
	}
}

// JustItem creates a single from one item.
func JustItem(item interface{}, opts ...Option) Single {
	return &SingleImpl{
		iterable: newJustIterable(item)(opts...),
	}
}

// Merge combines multiple Observables into one by merging their emissions
func Merge(observables []Observable, opts ...Option) Observable {
	option := parseOptions(opts...)
	ctx := option.buildContext(emptyContext)
	next := option.buildChannel()
	wg := sync.WaitGroup{}
	wg.Add(len(observables))

	f := func(o Observable) {
		defer wg.Done()
		observe := o.Observe(opts...)
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-observe:
				if !ok {
					return
				}
				if item.Error() {
					next <- item
					return
				}
				next <- item
			}
		}
	}

	for _, o := range observables {
		go f(o)
	}

	go func() {
		wg.Wait()
		close(next)
	}()
	return &ObservableImpl{
		iterable: newChannelIterable(next),
	}
}

// Never creates an Observable that emits no items and does not terminate.
func Never() Observable {
	next := make(chan Item)
	return &ObservableImpl{
		iterable: newChannelIterable(next),
	}
}

// Range creates an Observable that emits count sequential integers beginning
// at start.
func Range(start, count int, opts ...Option) Observable {
	if count < 0 {
		return Thrown(IllegalInputError{error: "count must be positive"})
	}
	if start+count-1 > math.MaxInt32 {
		return Thrown(IllegalInputError{error: "max value is bigger than math.MaxInt32"})
	}
	return &ObservableImpl{
		iterable: newRangeIterable(start, count, opts...),
	}
}

// Start creates an Observable from one or more directive-like Supplier
// and emits the result of each operation asynchronously on a new Observable.
func Start(fs []Supplier, opts ...Option) Observable {
	option := parseOptions(opts...)
	next := option.buildChannel()
	ctx := option.buildContext(emptyContext)

	go func() {
		defer close(next)
		for _, f := range fs {
			select {
			case <-ctx.Done():
				return
			case next <- f(ctx):
			}
		}
	}()

	return &ObservableImpl{
		iterable: newChannelIterable(next),
	}
}

// Thrown creates an Observable that emits no items and terminates with an error.
func Thrown(err error) Observable {
	next := make(chan Item, 1)
	next <- Error(err)
	close(next)
	return &ObservableImpl{
		iterable: newChannelIterable(next),
	}
}

// Timer returns an Observable that completes after a specified delay.
func Timer(d Duration, opts ...Option) Observable {
	option := parseOptions(opts...)
	next := make(chan Item, 1)
	ctx := option.buildContext(emptyContext)

	go func() {
		defer close(next)
		select {
		case <-ctx.Done():
			return
		case <-time.After(d.duration()):
			return
		}
	}()
	return &ObservableImpl{
		iterable: newChannelIterable(next),
	}
}
