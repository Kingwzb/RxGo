package rxgo

import (
	"context"
	"sync"
)

type observer struct {
	options Option
	channel chan Item
	ctx     context.Context
}
type eventSourceIterable struct {
	sync.RWMutex
	ctx         context.Context
	observers   []observer
	disposed    bool
	opts        []Option
	options     Option
	newObserver chan observer
}

func newEventSourceIterable(ctx context.Context, next <-chan Item, strategy BackpressureStrategy, opts ...Option) Iterable {
	it := &eventSourceIterable{
		ctx:         ctx,
		observers:   make([]observer, 0),
		opts:        opts,
		options:     parseOptions(opts...),
		newObserver: make(chan observer, 1),
	}

	go func() {
		defer func() {
			it.closeAllObservers()
		}()

		unsubscribe := func(toRemove []int) {
			if len(toRemove) > 0 {
				remaining := make([]observer, 0, len(it.observers)-len(toRemove))
				which := 0
				for idx, observer := range it.observers {
					if which >= len(toRemove) || idx != toRemove[which] {
						remaining = append(remaining, observer)
					} else {
						close(observer.channel)
						which++
					}
				}
				it.observers = remaining
			}
		}
		deliver := func(item Item, ob *observer) (done bool, remove bool) {
			strategy := ob.options.getBackPressureStrategy()
			//fmt.Printf("deliver item in mode %v\n", strategy)
			switch strategy {
			default:
				fallthrough
			case Block:
				select {
				case <-ob.ctx.Done():
					return false, true
				default:
					if !item.SendContext(ctx, ob.channel) {
						//fmt.Printf("failed to send 1\n")
						return true, false
					}
					return false, false
				}
			case Drop:
				select {
				default:
					//fmt.Printf("failed to send item to one observer in drop mode : %+v\n", item)
				case <-ob.ctx.Done():
					return false, true
				case <-ctx.Done():
					//fmt.Printf("drop done\n")
					return true, false
				case ob.channel <- item:
					//fmt.Printf("delivered item to one observer in drop mode : %+v\n", item)
				}
			}
			return false, false
		}
		deliverAll := func(item Item) (done bool) {
			//fmt.Printf("sending item to %d observers %+v\n", len(it.observers), item)
			toRemove := []int{}
			for idx, observer := range it.observers {
				done, remove := deliver(item, &observer)
				if done {
					return true
				}
				if remove {
					toRemove = append(toRemove, idx)
				}
			}
			unsubscribe(toRemove)
			return false
		}

		var latestValue Item
		sendInitialValue := false
		initialValue := make(chan Item, 1)
		if flag, val := it.options.sendLatestAsInitial(); flag {
			select {
			case item, ok := <-next:
				if !ok {
					return
				}
				if item.E != nil {
					latestValue = Error(item.E)
				} else {
					latestValue = Of(item.V)
				}
			default:
				latestValue = Of(val)
			}
			sendInitialValue = true
			initialValue <- latestValue
		}
		for {
			select {
			case <-ctx.Done():
				return
			case observer, ok := <-it.newObserver:
				if !ok {
					return
				}
				if sendInitialValue && initialValue == nil {
					//fmt.Printf("delivering latest value: %+v\n", latestValue)
					if done, remove := deliver(latestValue, &observer); done || remove {
						//fmt.Println("exit iterable_eventsource")
						return
					}
				}
				it.observers = append(it.observers, observer)
			case item, ok := <-next:
				if !ok {
					//fmt.Println("exit iterable_eventsource")
					return
				}
				if item.E != nil {
					latestValue = Error(item.E)
				} else {
					latestValue = Of(item.V)
				}
				if done := deliverAll(item); done {
					//fmt.Println("exit iterable_eventsource")
					return
				}
			case item := <-initialValue:
				initialValue = nil
				if done := deliverAll(item); done {
					//fmt.Println("exit iterable_eventsource")
					return
				}
			}
		}
	}()

	return it
}

func (i *eventSourceIterable) closeAllObservers() {
	i.Lock()
	for _, observer := range i.observers {
		close(observer.channel)
	}
	i.disposed = true
	i.Unlock()
}

func (i *eventSourceIterable) Observe(opts ...Option) <-chan Item {
	option := parseOptions(append(i.opts, opts...)...)
	next := option.buildChannel()

	i.RLock()
	if i.disposed {
		close(next)
	} else {
		go func() {
			select {
			case <-i.ctx.Done():
			case <-option.buildContext(emptyContext).Done():
			case i.newObserver <- observer{options: option, channel: next, ctx: option.buildContext(emptyContext)}:
			}
		}()
	}
	i.RUnlock()
	return next
}
