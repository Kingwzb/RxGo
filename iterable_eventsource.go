package rxgo

import (
	"context"
	"sync"
)

type observer struct {
	options Option
	channel chan Item
}
type eventSourceIterable struct {
	sync.RWMutex
	observers   []observer
	disposed    bool
	opts        []Option
	newObserver chan observer
}

func newEventSourceIterable(ctx context.Context, next <-chan Item, strategy BackpressureStrategy, opts ...Option) Iterable {
	it := &eventSourceIterable{
		observers:   make([]observer, 0),
		opts:        opts,
		newObserver: make(chan observer),
	}

	go func() {
		defer func() {
			it.closeAllObservers()
		}()

		deliver := func(item Item, ob *observer) (done bool) {
			it.RLock()
			defer it.RUnlock()

			switch strategy {
			default:
				fallthrough
			case Block:
				if ob != nil {
					if !item.SendContext(ctx, ob.channel) {
						return true
					}
				} else {
					for _, observer := range it.observers {
						if !item.SendContext(ctx, observer.channel) {
							return true
						}
					}
				}
			case Drop:
				if ob != nil {
					select {
					default:
					case <-ctx.Done():
						return true
					case ob.channel <- item:
					}
				} else {
					for _, observer := range it.observers {
						select {
						default:
						case <-ctx.Done():
							return true
						case observer.channel <- item:
						}
					}
				}
			}
			return
		}

		var latestValue Item
		hasLatestValue := false
		for {
			select {
			case <-ctx.Done():
				return
			case observer, ok := <-it.newObserver:
				if !ok {
					return
				}
				if flag, initValue := observer.options.sendLatestAsInitial(); flag {
					if hasLatestValue {
						if done := deliver(latestValue, &observer); done {
							return
						}
					} else {
						if done := deliver(Of(initValue), &observer); done {
							return
						}
					}
				}
				it.Lock()
				it.observers = append(it.observers, observer)
				it.Unlock()
			case item, ok := <-next:
				if !ok {
					return
				}
				latestValue = item
				hasLatestValue = true
				if done := deliver(item, nil); done {
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
	close(i.newObserver)
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
		i.newObserver <- observer{options: option, channel: next}
	}
	i.RUnlock()
	return next
}
