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
					toRemove := []int{}
					for idx, observer := range it.observers {
						select {
						case <-observer.ctx.Done():
							toRemove = append(toRemove, idx)
						default:
							if !item.SendContext(ctx, observer.channel) {
								return true
							}
						}
					}
					unsubscribe(toRemove)
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
					toRemove := []int{}
					for idx, observer := range it.observers {
						select {
						default:
						case <-ctx.Done():
							return true
						case <-observer.ctx.Done():
							toRemove = append(toRemove, idx)
						case observer.channel <- item:
						}
					}
					unsubscribe(toRemove)
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
		i.newObserver <- observer{options: option, channel: next, ctx: option.buildContext(context.Background())}
	}
	i.RUnlock()
	return next
}
