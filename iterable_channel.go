package rxgo

import (
	"context"
	"sync"
)

type subscriber struct {
	options Option
	channel chan Item
	ctx     context.Context
}
type channelIterable struct {
	next                   <-chan Item
	opts                   []Option
	subscribers            []subscriber
	mutex                  sync.RWMutex
	producerAlreadyCreated bool
}

func newChannelIterable(next <-chan Item, opts ...Option) Iterable {
	return &channelIterable{
		next:        next,
		subscribers: make([]subscriber, 0),
		opts:        opts,
	}
}

func (i *channelIterable) Observe(opts ...Option) <-chan Item {
	mergedOptions := append(i.opts, opts...)
	option := parseOptions(mergedOptions...)

	if !option.isConnectable() {
		return i.next
	}

	if option.isConnectOperation() {
		i.connect(option.buildContext(emptyContext))
		return nil
	}

	ch := option.buildChannel()
	i.mutex.Lock()
	i.subscribers = append(i.subscribers, subscriber{options: option, channel: ch, ctx: option.buildContext(emptyContext)})
	i.mutex.Unlock()
	return ch
}

func (i *channelIterable) connect(ctx context.Context) {
	i.mutex.Lock()
	if !i.producerAlreadyCreated {
		go i.produce(ctx)
		i.producerAlreadyCreated = true
	}
	i.mutex.Unlock()
}

func (i *channelIterable) produce(ctx context.Context) {
	defer func() {
		i.mutex.RLock()
		for _, subscriber := range i.subscribers {
			close(subscriber.channel)
		}
		i.mutex.RUnlock()
	}()

	unsubscribe := func(toRemove []int) {
		if len(toRemove) > 0 {
			remaining := make([]subscriber, 0, len(i.subscribers)-len(toRemove))
			which := 0
			for idx, subscriber := range i.subscribers {
				if which >= len(toRemove) || idx != toRemove[which] {
					remaining = append(remaining, subscriber)
				} else {
					close(subscriber.channel)
					which++
				}
			}
			i.subscribers = remaining
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-i.next:
			if !ok {
				return
			}
			i.mutex.Lock()
			toRemove := []int{}
			for idx, subscriber := range i.subscribers {
				select {
				case <-subscriber.ctx.Done():
					toRemove = append(toRemove, idx)
				case subscriber.channel <- item:
				}
			}
			unsubscribe(toRemove)
			i.mutex.Unlock()

		}
	}
}
