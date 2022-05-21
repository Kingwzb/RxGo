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
	options                Option
	subscribers            []subscriber
	newSubscriber          chan subscriber
	mutex                  sync.RWMutex
	producerAlreadyCreated bool
}

func newChannelIterable(next <-chan Item, opts ...Option) Iterable {
	return &channelIterable{
		next:        next,
		subscribers: make([]subscriber, 0),
		opts:        opts,
		options:     parseOptions(opts...),
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
	if i.producerAlreadyCreated {
		i.newSubscriber <- subscriber{options: option, channel: ch, ctx: option.buildContext(context.Background())}
	} else {
		i.subscribers = append(i.subscribers, subscriber{options: option, channel: ch, ctx: option.buildContext(emptyContext)})
	}
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
	closeChan := false
	defer func() {
		i.mutex.RLock()
		if closeChan {
			for _, subscriber := range i.subscribers {
				close(subscriber.channel)
			}
		}
		i.producerAlreadyCreated = false
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
	deliver := func(item Item, ob *subscriber) (done bool) {
		i.mutex.RLock()
		defer i.mutex.RUnlock()

		switch i.options.getBackPressureStrategy() {
		default:
			fallthrough
		case Block:
			if ob != nil {
				if !item.SendContext(ctx, ob.channel) {
					return true
				}
			} else {
				toRemove := []int{}
				for idx, subscriber := range i.subscribers {
					select {
					case <-subscriber.ctx.Done():
						toRemove = append(toRemove, idx)
					default:
						if !item.SendContext(ctx, subscriber.channel) {
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
				for idx, observer := range i.subscribers {
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
	if flag, initValue := i.options.sendLatestAsInitial(); flag {
		func() {
			i.mutex.RLock()
			defer i.mutex.RUnlock()
			for _, subscriber := range i.subscribers {
				if done := deliver(Of(initValue), &subscriber); done {
					return
				}

			}
		}()
	}
	i.mutex.RUnlock()
	var latestValue Item
	hasLatestValue := false
	for {
		select {
		case <-ctx.Done():
			return
		case subscriber, ok := <-i.newSubscriber:
			if !ok {
				return
			}
			if flag, initValue := subscriber.options.sendLatestAsInitial(); flag {
				if hasLatestValue {
					subscriber.channel <- latestValue
				} else {
					subscriber.channel <- Of(initValue)
				}
			}
			i.mutex.Lock()
			i.subscribers = append(i.subscribers, subscriber)
			i.mutex.Unlock()
		case item, ok := <-i.next:
			if !ok {
				closeChan = true
				return
			}
			latestValue = item
			hasLatestValue = true
			if done := deliver(item, nil); done {
				return
			}
		}
	}
}
