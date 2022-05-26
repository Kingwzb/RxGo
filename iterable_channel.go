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
		next:          next,
		subscribers:   make([]subscriber, 0),
		opts:          opts,
		options:       parseOptions(opts...),
		newSubscriber: make(chan subscriber, 1),
	}
}

func (i *channelIterable) Observe(opts ...Option) <-chan Item {
	mergedOptions := append(i.opts, opts...)
	option := parseOptions(mergedOptions...)
	//fmt.Printf("iterChan Observe option: %+v\n", option)
	if !option.isConnectable() {
		return i.next
	}
	if option.isConnectOperation() {
		ctx := option.buildContext(emptyContext)
		i.connect(ctx)
		return nil
	}
	c := i.addSubscriber(option)
	return c
}

func (i *channelIterable) addSubscriber(option Option) chan Item {
	i.mutex.Lock()
	ch := i.options.buildChannel()
	if i.producerAlreadyCreated {
		i.mutex.Unlock()
		go func() {
			select {
			case <-option.buildContext(emptyContext).Done():
			case i.newSubscriber <- subscriber{options: option, channel: ch, ctx: option.buildContext(emptyContext)}:
			}
		}()
	} else {
		//fmt.Printf("adding new observer to %d on Observe\n", len(i.subscribers))
		i.subscribers = append(i.subscribers, subscriber{options: option, channel: ch, ctx: option.buildContext(emptyContext)})
		i.mutex.Unlock()
	}
	return ch
}

func (i *channelIterable) connect(ctx context.Context) {
	i.mutex.Lock()
	if !i.producerAlreadyCreated {
		i.producerAlreadyCreated = true
		i.mutex.Unlock()
		go i.produce(ctx)
		return
	}
	i.mutex.Unlock()
}

func (i *channelIterable) produce(ctx context.Context) {
	//fmt.Println("started producer")
	closeChan := false
	defer func() {
		i.mutex.Lock()
		if closeChan {
			for _, subscriber := range i.subscribers {
				//fmt.Printf("closing subscribe %d channel\n", idx)
				close(subscriber.channel)
			}
		}
		i.producerAlreadyCreated = false
		i.mutex.Unlock()
	}()

	unsubscribe := func(toRemove []int) {
		if len(toRemove) > 0 {
			//i.mutex.Lock()
			remaining := make([]subscriber, 0, len(i.subscribers)-len(toRemove))
			which := 0
			for idx, subscriber := range i.subscribers {
				if which >= len(toRemove) || idx != toRemove[which] {
					remaining = append(remaining, subscriber)
				} else {
					//fmt.Printf("closing subscribe %d channel\n", idx)
					close(subscriber.channel)
					which++
				}
			}
			i.subscribers = remaining
			//i.mutex.Unlock()
		}
	}
	deliver := func(item Item, ob *subscriber) (done bool, remove bool) {
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
		//fmt.Printf("sending item to %d observers %+v\n", len(i.subscribers), item)
		toRemove := []int{}
		for idx, subscriber := range i.subscribers {
			done, remove := deliver(item, &subscriber)
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
	if flag, val := i.options.sendLatestAsInitial(); flag {
		select {
		case item, ok := <-i.next:
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
	//fmt.Println("started producer for loop")
	for {
		select {
		case <-ctx.Done():
			return
		case subscriber, ok := <-i.newSubscriber:
			if !ok {
				return
			}
			if sendInitialValue && initialValue == nil {
				//fmt.Printf("delivering latest value: %+v\n", latestValue)
				if done, remove := deliver(latestValue, &subscriber); done || remove {
					return
				}
			}
			//fmt.Printf("adding observer to %d observers\n", len(i.subscribers))
			i.subscribers = append(i.subscribers, subscriber)
		case item, ok := <-i.next:
			if !ok {
				closeChan = true
				return
			}
			latestValue = item
			if done := deliverAll(item); done {
				return
			}
		case item := <-initialValue:
			initialValue = nil
			//fmt.Printf("sending initial value %+v\n", item)
			if done := deliverAll(item); done {
				return
			}
		}
	}
}
