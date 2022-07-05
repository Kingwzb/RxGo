package rxgo

import (
	"context"
	"fmt"
	"sync"
)

type subscriber struct {
	options Option
	channel chan Item
	ctx     context.Context
}
type channelIterable struct {
	next          <-chan Item
	opts          []Option
	options       Option
	ctx           context.Context
	producerCtx   context.Context
	latestVal     Item
	hasLatestVal  bool
	subscribers   []subscriber
	newSubscriber chan subscriber
	mutex         sync.RWMutex
}

func newChannelIterable(next <-chan Item, opts ...Option) Iterable {
	option := parseOptions(opts...)
	return &channelIterable{
		next:          next,
		subscribers:   make([]subscriber, 0),
		opts:          opts,
		options:       option,
		ctx:           option.buildContext(emptyContext),
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
		//fmt.Println("doing connect operation")
		ctx := option.buildContext(emptyContext)
		i.connect(ctx)
		return nil
	}
	c := i.addSubscriber(option)
	return c
}

func (i *channelIterable) addSubscriber(option Option) chan Item {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	ch := i.options.buildChannel()
	if i.producerCtx != nil {
		//fmt.Println("sending new subscriber to newSubscriber channel")
		go func() {
			select {
			case <-option.buildContext(emptyContext).Done():
			case i.newSubscriber <- subscriber{options: option, channel: ch, ctx: option.buildContext(emptyContext)}:
				//fmt.Println("sent new subscriber to newSubscriber channel")
			}
		}()
	} else {
		//fmt.Println("sending new subscriber to subscribers list")
		ctx := option.buildContext(emptyContext)
		//fmt.Printf("adding new observer %v to %d on Observe\n", ctx.Value("path"), len(i.subscribers))
		i.subscribers = append(i.subscribers, subscriber{options: option, channel: ch, ctx: ctx})
	}
	return ch
}

func (i *channelIterable) connect(ctx context.Context) {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	if i.producerCtx == nil {
		i.producerCtx = ctx
		go i.produce(ctx)
	} else {
		select {
		case <-i.producerCtx.Done(): // previous was disconnected
			i.producerCtx = ctx
			go i.produce(ctx)
		default:
			//fmt.Println("producer is not done yet, do nothing at connect")
		}
	}
}

func (i *channelIterable) produce(ctx context.Context) {
	if i.options.toLogTracePath() {
		fmt.Printf("started producer for %v\n", ctx.Value("path"))
	}
	defer func() {
		if i.options.toLogTracePath() {
			fmt.Printf("stopped producer for %v\n", ctx.Value("path"))
		}
		i.mutex.Lock()
		/*for _, subscriber := range i.subscribers {
			//fmt.Printf("closing subscribe %d channel\n", idx)
			close(subscriber.channel)
		}*/
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
					if i.options.toLogTracePath() {
						fmt.Printf("closing subscribe %d channel\n", idx)
					}
					close(subscriber.channel)
					which++
				}
			}
			i.subscribers = remaining
			if i.options.toLogTracePath() {
				fmt.Printf("removed %v observers for %v, %v remaining\n", len(toRemove), ctx.Value("path"), len(i.subscribers))
			}
			//i.mutex.Unlock()
		}
	}
	deliver := func(item Item, ob *subscriber) (done bool, remove bool) {
		logTracePath := i.options.toLogTracePath() || ob.options.toLogTracePath()
		strategy := ob.options.getBackPressureStrategy()
		if logTracePath {
			fmt.Printf("observer %v deliver item in mode %v for %v\n", ob.ctx.Value("path"), strategy, ctx.Value("path"))
		}
		switch strategy {
		default:
			fallthrough
		case Block:
			select {
			case <-ob.ctx.Done():
				if logTracePath {
					fmt.Printf("observer %v ctx is done for %v\n", ob.ctx.Value("path"), ctx.Value("path"))
				}
				return false, true
			default:
				if logTracePath {
					fmt.Printf("sending to observer %v for %v: %+v\n", ob.ctx.Value("path"), item, ctx.Value("path"))
				}
				if !item.SendContext(ctx, ob.channel) {
					fmt.Printf("failed to send to %v for %v: %v\n", ob.ctx.Value("path"), item, ctx.Value("path"))
					return true, false
				}
				if logTracePath {
					fmt.Printf("sent to %v for %v: %v\n", ob.ctx.Value("path"), item, ctx.Value("path"))
				}
				return false, false
			}
		case Drop:
			select {
			default:
				if logTracePath {
					fmt.Printf("failed to send item to observer %v for %v in drop mode : %+v\n", ob.ctx.Value("path"), ctx.Value("path"), item)
				}
			case <-ob.ctx.Done():
				if logTracePath {
					fmt.Printf("observer %v ctx for %v is done\n", ob.ctx.Value("path"), ctx.Value("path"))
				}
				return false, true
			case <-ctx.Done():
				if logTracePath {
					fmt.Printf("ctx is done for %v\n", ctx.Value("path"))
				}
				return true, false
			case ob.channel <- item:
				if logTracePath {
					fmt.Printf("delivered to %v for %v: %+v\n", ob.ctx.Value("path"), ctx.Value("path"), item)
				}
			}
		}
		return false, false
	}
	deliverAll := func(item Item) (done bool) {
		subPaths := []interface{}{}
		for _, sub := range i.subscribers {
			subPaths = append(subPaths, sub.ctx.Value("path"))
		}
		if i.options.toLogTracePath() {
			defer fmt.Printf("%v done sending item to %d observers %+v\n", ctx.Value("path"), len(i.subscribers), item)
		}
		toRemove := []int{}
		i.mutex.Lock()
		defer i.mutex.Unlock()
		if i.options.toLogTracePath() {
			fmt.Printf("sending item to %d observers %+v for %v: %+v\n", len(i.subscribers), subPaths, ctx.Value("path"), item)
		}
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
	initialValue := make(chan Item, 1)
	if flag, val := i.options.sendLatestAsInitial(); flag {
		select {
		case item, ok := <-i.next:
			if !ok {
				return
			}
			if item.E == nil {
				if i.options.toLogTracePath() {
					fmt.Printf("using option's initial value %+v for %v\n", item.V, ctx.Value("path"))
				}
				i.latestVal = Of(item.V)
				i.hasLatestVal = true
			}
		default:
			if !i.hasLatestVal && val != nil {
				i.hasLatestVal = true
				i.latestVal = Of(val)
			}
		}
		if i.hasLatestVal {
			fmt.Printf("using latestVal %+v as initial value for %v\n", i.latestVal, ctx.Value("path"))
			initialValue <- i.latestVal
		}
	}
	if i.options.toLogTracePath() {
		fmt.Printf("started producer for %v\n", ctx.Value("path"))
	}
	for {
		select {
		case <-ctx.Done():
			return
		case subscriber, ok := <-i.newSubscriber:
			if !ok {
				return
			}
			if i.hasLatestVal && initialValue == nil {
				if i.options.toLogTracePath() {
					fmt.Printf("delivering latestVal %+v to new subscriber %v for %v\n", i.latestVal, subscriber.ctx.Value("path"), ctx.Value("path"))
				}
				if done, remove := deliver(i.latestVal, &subscriber); done || remove {
					if i.options.toLogTracePath() {
						fmt.Printf("failed to deliver latestVal %+v to new subscriber %v for %v\n", i.latestVal, subscriber.ctx.Value("path"), ctx.Value("path"))
					}
					return
				}
			}
			if i.options.toLogTracePath() {
				fmt.Printf("added observer %v to %d observers for %v\n", subscriber.ctx.Value("path"), len(i.subscribers), ctx.Value("path"))
			}
			i.subscribers = append(i.subscribers, subscriber)
		case item, ok := <-i.next:
			if !ok {
				return
			}
			if i.options.toLogTracePath() {
				fmt.Printf("received latestVal %+v for %v\n", item, ctx.Value("path"))
			}
			i.hasLatestVal = true
			i.latestVal = item
			if i.options.toLogTracePath() {
				fmt.Printf("set latestVal1 %+v for %v\n", item, ctx.Value("path"))
			}
			initialValue = nil
			if done := deliverAll(item); done {
				return
			}
		case item, ok := <-initialValue:
			if ok {
				if i.options.toLogTracePath() {
					fmt.Printf("received initial value(latestVal2) %+v for %v\n", item, ctx.Value("path"))
				}
				initialValue = nil
				if i.options.toLogTracePath() {
					fmt.Printf("set latestVal2 %+v for %v\n", item, ctx.Value("path"))
				}
				i.hasLatestVal = true
				i.latestVal = item
				if i.options.toLogTracePath() {
					fmt.Printf("sending initial value %+v for %v\n", item, ctx.Value("path"))
				}
				if done := deliverAll(item); done {
					return
				}
			}
		}
	}
}
