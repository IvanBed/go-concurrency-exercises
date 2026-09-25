//////////////////////////////////////////////////////////////////////
//
// Given is a producer-consumer scenario, where a producer reads in
// tweets from a mockstream and a consumer is processing the
// data. Your task is to change the code so that the producer as well
// as the consumer can run concurrently
//

package main

import (
	"fmt"
	"time"
)

func producer(stream Stream, tweets chan *Tweet, wg *sync.WaitGroup) () {
	defer func() {
		close(tweets)
		wg.Done()
	} () 

	for {
		tweet, err := stream.Next()
		if err == ErrEOF {
			break
		}
		tweets <- tweet
	}
}

func consumer(tweets chan *Tweet, wg *sync.WaitGroup) {
	defer wg.Done()
	for t := range tweets {
		if t.IsTalkingAboutGo() {
			fmt.Println(t.Username, "\ttweets about golang")
		} else {
			fmt.Println(t.Username, "\tdoes not tweet about golang")
		}
	}
}

func main() {
	var wg sync.WaitGroup
	wg.Add(2)

	start := time.Now()
	stream := GetMockStream()
    
	tweetsCh := make(chan *Tweet, 0)
	
	// Producer
	go producer(stream, tweetsCh, &wg)
	// Consumer
	go consumer(tweetsCh, &wg)
    wg.Wait()

	fmt.Printf("Process took %s\n", time.Since(start))
}
