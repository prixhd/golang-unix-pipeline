package main

import (
	"fmt"
	"sort"
	"sync"
)

func RunPipeline(cmds ...cmd) {
	var in chan interface{}

	for i, command := range cmds {
		out := make(chan interface{})

		if i == len(cmds)-1 {
			command(in, out)
		} else {
			go func(job cmd, input, output chan interface{}) {
				job(input, output)
				close(output)
			}(command, in, out)
			in = out
		}

	}

}

func SelectUsers(in, out chan interface{}) {

	var wg sync.WaitGroup
	seen := make(map[uint64]bool)
	var mu sync.Mutex

	for email := range in {
		wg.Add(1)
		go func(email interface{}) {
			defer wg.Done()
			user := GetUser(email.(string))
			var isNew bool

			mu.Lock()
			if !seen[user.ID] {
				seen[user.ID] = true
				isNew = true
			}
			mu.Unlock()

			if isNew {
				out <- user
			}

		}(email)
	}
	wg.Wait()

}

func SelectMessages(in, out chan interface{}) {
	var wg sync.WaitGroup
	batch := make([]User, 0, GetMessagesMaxUsersBatch)

	processBatch := func(users []User) {
		wg.Add(1)

		go func() {
			defer wg.Done()

			messages, err := GetMessages(users...)
			if err != nil {
				return
			}

			for _, msgID := range messages {
				out <- msgID
			}
		}()
	}

	for rawUser := range in {
		user := rawUser.(User)
		batch = append(batch, user)

		if len(batch) == GetMessagesMaxUsersBatch {
			processBatch(batch)
			batch = make([]User, 0, GetMessagesMaxUsersBatch)
		}
	}

	if len(batch) > 0 {
		processBatch(batch)
	}

	wg.Wait()
}

func CheckSpam(in, out chan interface{}) {
	var wg sync.WaitGroup

	sem := make(chan struct{}, HasSpamMaxAsyncRequests)

	for rawMsg := range in {
		msgID := rawMsg.(MsgID)

		sem <- struct{}{}
		wg.Add(1)

		go func(id MsgID) {
			defer wg.Done()
			defer func() {
				<-sem
			}()

			hasSpam, err := HasSpam(id)
			if err != nil {
				return
			}

			out <- MsgData{
				ID:      id,
				HasSpam: hasSpam,
			}
		}(msgID)
	}

	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	results := make([]MsgData, 0)

	for rawMsg := range in {
		msg := rawMsg.(MsgData)
		results = append(results, msg)
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].HasSpam != results[j].HasSpam {
			return results[i].HasSpam
		}

		return results[i].ID < results[j].ID
	})

	for _, msg := range results {
		out <- fmt.Sprintf("%t %d", msg.HasSpam, msg.ID)
	}
}
