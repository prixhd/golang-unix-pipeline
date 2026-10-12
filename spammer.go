package main

import "sync"

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
	// 	in - string
	// 	out - User
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
	// 	in - User
	// 	out - MsgID
}

func CheckSpam(in, out chan interface{}) {
	// in - MsgID
	// out - MsgData
}

func CombineResults(in, out chan interface{}) {
	// in - MsgData
	// out - string
}
