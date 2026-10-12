package main

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
