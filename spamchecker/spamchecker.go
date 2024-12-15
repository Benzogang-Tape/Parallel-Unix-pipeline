package main

import (
	"cmp"
	"fmt"
	"log"
	"slices"
	"sync"
)

func RunPipeline(cmds ...cmd) {
	in := make(chan interface{})
	wg := &sync.WaitGroup{}
	commandRunner := func(in, out chan interface{}, commandIdx int, group *sync.WaitGroup) {
		defer group.Done()
		defer close(out)
		cmds[commandIdx](in, out)
	}

	for cmdIdx := range cmds {
		wg.Add(1)
		out := make(chan interface{})
		go commandRunner(in, out, cmdIdx, wg)
		in = out
	}
	wg.Wait()
}

func SelectUsers(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	uniqueUsers := make(map[string]struct{}, 10)
	getUserByEmail := func(email interface{}) (User, error) {
		emailString, ok := email.(string)
		if !ok {
			return User{}, fmt.Errorf("SelectUsers: failed to convert to type %T", "")
		}
		return GetUser(emailString), nil

	}
	passUniqueUsers := func(email interface{}, group *sync.WaitGroup) {
		defer group.Done()
		user, err := getUserByEmail(email)
		if err != nil {
			log.Println(err)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if _, isAlias := uniqueUsers[user.Email]; !isAlias {
			uniqueUsers[user.Email] = struct{}{}
			out <- user
		}
	}

	for email := range in {
		wg.Add(1)
		go passUniqueUsers(email, wg)
	}
	wg.Wait()
}

func SelectMessages(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	usersBatch := make([]User, 0, GetMessagesMaxUsersBatch)
	for user := range in {
		userStruct, ok := user.(User)
		if !ok {
			log.Printf("SelectMessages: failed to convert to type: %T\n", User{})
			continue
		}
		usersBatch = append(usersBatch, userStruct)
		if len(usersBatch) < GetMessagesMaxUsersBatch {
			continue
		}
		wg.Add(1)
		go getMessagesByBatch(slices.Clone(usersBatch), out, wg)
		usersBatch = usersBatch[:0]
	}
	if len(usersBatch) != 0 {
		wg.Add(1)
		go getMessagesByBatch(slices.Clone(usersBatch), out, wg)
	}
	wg.Wait()
}

func getMessagesByBatch(batch []User, out chan<- interface{}, group *sync.WaitGroup) {
	defer group.Done()
	messages, err := GetMessages(batch...)
	if err != nil {
		log.Printf("GetMessages func returned error: %s\n", err.Error())
		return
	}
	for _, message := range messages {
		out <- message
	}
}

func CheckSpam(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	for i := 0; i < HasSpamMaxAsyncRequests; i++ {
		wg.Add(1)
		go checkSpamWorker(wg, in, out)
	}
	wg.Wait()
}

func checkSpamWorker(group *sync.WaitGroup, in, out chan interface{}) {
	defer group.Done()
	for msgID := range in {
		message, ok := msgID.(MsgID)
		if !ok {
			log.Printf("CheckSpam: failed to convert to type: %T\n", MsgID(0))
			return
		}
		hasSpam, err := HasSpam(message)
		if err != nil {
			log.Printf("HasSpam func returned error: %s", err.Error())
			return
		}
		out <- MsgData{message, hasSpam}
	}
}

func CombineResults(in, out chan interface{}) {
	messages := make([]MsgData, 0, 42)
	for msgData := range in {
		messageStruct, ok := msgData.(MsgData)
		if !ok {
			log.Printf("CombineResults: failed to convert to type: %T", MsgData{})
			continue
		}
		messages = append(messages, messageStruct)
	}
	slices.SortFunc(messages, func(a, b MsgData) int {
		if a.HasSpam && !b.HasSpam {
			return -1
		}
		if !a.HasSpam && b.HasSpam {
			return 1
		}
		return cmp.Compare(a.ID, b.ID)
	})
	for _, msg := range messages {
		out <- fmt.Sprintf("%t %v", msg.HasSpam, msg.ID)
	}
}
