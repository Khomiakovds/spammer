package main

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
)

type cmd func(in, out chan interface{})

type Stat struct {
	RunGetUser            uint32
	RunGetMessages        uint32
	GetMessagesTotalUsers uint32
	RunHasSpam            uint32
}

var stat Stat

func RunPipeline(cmds ...cmd) {
	var wg sync.WaitGroup
	channels := make([]chan interface{}, len(cmds)+1)
	for i := range channels {
		channels[i] = make(chan interface{})
	}

	
	for i, c := range cmds {
		wg.Add(1)
		go func(c cmd, in, out chan interface{}) {
			defer wg.Done()
			c(in, out)
			close(out)
		}(c, channels[i], channels[i+1])
	}

	go func() {
		wg.Wait()
		close(channels[0])
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range channels[len(channels)-1] {
		}
	}()
	wg.Wait()
}

type User struct {
	ID    uint64
	Email string
}

type MsgID struct {
	UserID uint64
	Msg    string
}

type MsgData struct {
	ID   MsgID
	Data string
}

func SelectUsers(in, out chan interface{}) {
	defer close(out)
	for data := range in {
		email := data.(string)
		hash := sha1.New()
		hash.Write([]byte(email))
		userID := big.NewInt(0).SetBytes(hash.Sum(nil)).Uint64()
		user := User{
			ID:    userID,
			Email: email,
		}
		out <- user
		atomic.AddUint32(&stat.RunGetUser, 1)
	}
}

func SelectMessages(in, out chan interface{}) {
	defer close(out)
	for data := range in {
		user := data.(User)
		for i := 0; i < 3; i++ {
			msg := MsgID{
				UserID: user.ID,
				Msg:    fmt.Sprintf("Message %d from %s", i+1, user.Email),
			}
			out <- msg
			atomic.AddUint32(&stat.RunGetMessages, 1)
		}
	}
}

func CheckSpam(in, out chan interface{}) {
	defer close(out)
	for data := range in {
		msg := data.(MsgID)
		spam := fmt.Sprintf("true %d", msg.UserID)
		out <- MsgData{
			ID:   msg,
			Data: spam,
		}
		atomic.AddUint32(&stat.RunHasSpam, 1)
	}
}

func CombineResults(in, out chan interface{}) {
	defer close(out)
	var results []string
	for data := range in {
		msgData := data.(MsgData)
		results = append(results, msgData.Data)
	}
	for _, result := range results {
		out <- result
	}
}