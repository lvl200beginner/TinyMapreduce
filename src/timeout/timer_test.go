package timeout

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

func TestCode(t *testing.T) {

	ch := make(chan struct{}, 1)

	go createroutine(ch, 1)
	go createroutine(ch, 4)
	go createroutine(ch, 6)
	go createroutine(ch, 12)
	go createroutine(ch, 23)
	println("Goroutine数量：", runtime.NumGoroutine())
	for {
		now := time.Now()
		select {
		case <-ch:
			fmt.Println("业务逻辑执行完成！")
		case <-time.After(time.Second * 10):
			fmt.Println("超时...")
		}
		fmt.Printf("%f sec passed! \n", time.Since(now).Seconds())
	}
}

func resettimeout(ch chan struct{}) {
	println("reset start!")
	ch <- struct{}{}
	println("reset end!")
}

func createroutine(ch chan struct{}, sec time.Duration) {
	fmt.Printf("new routine!,sleep %d sec \n", sec)
	time.Sleep(sec * time.Second)
	resettimeout(ch)
}
