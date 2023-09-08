package kvraft

import (
	"fmt"
	"time"
)

type SampleStruct struct {
	Value string
}

func sfs() {
	a := &SampleStruct{Value: ""}
	go func() {
		for {
			fmt.Printf("%+v %v", a.Value, time.Now().Unix())
		}
	}()
	for {
		go func() {
			a.Value = ""
		}()
		time.Sleep(time.Microsecond)
		go func() {
			a.Value = "value"
		}()
		time.Sleep(time.Microsecond)
	}
}
