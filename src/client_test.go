package main

import (
	"log"
	"net"
	"testing"
)

func TestReportLocalBlobs(t *testing.T) {
	// reportLocalBlobs()
}

func TestSlice(t *testing.T) {

}

func TestHttp(t *testing.T) {
	conn, err := net.Dial("tcp", "10.10.108.99:3389")
	if err != nil {
		log.Println("无法连接", err)
		return
	}

	a := conn.LocalAddr().(*net.TCPAddr).IP
	log.Println(a)
	defer conn.Close()
}
