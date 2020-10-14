package main

import (
	"io"
	"log"
	"net/http"
	"os"
	"testing"
)

const BUF_LEN = 4 * 1024 * 1024

func fileClient() {
	response, err := http.Get("http://10.10.108.60:5000/v2/centos/blobs/sha256:3c72a8ed68140139e483fe7368ae4d9651422749e91483557cbd5ecf99a96110")
	if err != nil {
		log.Println("请求url失败", err)
		return
	}
	log.Printf("Connected to %v\n", response.Request.URL)
	err = os.MkdirAll(`13`, 0666)
	if err != nil {
		log.Println("cannot create dir, ", err)
	}
	f, err := os.OpenFile(`myblob`, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644) // 第一个选项是截断(追加的反义词)
	if err != nil {
		log.Println("创建新文件失败", err)
		return
	}
	defer f.Close()
	buffer := [BUF_LEN]byte{}
	for true {
		n, err := response.Body.Read(buffer[:])
		if err != nil {
			if err == io.EOF {
				log.Println("下载文件结束")
				f.Write(buffer[:n])
				break
			} else {
				log.Println("下载文件失败", err)
				return
			}
		}
		log.Printf("writing %v bytes\n", n)
		n, err = f.Write(buffer[:n])
		//if n < BUF_LEN {
		//	break
		//}
	}
}

func TestDownload(t *testing.T) {
	fileClient()
}
