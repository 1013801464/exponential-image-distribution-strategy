package main

import (
	"container/list"
	"fmt"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"runtime"
	"strings"
	"sync"
)

const PORT string = "13000"

func httpServer() {
	urlSuffix := ""
	if runtime.GOOS == "windows" {
		log.Println("this is windows")
		urlSuffix = "{\\?.*}"
	}
	rtr := mux.NewRouter()
	rtr.HandleFunc("/submit", func(writer http.ResponseWriter, request *http.Request) {
		ips := request.PostFormValue("ip")
		data := request.PostFormValue("data")
		submitImageInfo(ips, data)
	}).Methods("GET", "POST")
	rtr.HandleFunc("/request/{image:[0-9a-zA-Z:._\\-]+}/{file:[:\\.a-zA-z0-9]+}"+urlSuffix, func(writer http.ResponseWriter, request *http.Request) {
		params := mux.Vars(request)
		ips := request.URL.Query()["ip"][0]       // 字符串的IP
		result := newRequest(params["file"], ips) // 计算目标IP地址
		_, _ = writer.Write([]byte(result))       // TODO 将源IP：ips,文件名params["file"],目标ip：result，记录时间
	}).Methods("GET")
	rtr.HandleFunc("/complete/{file:[:.a-zA-z0-9]+}"+urlSuffix, func(writer http.ResponseWriter, request *http.Request) {
		params := mux.Vars(request)
		fileName := params["file"]
		query := request.URL.Query()
		log.Println("Got", request.URL)
		ip1s := query["ip1"][0]
		ip2s := query["ip2"][0]
		status := query["success"][0]
		taskComplete(fileName, ip1s, ip2s, status == "1") // TODO 保存本行的4个数据
		_, err := writer.Write([]byte("ok"))
		if err != nil {
			log.Println("Cannot return ok to client,", err)
			return
		}
	}).Methods("GET")
	rtr.HandleFunc("/new", func(writer http.ResponseWriter, request *http.Request) {
		clearFileInfos()
	})
	http.Handle("/", rtr)
	log.Printf("Listening at :%s...\n", PORT)
	_ = http.ListenAndServe(":"+PORT, nil)
}

const REGISTRY = "-1"         // 仓库的标识符
var lockForMutexes sync.Mutex // 修改fileInfos的锁

type FileInfo struct {
	listMutex   *sync.Mutex
	fetchMutex  *sync.Mutex
	appendMutex *sync.Mutex
	resources   *list.List
	request     chan bool
	response    chan bool
}

var fileInfos map[string]FileInfo

func init() {
	log.Println("init() was called")
	fileInfos = make(map[string]FileInfo)
}

func clearFileInfos() {
	for _, value := range fileInfos {
		close(value.request)
		close(value.response)
	}
	fileInfos = make(map[string]FileInfo)
}

// 为文件名和IP地址选择一个目标IP地址
func newRequest(fileName string, ip string) (url string) {
	log.Printf("find a new request %v, %v\n", fileName, ip)
	prepareForFileName(fileName)
	targetIP := fetchFromArray(fileName)
	log.Printf("ask %v to download from %v", ip, targetIP)
	return fmt.Sprintf("%s", targetIP)
}

func taskComplete(fileName string, ip1, ip2 string, success bool) {
	appendToArray(fileName, ip1, ip2, success)
}

// 提交某个IP的镜像列表
func submitImageInfo(ip string, data string) {
	log.Printf("%s is submitting %v...\n", ip, data)
	fileNames := strings.Split(data, ",")
	for _, fileName := range fileNames {
		prepareForFileName(fileName)
		fileInfos[fileName].resources.PushBack(ip)
	}
}

// 向资源列表添加两个IP
func appendToArray(fileName string, ip1, ip2 string, success bool) {
	// 1. 锁住本函数
	fileInfos[fileName].appendMutex.Lock()
	defer fileInfos[fileName].appendMutex.Unlock()
	// 2. 锁住队列 向队列添加数据
	fileInfos[fileName].listMutex.Lock()
	add1, add2 := isNotDuplicate(fileInfos[fileName].resources, ip1, ip2)
	if add1 {
		fileInfos[fileName].resources.PushBack(ip1)
	}
	if add2 && success {
		fileInfos[fileName].resources.PushBack(ip2)
	}
	// 3. 通知非空
	if add1 || (add2 && success) {
		select {
		case <-fileInfos[fileName].request:
			fileInfos[fileName].response <- true
		default:
		}
	}
	// 4. 释放锁
	fileInfos[fileName].listMutex.Unlock()
}

// 检查两个元素在list中是否重复 (调用的上下文)
func isNotDuplicate(l *list.List, ip1 string, ip2 string) (bool, bool) {
	var add1 = true
	var add2 = true
	for i := l.Front(); i != nil; i = i.Next() {
		if i.Value.(string) == ip1 {
			add1 = false
		}
		if i.Value.(string) == ip2 {
			add2 = false
		}
		if !add1 && !add2 {
			break
		}
	}
	return add1, add2
}

// 从文件名对应的资源列表中取出一个IP
func fetchFromArray(fileName string) (ip string) {
	// 1. 锁住本函数
	fileInfos[fileName].fetchMutex.Lock()
	defer fileInfos[fileName].fetchMutex.Unlock()
	// 2. 等待队列非空
	if fileInfos[fileName].resources.Len() == 0 {
		fileInfos[fileName].request <- true
		<-fileInfos[fileName].response
	}
	// 3. 锁住队列, 开始读取
	fileInfos[fileName].listMutex.Lock()
	defer fileInfos[fileName].listMutex.Unlock()
	defer fileInfos[fileName].resources.Remove(fileInfos[fileName].resources.Front())
	ip = fileInfos[fileName].resources.Front().Value.(string) // .(string) 类型断言
	return
}

// 如果文件的信息不存在于map中则进行创建. 否则什么都不做
func prepareForFileName(fileName string) {
	// 检测文件的信息是否已经创建 (如果没有, 则:)
	if _, ok := fileInfos[fileName]; !ok { // 判断元素是否在list中
		lockForMutexes.Lock()
		if _, ok := fileInfos[fileName]; !ok {
			fileInfos[fileName] = FileInfo{
				resources:   list.New(),
				listMutex:   &sync.Mutex{},
				fetchMutex:  &sync.Mutex{},
				appendMutex: &sync.Mutex{},
				request:     make(chan bool),
				response:    make(chan bool),
			}
			fileInfos[fileName].resources.PushBack(REGISTRY)
		}
		lockForMutexes.Unlock()
	}
}

func main() {
	httpServer()
}
