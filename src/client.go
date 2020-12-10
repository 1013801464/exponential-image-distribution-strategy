package main

import (
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const HTTP_PORT string = "13001"
const TCP_PORT string = "13002"
const SERVER_SOCKET string = "10.10.108.60:13000"

// 仓库的IP定义为-1
const REGISTRY = "-1"

// 60仓库的blob URL, 第一个%s填镜像名, 第二个%s填blob名
const REGISTRY_BLOB_PATH_PREFIX = "http://10.10.108.60:5000/v2/%s/blobs/%s"
const LOCAL_BLOB_PATH_PREFIX = "/usr/local/nginx/docker_blobs"

// 4MB的文件缓冲区
const BUF_LEN = 4 * 1024 * 1024

var localIP string

func init() {
	log.Println("init() was called")
	localIP = getLocalIP()
	reportLocalBlobs()
}

// 汇报本地存在的blob
func reportLocalBlobs() {
	// 1. 读取本地blobs文件夹下的所有文件
	fileList := make([]string, 0, 10)
	reader, err := ioutil.ReadDir(LOCAL_BLOB_PATH_PREFIX)
	if err != nil {
		log.Println("Cannot read dir, ", err)
		return
	} else {
		for _, f := range reader {
			if !f.IsDir() {
				fileList = append(fileList, f.Name())
			}
		}
	}
	log.Printf("Reporting %d items...\n", len(fileList))
	// 2. 封装 post form 并发送
	values := url.Values{}
	values.Add("ip", fmt.Sprintf("%s", localIP))
	values.Add("data", strings.Join(fileList, ","))
	_, err = http.PostForm("http://"+SERVER_SOCKET+"/submit", values)
	if err != nil {
		log.Println("Post failed,", err)
		return
	}
}

func getLocalIP() (ip string) {
	conn, err := net.Dial("tcp", "10.10.108.99:3389")
	if err != nil {
		log.Println("无法连接", err)
		return
	}
	defer conn.Close()
	log.Println("local ip is", conn.LocalAddr().(*net.TCPAddr).IP.String())
	return conn.LocalAddr().(*net.TCPAddr).IP.String()
}

// 接收nginx发来的blob请求
func httpServer() {
	rtr := mux.NewRouter()
	rtr.HandleFunc("/v2/{image:[a-zA-Z0-9-]+}/blobs/{file:[:.a-zA-z0-9]+}", func(writer http.ResponseWriter, request *http.Request) {
		params := mux.Vars(request)
		fileName := params["file"]
		imageName := params["image"]
		log.Printf("You are requesting %v-%v\n", imageName, fileName)
		target := handle(imageName, fileName)
		var err error = nil
		if target == REGISTRY {
			err = pullFromRegistry(writer, fileName, imageName)
		} else if target != "" {
			err = pullFromClient(writer, fileName, target)
			if err != nil {
				log.Println("Fallback to registry...")
				err = pullFromRegistry(writer, fileName, imageName)
			}
		}
		// 如果汇报失败要不断重复报告
		var reportCount = 0
		for reportCompletion(fileName, localIP, target, err == nil) {
			time.Sleep(time.Second)
			reportCount++
			if reportCount > 10 {
				break
			}
		}
	}).Methods("GET")
	http.Handle("/", rtr)
	log.Println("HTTP Server is starting, listening :" + HTTP_PORT + "...")
	err := http.ListenAndServe(":"+HTTP_PORT, nil)
	if err != nil {
		log.Fatalf("Cannot start server, %v\n", err)
	}
}

// 接收其它客户端发来的blob下载请求
func tcpServer() {
	listener, err := net.Listen("tcp", ":"+TCP_PORT)
	if err != nil {
		log.Println("[C211] Cannot start tcp server, ", err)
		return
	}
	log.Println("[C210] TCP Server started, listening at :" + TCP_PORT + "...")
	for true {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("[C212] Accept failed, ", err)
			continue
		}
		log.Printf("[C213] Get a new connection from %v\n", conn.RemoteAddr())
		go func(conn net.Conn) {
			defer conn.Close()
			// 1. 接收文件名
			// 接收文件名本不需要这么大的缓冲区, 后面发文件要用
			buffer := [BUF_LEN]byte{}
			n, err := conn.Read(buffer[:])
			if err != nil {
				log.Println("[C214] Cannot read bytes from client, ", err)
				return
			}
			fileName := string(buffer[:n])
			// 2. 读取本地文件 (文件夹应该是存在的)
			fullLocalPath := fmt.Sprintf("%s%c%s", LOCAL_BLOB_PATH_PREFIX, os.PathSeparator, fileName)
			f, err := os.Open(fullLocalPath)
			log.Println("[C215]", "reading local file", fullLocalPath)
			if err != nil {
				log.Println("[C216] Open local file failed, ", err)
				return
			}
			defer f.Close()
			// 3. 一边读文件一边发送
			for true {
				nr, err1 := f.Read(buffer[:])
				_, err2 := conn.Write(buffer[:nr])
				if err1 != nil {
					if err1 == io.EOF {
						break
					} else {
						log.Println("[C217] Cannot read local file, ", err1)
						return
					}
				}
				if err2 != nil {
					log.Println("[C218] Cannot send to client, ", err2)
					return
				}
				// log.Printf("Send %v bytes\n", nw)
			}
		}(conn)
	}
}

// 处理文件下载请求 返回空串表示出错
func handle(image string, blob string) (ip string) {
	// 向 director 转发请求
	response, err := http.Get(fmt.Sprintf("http://"+SERVER_SOCKET+"/request/%v/%v?ip=%s", image, blob, localIP))
	if err != nil {
		log.Println("Cannot connect to server, ", err)
		return ""
	}
	log.Printf("Connected to %v\n", response.Request.URL)
	// 等待 director 返回结果
	bytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Println("Cannot read response.Body,", err)
		return ""
	}
	ip = string(bytes)
	log.Printf("Receive %s from director\n", ip)
	return
}

// 从仓库拉取 (http), 然后调用 `download()`
func pullFromRegistry(writer http.ResponseWriter, blob string, image string) error {
	fullURL := fmt.Sprintf(REGISTRY_BLOB_PATH_PREFIX, image, blob)
	log.Printf("Pulling from registry %s...\n", fullURL)
	response, err := http.Get(fullURL)
	if err != nil {
		log.Printf("Failed to connect to %s, %v\n", fullURL, err)
		return err
	}
	return download(writer, response.Body, blob)
}

// 从reader中读取数据并保存到本地文件, 同时向writer返回字节流
func download(writer http.ResponseWriter, reader io.ReadCloser, blob string) error {
	// 1. 创建保存blob的本地文件夹
	err := os.MkdirAll(LOCAL_BLOB_PATH_PREFIX, 0666)
	if err != nil {
		log.Printf("Cannot create dir %s, %v\n", LOCAL_BLOB_PATH_PREFIX, err)
		return err
	}
	// 2. 创建本地文件
	fullLocalPath := fmt.Sprintf("%s%c%s", LOCAL_BLOB_PATH_PREFIX, os.PathSeparator, blob)
	f, err := os.OpenFile(fullLocalPath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Cannot create file %s, %v\n", fullLocalPath, err)
		return err
	}
	defer f.Close()
	// 3. 开始从 reader 中读取流数据
	buffer := [BUF_LEN]byte{}
	for true {
		// 4.1 读取
		n, err1 := reader.Read(buffer[:])
		// 4.2 保存到文件
		// log.Printf("Writing %v bytes\n", n)
		_, err2 := f.Write(buffer[:n])
		// 4.3 发送给客户端
		_, err3 := writer.Write(buffer[:n])
		// 4.4 异常处理 (golang 要求后检查错误: https://pkg.go.dev/io#Reader)
		if err1 != nil {
			if err1 == io.EOF {
				log.Printf("[C201] Blob %s is downloaded successfully.\n", blob)
				break
			} else {
				log.Println("[C202] Cannot download file, ", err1)
				return err1
			}
		}
		if err2 != nil {
			log.Println("[C203] Cannot write to local file", err2)
			return err2
		}
		if err3 != nil {
			log.Printf("[C204] Cannot send to client, %v\n", err3)
			return err3
		}
	}
	return nil
}

// 从其它客户端拉取
func pullFromClient(writer http.ResponseWriter, blob string, ip string) error {
	if ip == localIP {
		loadLocalFile(writer, blob)
		return nil
	}
	// 1. 连接其它客户端
	remoteIP := fmt.Sprintf("%s:%s", ip, TCP_PORT)
	conn, err := net.Dial("tcp", remoteIP)
	if err != nil {
		log.Printf("Cannot download from %s, %v\n", remoteIP, err)
		return err
	}
	log.Printf("Connected to %s\n", remoteIP)
	defer conn.Close()
	// 2. 发送文件名
	_, err = conn.Write([]byte(blob))
	if err != nil {
		log.Println("Cannot send file name, ", err)
		return err
	}
	// 3. 下载 & 返回文件
	return download(writer, conn, blob)
}

// 从本地文件夹读取blob然后返回给writer
func loadLocalFile(writer http.ResponseWriter, blob string) {
	fullLocalPath := fmt.Sprintf("%s%c%s", LOCAL_BLOB_PATH_PREFIX, os.PathSeparator, blob)
	f, err := os.OpenFile(fullLocalPath, os.O_RDONLY, 0644)
	if err != nil {
		log.Println("[C321] Cannot read local file", fullLocalPath, err)
		return
	}
	defer f.Close()
	tmp := [BUF_LEN]byte{}
	sum_m := 0
	for {
		n, err1 := f.Read(tmp[:])
		m, err2 := writer.Write(tmp[:n])
		sum_m += m
		if err1 != nil {
			if err1 == io.EOF {
				log.Printf("Returned %d bytes totally.\n", sum_m)
				break
			} else {
				log.Println("读取文件失败", err1)
				return
			}
		}
		if err2 != nil {
			log.Println("Cannot send to client,", err2)
			return
		}
	}
}

// 向director汇报任务完成, 返回是否失败
func reportCompletion(blob string, ip1, ip2 string, success bool) (failed bool) {
	successStr := "0"
	if success {
		successStr = "1"
	}
	resp, err := http.Get(fmt.Sprintf("http://"+SERVER_SOCKET+"/complete/%s?ip1=%s&ip2=%s&success=%s", blob, ip1, ip2, successStr))
	if err != nil {
		log.Println("[C241] Cannot report completion to director, ", err)
		return true
	}
	log.Printf("[C240] Connected to %v\n", resp.Request.URL)
	buffer := [1024]byte{}
	n, err := resp.Body.Read(buffer[:])
	result := string(buffer[:n])
	if err != nil && err != io.EOF {
		log.Println("[C242] Cannot get message from director,", err)
		return true
	}
	log.Printf("[C243] Director returned %s\n", result)
	return result != "ok"
}

func main() {
	go httpServer()
	tcpServer()
}
