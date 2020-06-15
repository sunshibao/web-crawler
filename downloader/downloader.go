package downloader

import (
	// "net"
	"net/http"
	// "time"
	base "web-crawler/base"
	"web-crawler/logging"
	mdw "web-crawler/middleware"
)

// 日志记录器。
var logger logging.Logger = base.NewLogger()

// ID生成器。
var downloaderIdGenerator mdw.IdGenerator = mdw.NewIdGenerator()

// 生成并返回ID。
func genDownloaderId() uint32 {
	return downloaderIdGenerator.GetUint32()
}

// 网页下载器的接口类型。
type PageDownloader interface {
	Id() uint32                                        // 获得ID。
	Download(req base.Request) (*base.Response, error) // 根据请求下载网页并返回响应。
}

// NewPageDownloader 创建网页下载器。
func NewPageDownloader(client *http.Client) PageDownloader {
	id := genDownloaderId()
	if client == nil {
		client = &http.Client{
		// Transport: &http.Transport{
		// 	Dial: func(netw, addr string) (net.Conn, error) {
		// 		c, err := net.DialTimeout(netw, addr, time.Second) //设置建立连接超时
		// 		if err != nil {
		// 			return nil, err
		// 		}
		// 		c.SetDeadline(time.Now().Add(time.Second * 50)) //设置发送接收数据超时
		// 		return c, nil
		// 	},
		// },
		}
	}
	return &myPageDownloader{
		id:         id,
		httpClient: *client,
	}
}

// 网页下载器的实现类型。
type myPageDownloader struct {
	id         uint32      // ID。
	httpClient http.Client // HTTP客户端。
}

func (dl *myPageDownloader) Id() uint32 {
	return dl.id
}

func (dl *myPageDownloader) Download(req base.Request) (*base.Response, error) {
	httpReq := req.HttpReq()
	logger.Infof("Do the request (url=%s)... \n", httpReq.URL)
	httpResp, err := dl.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	return base.NewResponse(httpResp, req.Depth()), nil
}
