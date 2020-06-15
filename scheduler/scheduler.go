package scheduler

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
	anlz "web-crawler/analyzer"
	base "web-crawler/base"
	dl "web-crawler/downloader"
	ipl "web-crawler/itempipeline"
	"web-crawler/logging"
	mdw "web-crawler/middleware"
)

// 组件的统一代号。
const (
	DOWNLOADER_CODE   = "downloader"
	ANALYZER_CODE     = "analyzer"
	ITEMPIPELINE_CODE = "item_pipeline"
	SCHEDULER_CODE    = "scheduler"
)

// 日志记录器。
var logger logging.Logger = base.NewLogger()

// GenHttpClient 被用来生成HTTP客户端的函数类型。
type GenHttpClient func() *http.Client

// Scheduler 调度器的接口类型。
type Scheduler interface {
	// 开启调度器。
	// 调用该方法会使调度器创建和初始化各个组件。在此之后，调度器会激活爬取流程的执行。
	// 参数channelArgs代表通道参数的容器。
	// 参数poolBaseArgs代表池基本参数的容器。
	// 参数crawlDepth代表了需要被爬取的网页的最大深度值。深度大于此值的网页会被忽略。
	// 参数httpClientGenerator代表的是被用来生成HTTP客户端的函数。
	// 参数respParsers的值应为分析器所需的被用来解析HTTP响应的函数的序列。
	// 参数itemProcessors的值应为需要被置入条目处理管道中的条目处理器的序列。
	// 参数firstHttpReq即代表首次请求。调度器会以此为起始点开始执行爬取流程。
	Start(channelArgs base.ChannelArgs,
		poolBaseArgs base.PoolBaseArgs,
		crawlDepth uint32,
		httpClientGenerator GenHttpClient,
		respParsers []anlz.ParseResponse,
		itemProcessors []ipl.ProcessItem,
		firstHttpReq *http.Request) (err error)
	// 调用该方法会停止调度器的运行。所有处理模块执行的流程都会被中止。
	Stop() bool
	// 判断调度器是否正在运行。
	Running() bool
	// 获得错误通道。调度器以及各个处理模块运行过程中出现的所有错误都会被发送到该通道。
	// 若该方法的结果值为nil，则说明错误通道不可用或调度器已被停止。
	ErrorChan() <-chan error
	// 判断所有处理模块是否都处于空闲状态。
	Idle() bool
	// 获取摘要信息。
	Summary(prefix string) SchedSummary
}

// NewScheduler 创建调度器。
func NewScheduler() Scheduler {
	return &myScheduler{}
}

// 调度器的实现类型。
// urlMap        map[string]bool
type myScheduler struct {
	channelArgs   base.ChannelArgs      // 通道参数的容器。
	poolBaseArgs  base.PoolBaseArgs     // 池基本参数的容器。
	crawlDepth    uint32                // 爬取的最大深度。首次请求的深度为0。
	primaryDomain string                // 主域名。
	chanman       mdw.ChannelManager    // 通道管理器。
	stopSign      mdw.StopSign          // 停止信号。
	dlpool        dl.PageDownloaderPool // 网页下载器池。
	analyzerPool  anlz.AnalyzerPool     // 分析器池。
	itemPipeline  ipl.ItemPipeline      // 条目处理管道。
	reqCache      requestCache          // 请求缓存。
	urlMap        *concurrentMap        // 已请求的URL的字典。
	running       uint32                // 运行标记。0表示未运行，1表示已运行，2表示已停止。
}

func (sched *myScheduler) Start(
	channelArgs base.ChannelArgs,
	poolBaseArgs base.PoolBaseArgs,
	crawlDepth uint32,
	httpClientGenerator GenHttpClient,
	respParsers []anlz.ParseResponse,
	itemProcessors []ipl.ProcessItem,
	firstHttpReq *http.Request) (err error) {

	defer func() {
		if p := recover(); p != nil {
			errMsg := fmt.Sprintf("Fatal Scheduler Error: %s\n", p)
			logger.Error(errMsg)
			err = errors.New(errMsg)
		}
	}()

	if atomic.LoadUint32(&sched.running) == 1 {
		return errors.New("The scheduler has been started!\n")
	}

	atomic.StoreUint32(&sched.running, 1)

	// chanal args
	if err := channelArgs.Check(); err != nil {
		return err
	}
	sched.channelArgs = channelArgs

	// pool args
	if err := poolBaseArgs.Check(); err != nil {
		return err
	}
	sched.poolBaseArgs = poolBaseArgs

	// 爬取深度
	sched.crawlDepth = crawlDepth

	// 生成通道管理器
	sched.chanman = generateChannelManager(sched.channelArgs)
	if httpClientGenerator == nil {
		return errors.New("The HTTP client generator list is invalid!")
	}
	// 生成下载器池
	dlpool, err := generatePageDownloaderPool(sched.poolBaseArgs.PageDownloaderPoolSize(), httpClientGenerator)
	if err != nil {
		errMsg := fmt.Sprintf("Occur error when get page downloader pool: %s\n", err)
		return errors.New(errMsg)
	}
	sched.dlpool = dlpool

	// 生成分析器池
	analyzerPool, err := generateAnalyzerPool(sched.poolBaseArgs.AnalyzerPoolSize())
	if err != nil {
		errMsg := fmt.Sprintf("Occur error when get analyzer pool: %s\n", err)
		return errors.New(errMsg)
	}
	sched.analyzerPool = analyzerPool

	if itemProcessors == nil {
		return errors.New("The item processor list is invalid!")
	}
	for i, ip := range itemProcessors {
		if ip == nil {
			errMsg := fmt.Sprintf("The %dth item processor is invalid!", i)
			return errors.New(errMsg)
		}
	}
	sched.itemPipeline = generateItemPipeline(itemProcessors)

	if sched.stopSign == nil {
		sched.stopSign = mdw.NewStopSign()
	} else {
		sched.stopSign.Reset()
	}

	sched.reqCache = newRequestCache()
	sched.urlMap = newConcurrentMap()

	sched.run(respParsers)
	// sched.startDownloading()
	// sched.activateAnalyzers(respParsers)
	// sched.openItemPipeline()
	sched.schedule(time.Second)

	if firstHttpReq == nil {
		return errors.New("The first HTTP request is invalid!")
	}
	pd, err := getPrimaryDomain(firstHttpReq.Host)
	if err != nil {
		return err
	}
	sched.primaryDomain = pd

	firstReq := base.NewRequest(firstHttpReq, 0)

	sched.reqCache.put(firstReq)
	// sched.getReqChan() <- *firstReq

	return nil
}

func (sched *myScheduler) Stop() bool {
	if atomic.LoadUint32(&sched.running) != 1 {
		return false
	}
	sched.stopSign.Sign()
	sched.chanman.Close()
	sched.reqCache.close()
	atomic.StoreUint32(&sched.running, 2)
	return true
}

func (sched *myScheduler) Running() bool {
	return atomic.LoadUint32(&sched.running) == 1
}

func (sched *myScheduler) ErrorChan() <-chan error {
	if sched.chanman.Status() != mdw.CHANNEL_MANAGER_STATUS_INITIALIZED {
		return nil
	}
	return sched.getErrorChan()
}

func (sched *myScheduler) Idle() bool {
	idleDlPool := sched.dlpool.Used() == 0
	idleAnalyzerPool := sched.analyzerPool.Used() == 0
	idleItemPipeline := sched.itemPipeline.ProcessingNumber() == 0
	if idleDlPool && idleAnalyzerPool && idleItemPipeline {
		return true
	}
	return false
}

func (sched *myScheduler) Summary(prefix string) SchedSummary {
	return NewSchedSummary(sched, prefix)
}

func (sched *myScheduler) run(respParsers []anlz.ParseResponse) {
	go func() {

		sched.itemPipeline.SetFailFast(true)
		code := ITEMPIPELINE_CODE

		for {

			select {
			case req, ok := <-sched.getReqChan():
				{
					if !ok {
						logger.Error("<======Get (req) chan error")
						continue
					}
					go sched.download(req)
				}
			case resp, ok := <-sched.getRespChan():
				{
					if !ok {
						logger.Error("<======Get (resp) chan error")
						continue
					}
					go sched.analyze(respParsers, resp)
				}
			case item, ok := <-sched.getItemChan():
				{
					if !ok {
						logger.Error("<======Get (item) chan error")
						continue
					}
					go func(item base.Item) {
						defer func() {
							if p := recover(); p != nil {
								errMsg := fmt.Sprintf("致命的Item处理错误: %s\n", p)
								logger.Error(errMsg)
							}
						}()
						errs := sched.itemPipeline.Send(item)
						if errs != nil {
							for _, err := range errs {
								sched.sendError(err, code)
							}
						}
					}(item)
					// time.Sleep(time.Microsecond)
				}
			}
		}
	}()
}

// 开始下载。
// func (sched *myScheduler) startDownloading() {
// 	go func() {
// 		for {
// 			req, ok := <-sched.getReqChan()
// 			if !ok {
// 				break
// 			}
// 			sched.download(req)
// 		}
// 	}()
// }

// 下载。
// 1:获取downloader
// 2:下载
// 3:把响应放到响应队列中，出错，则把出错信息放入出错队列
// 4:返回downloader到池子
func (sched *myScheduler) download(req base.Request) {
	defer func() {
		if p := recover(); p != nil {
			errMsg := fmt.Sprintf("Fatal Download Error: %s\n", p)
			logger.Error(errMsg)
		}
	}()
	downloader, err := sched.dlpool.Take()
	if err != nil {
		errMsg := fmt.Sprintf("Downloader pool error: %s", err)
		sched.sendError(errors.New(errMsg), SCHEDULER_CODE)
		return
	}
	defer func() {
		err := sched.dlpool.Return(downloader)
		if err != nil {
			errMsg := fmt.Sprintf("Downloader pool error: %s", err)
			sched.sendError(errors.New(errMsg), SCHEDULER_CODE)
		}
	}()
	code := generateCode(DOWNLOADER_CODE, downloader.Id())
	respp, err := downloader.Download(req)
	if respp != nil {
		sched.sendResp(*respp, code)
	}
	if err != nil {
		sched.sendError(err, code)
	}
}

// 激活分析器。
// func (sched *myScheduler) activateAnalyzers(respParsers []anlz.ParseResponse) {
// 	go func() {
// 		for {
// 			resp, ok := <-sched.getRespChan()
// 			if !ok {
// 				logger.Error("<======Get resp chan error")
// 				break
// 			}
// 			go sched.analyze(respParsers, resp)
// 		}
// 	}()
// }

// 分析。
func (sched *myScheduler) analyze(respParsers []anlz.ParseResponse, resp base.Response) {
	defer func() {
		if p := recover(); p != nil {
			errMsg := fmt.Sprintf("Fatal Analysis Error: %s\n", p)
			logger.Error(errMsg)
		}
	}()
	analyzer, err := sched.analyzerPool.Take()
	if err != nil {
		errMsg := fmt.Sprintf("Analyzer pool error: %s", err)
		sched.sendError(errors.New(errMsg), SCHEDULER_CODE)
		return
	}
	defer func() {
		err := sched.analyzerPool.Return(analyzer)
		if err != nil {
			errMsg := fmt.Sprintf("Analyzer pool error: %s", err)
			sched.sendError(errors.New(errMsg), SCHEDULER_CODE)
		}
	}()
	code := generateCode(ANALYZER_CODE, analyzer.Id())
	dataList, errs := analyzer.Analyze(respParsers, resp)
	if dataList != nil {
		for _, data := range dataList {
			if data == nil {
				continue
			}
			// 根据自定义的解析函数，得到页面结果dataList
			switch d := data.(type) {
			case *base.Request:
				// 放入到请求缓存中
				// sched.getReqChan() <- *d
				sched.saveReqToCache(*d, code)
			case *base.Item:
				// 放入到条目通道中
				sched.sendItem(*d, code)
			default:
				errMsg := fmt.Sprintf("Unsupported data type '%T'! (value=%v)\n", d, d)
				sched.sendError(errors.New(errMsg), code)
			}
		}
	}
	if errs != nil {
		for _, err := range errs {
			sched.sendError(err, code)
		}
	}
}

// 打开条目处理管道。
// func (sched *myScheduler) openItemPipeline() {
// 	go func() {
// 		sched.itemPipeline.SetFailFast(true)
// 		code := ITEMPIPELINE_CODE
// 		// 等待条目管道条目
// 		for {
// 			select {
// 			case item := <-sched.getItemChan():
// 				{
// 					go func(item base.Item) {
// 						defer func() {
// 							if p := recover(); p != nil {
// 								errMsg := fmt.Sprintf("Fatal Item Processing Error: %s\n", p)
// 								logger.Error(errMsg)
// 							}
// 						}()
// 						errs := sched.itemPipeline.Send(item)
// 						if errs != nil {
// 							for _, err := range errs {
// 								sched.sendError(err, code)
// 							}
// 						}
// 					}(item)
// 					time.Sleep(time.Microsecond)
// 				}
// 			}
// 		}
// 	}()
// }

// 把请求存放到请求缓存。同时过滤不符合要求的请求,接收http或https的请求
func (sched *myScheduler) saveReqToCache(req base.Request, code string) bool {

	httpReq := req.HttpReq()
	if httpReq == nil {
		logger.Warnln("Ignore the request! It's HTTP request is invalid!")
		return false
	}

	reqURL := httpReq.URL
	if reqURL == nil {
		logger.Warnln("Ignore the request! It's url is is invalid!")
		return false
	}
	// fmt.Println("req=============>>", reqURL.String())
	// 忽略不是http or https的请求
	scheme := strings.ToLower(reqURL.Scheme)
	if scheme != "http" && scheme != "https" {
		logger.Warnf("Ignore the request! It's url scheme '%s', but should be 'http'!\n", reqURL.Scheme)
		return false
	}

	// if _, ok := sched.urlMap[reqURL.String()]; ok {
	// 	logger.Warnf("Ignore the request! It's url is repeated. (requestUrl=%s)\n", reqURL)
	// 	return false
	// }

	// 只抓取跟主域名相关的网页信息
	if pd, _ := getPrimaryDomain(httpReq.Host); pd != sched.primaryDomain {
		logger.Warnf("Ignore the request! It's host '%s' not in primary domain '%s'. (requestUrl=%s)\n",
			httpReq.Host, sched.primaryDomain, reqURL)
		return false
	}

	// 忽略深度大于爬取深度的请求
	if req.Depth() > sched.crawlDepth {
		logger.Warnf("Ignore the request! It's depth %d greater than %d. (requestUrl=%s)\n",
			req.Depth(), sched.crawlDepth, reqURL)
		return false
	}

	if sched.stopSign.Signed() {
		sched.stopSign.Deal(code)
		return false
	}
	// 忽略已存在的URL
	if err := sched.urlMap.put(reqURL.String()); err != nil {
		logger.Warnf("Ignore the request! It's url is repeated. (requestUrl=%s)\n", reqURL)
		return false
	}

	sched.reqCache.put(&req)
	// sched.urlMap[reqURL.String()] = true
	return true
}

// 发送响应。
func (sched *myScheduler) sendResp(resp base.Response, code string) bool {
	if sched.stopSign.Signed() {
		sched.stopSign.Deal(code)
		return false
	}
	sched.getRespChan() <- resp
	return true
}

// 发送条目。
func (sched *myScheduler) sendItem(item base.Item, code string) bool {
	if sched.stopSign.Signed() {
		sched.stopSign.Deal(code)
		return false
	}
	// fmt.Println("Item==========>>", item)
	sched.getItemChan() <- item
	return true
}

// 发送错误。
func (sched *myScheduler) sendError(err error, code string) bool {
	if err == nil {
		return false
	}
	codePrefix := parseCode(code)[0]
	var errorType base.ErrorType
	switch codePrefix {
	case DOWNLOADER_CODE:
		errorType = base.DOWNLOADER_ERROR
	case ANALYZER_CODE:
		errorType = base.ANALYZER_ERROR
	case ITEMPIPELINE_CODE:
		errorType = base.ITEM_PROCESSOR_ERROR
	}
	cError := base.NewCrawlerError(errorType, err.Error())
	if sched.stopSign.Signed() {
		sched.stopSign.Deal(code)
		return false
	}
	go func() {
		sched.getErrorChan() <- cError
	}()
	return true
}

// 调度。适当的搬运请求缓存中的请求到请求通道。
func (sched *myScheduler) schedule(interval time.Duration) {

	go func() {
		for {

			if sched.stopSign.Signed() {
				sched.stopSign.Deal(SCHEDULER_CODE)
				return
			}

			for _, req := range sched.reqCache.getAll() {

				go func(req *base.Request) {
					if sched.stopSign.Signed() {
						sched.stopSign.Deal(SCHEDULER_CODE)
						return
					}
					sched.getReqChan() <- *req
				}(req)
			}

			sched.reqCache.reset()
			// 当前请求通道空闲的位置，放入请求
			// remainder := cap(sched.getReqChan()) - len(sched.getReqChan())
			// var temp *base.Request
			// for remainder > 0 {
			// 	temp = sched.reqCache.get()
			// 	if temp == nil {
			// 		break
			// 	}
			// 	if sched.stopSign.Signed() {
			// 		sched.stopSign.Deal(SCHEDULER_CODE)
			// 		return
			// 	}
			// 	sched.getReqChan() <- *temp
			// 	remainder--
			// }
			time.Sleep(interval)
		}
	}()
}

// 获取通道管理器持有的请求通道。
func (sched *myScheduler) getReqChan() chan base.Request {
	reqChan, err := sched.chanman.ReqChan()
	if err != nil {
		panic(err)
	}
	return reqChan
}

// 获取通道管理器持有的响应通道。
func (sched *myScheduler) getRespChan() chan base.Response {
	respChan, err := sched.chanman.RespChan()
	if err != nil {
		panic(err)
	}
	return respChan
}

// 获取通道管理器持有的条目通道。
func (sched *myScheduler) getItemChan() chan base.Item {
	itemChan, err := sched.chanman.ItemChan()
	if err != nil {
		panic(err)
	}
	return itemChan
}

// 获取通道管理器持有的错误通道。
func (sched *myScheduler) getErrorChan() chan error {
	errorChan, err := sched.chanman.ErrorChan()
	if err != nil {
		panic(err)
	}
	return errorChan
}
