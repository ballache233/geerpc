package geerpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ballache233/geerpc/codec"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
)

type Server struct {
	serviceMap    sync.Map
	handleTimeout time.Duration
}

const MagicNumber = 0x3bef5c

type Option struct {
	MagicNumber    int
	CodecType      codec.Type
	ConnectTimeout time.Duration
	HandleTimeout  time.Duration
}

var DefaultOption = Option{
	MagicNumber:    MagicNumber,
	CodecType:      codec.GobType,
	ConnectTimeout: 10 * time.Second,
}

func NewServer() *Server {
	return &Server{}
}

func (server *Server) Accept(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("rpc server: accept tcp connection error", err)
			return
		}
		go server.ServeConn(conn)
	}
}

func (server *Server) ServeConn(conn io.ReadWriteCloser) {
	var opt Option
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: decode protocol error", err)
		return
	}
	if opt.MagicNumber != MagicNumber {
		log.Println("rpc server: magic number error")
		return
	}
	server.handleTimeout = opt.HandleTimeout
	f := codec.NewCodecFuncMap[opt.CodecType]
	server.ServeCodec(f(conn))
}

type request struct {
	header       *codec.Header
	argv, replyv reflect.Value
	svc          *service
	methodTyp    *methodType
}

var invalidRequest = struct{}{}

func (server *Server) ServeCodec(cc codec.Codec) {
	sending := new(sync.Mutex)
	wg := new(sync.WaitGroup)
	for {
		req, err := server.readRequest(cc)
		if err != nil {
			if req == nil {
				break //已经处理完成，退出
			}
			req.header.Error = err.Error()
			server.sendResponse(cc, req.header, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		go server.handleRequest(cc, req, sending, wg, server.handleTimeout)
	}
	wg.Wait()
	_ = cc.Close()
}

func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	h, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}

	req := new(request)
	req.header = h
	svc, methodTyp, err := server.findService(h.ServiceMethod)
	if err != nil {
		return req, err
	}
	req.methodTyp = methodTyp
	req.svc = svc
	req.argv = methodTyp.newArgv()
	req.replyv = methodTyp.newReplyv()
	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}
	if err := cc.ReadBody(argvi); err != nil {
		log.Println("rpc server: read body error", err)
		return req, err
	}
	return req, nil
}

func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	h := codec.Header{}
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read request header error", err)
		}
		return nil, err
	}
	return &h, nil
}

func (server *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: sending response error", err)
		return
	}
}

func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	called, sent := make(chan struct{}), make(chan struct{})
	go func() {
		err := req.svc.call(req.methodTyp, req.argv, req.replyv)
		called <- struct{}{}
		if err != nil {
			req.header.Error = err.Error()
			log.Println("rpc server: handle request error", err)
			server.sendResponse(cc, req.header, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		server.sendResponse(cc, req.header, req.replyv.Interface(), sending)
		sent <- struct{}{}
	}()
	select {
	case <-time.After(timeout):
		req.header.Error = fmt.Errorf("rpc server: handle request error").Error()
		server.sendResponse(cc, req.header, invalidRequest, sending)
		return
	case <-called:
		<-sent
	}
}

func (server *Server) Register(rcvr interface{}) error {
	s := newService(rcvr)
	if _, ok := server.serviceMap.LoadOrStore(s.name, s); ok {
		return errors.New("rpc server: service already defined")
	}
	return nil
}

func (server *Server) findService(serviceMethod string) (svc *service, methodTpy *methodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	svci, ok := server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: unregister service" + serviceMethod)
		return
	}
	svc = svci.(*service)
	methodTpy, ok = svc.method[methodName]
	if !ok {
		err = errors.New("rpc server: unregister method" + serviceMethod)
	}
	return
}

const (
	connected      = "200 connected to rpc server"
	defaultRPCPath = "/_geerpc_"
	debugPath      = "/debug/geerpc"
)

func (server *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodConnect {
		w.Header().Set("Content_Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}
	_, _ = io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	server.ServeConn(conn)
}

func (server *Server) HandleHTTP() {
	http.Handle(defaultRPCPath, server)
}
