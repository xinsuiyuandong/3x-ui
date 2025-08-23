package sub

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"x-ui/config"
	"x-ui/logger"
	"x-ui/util/common"
	"x-ui/web/middleware"
	"x-ui/web/network"
	"x-ui/web/service"

	"github.com/gin-gonic/gin"
)

type Server struct {
	httpServer *http.Server
	listener   net.Listener

	sub            *SUBController
	settingService service.SettingService

	ctx    context.Context
	cancel context.CancelFunc
}

func NewServer() *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		ctx:    ctx,
		cancel: cancel,
	}
}

func (s *Server) initRouter() (*gin.Engine, error) {
	if config.IsDebug() {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.DefaultWriter = io.Discard
		gin.DefaultErrorWriter = io.Discard
		gin.SetMode(gin.ReleaseMode)
	}

	engine := gin.Default()

	subDomain, err := s.settingService.GetSubDomain()
	if err != nil {
		return nil, err
	}

	if subDomain != "" {
		engine.Use(middleware.DomainValidatorMiddleware(subDomain))
	}

	LinksPath, _ := s.settingService.GetSubPath()
	JsonPath, _ := s.settingService.GetSubJsonPath()
	Encrypt, _ := s.settingService.GetSubEncrypt()
	ShowInfo, _ := s.settingService.GetSubShowInfo()
	RemarkModel, _ := s.settingService.GetRemarkModel()
	SubUpdates, _ := s.settingService.GetSubUpdates()
	SubJsonFragment, _ := s.settingService.GetSubJsonFragment()
	SubJsonNoises, _ := s.settingService.GetSubJsonNoises()
	SubJsonMux, _ := s.settingService.GetSubJsonMux()
	SubJsonRules, _ := s.settingService.GetSubJsonRules()
	SubTitle, _ := s.settingService.GetSubTitle()

	g := engine.Group("/")

	s.sub = NewSUBController(
		g, LinksPath, JsonPath, Encrypt, ShowInfo, RemarkModel, SubUpdates,
		SubJsonFragment, SubJsonNoises, SubJsonMux, SubJsonRules, SubTitle)

	return engine, nil
}

func (s *Server) Start() (err error) {
	defer func() {
		if err != nil {
			s.Stop()
		}
	}()

	subEnable, err := s.settingService.GetSubEnable()
	if err != nil {
		return err
	}
	if !subEnable {
		return nil
	}

	engine, err := s.initRouter()
	if err != nil {
		return err
	}

	certFile, _ := s.settingService.GetSubCertFile()
	keyFile, _ := s.settingService.GetSubKeyFile()
	listen, _ := s.settingService.GetSubListen()
	port, _ := s.settingService.GetSubPort()

	listenAddr := net.JoinHostPort(listen, strconv.Itoa(port))
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}

	// --- 使用 AutoHttpsListener 处理设备限制 ---
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err == nil {
			listener = network.NewAutoHttpsListener(listener, func(conn net.Conn) error {
				clientIP := conn.RemoteAddr().String()
				if err := service.CheckDeviceLimit(clientIP); err != nil {
					conn.Close()
					return err
				}
				// 在连接关闭时自动释放设备
				go func() {
					time.Sleep(24 * time.Hour) // 或者根据实际逻辑释放
					service.ReleaseDevice(clientIP)
				}()
				return nil
			})
			listener = tls.NewListener(listener, &tls.Config{
				Certificates: []tls.Certificate{cert},
			})
			logger.Info("Sub server running HTTPS on", listener.Addr())
		} else {
			logger.Error("Error loading certificates:", err)
			logger.Info("Sub server running HTTP on", listener.Addr())
		}
	} else {
		logger.Info("Sub server running HTTP on", listener.Addr())
	}
	s.listener = listener

	s.httpServer = &http.Server{
		Handler: engine,
	}

	go func() {
		if err := s.httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			logger.Error("Sub server error:", err)
		}
	}()

	return nil
}

func (s *Server) Stop() error {
	s.cancel()

	var err1 error
	var err2 error
	if s.httpServer != nil {
		err1 = s.httpServer.Shutdown(s.ctx)
	}
	if s.listener != nil {
		err2 = s.listener.Close()
	}
	return common.Combine(err1, err2)
}

func (s *Server) GetCtx() context.Context {
	return s.ctx
}
