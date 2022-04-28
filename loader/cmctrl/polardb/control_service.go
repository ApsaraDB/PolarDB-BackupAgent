/*
 * Copyright (c) 2018. Alibaba Cloud, All right reserved.
 * This software is the confidential and proprietary information of Alibaba Cloud ("Confidential Information").
 * You shall not disclose such Confidential Information and shall use it only in accordance with the terms of
 * the license agreement you entered into with Alibaba Cloud.
 */

package polardb

import (
	context "context"
	"log"
	"net"
	sync "sync"

	grpc "google.golang.org/grpc"
)

type server struct {
	UnimplementedControlServiceServer
	logger        *log.Logger
	responseCount int
	version       string
}

var once sync.Once

func GetCMCtrlServerInstance(port string, version string, logger *log.Logger) error {

	once.Do(func() {
		go initGrpcServer(port, version, logger)
	})

	return nil
}

const DefaultNetDev = "*"
const DefaultPort = "818"

func initGrpcServer(port string, version string, logger *log.Logger) error {

	logger.Printf("[INFO] init grpc server")

	hostIP, err := getIPByNetInterface(DefaultNetDev)
	if err != nil {
		logger.Printf("[INFO] get host ip fail %s", err.Error())
		return err
	}

	address := hostIP + ":" + port
	lis, err := net.Listen("tcp", address)
	if err != nil {
		logger.Printf("[INFO] listen fail %s", err.Error())
		return err
	}
	s := grpc.NewServer()

	logger.Printf("[INFO] start control service, address: %s", address)

	RegisterControlServiceServer(s, &server{logger: logger, responseCount: 0, version: version})
	if err := s.Serve(lis); err != nil {
		logger.Printf("[INFO] init server fail %s", err.Error())
		return err
	}

	return nil
}

func getIPByNetInterface(netInterName string) (string, error) {
	ipStr := "127.0.0.1"
	if netInterName == "0.0.0.0" || netInterName == "*" || netInterName == "" {
		return "0.0.0.0", nil
	}

	byName, err := net.InterfaceByName(netInterName)
	if err != nil {
		return ipStr, err
	}

	addresses, err := byName.Addrs()
	if err != nil {
		return ipStr, err
	}

	for _, addr := range addresses {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ipStr = ipnet.IP.String()
				break
			}
		}
	}

	return ipStr, nil
}

func (s *server) SyncInstance(ctx context.Context, in *SyncInstanceRequest) (*SyncInstanceResponse, error) {
	result := SyncInstanceResponse{}
	result.Code = 0
	result.Msg = "backup agent is running"
	result.Version = s.version

	if s.responseCount == 0 {
		s.logger.Printf("[INFO] SyncInstance request instype: %s, local: %s:%d, rw: %s:%d\n",
			in.Ins.Instype, in.Ins.Host, in.Ins.Port, in.Rw.Host, in.Rw.Port)

		s.logger.Printf("[INFO] SyncInstance Response, code: %d msg: %s version:%s", result.Code, result.Msg, result.Version)
	}

	s.responseCount = s.responseCount + 1
	if s.responseCount == 100 {
		s.responseCount = 0
	}

	return &result, nil
}
