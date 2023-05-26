package tcp

import (
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/lrondanini/bit-box/bitbox/cluster/utils"
)

type TcpServer struct {
	ip              string
	port            string
	nodeCommChannel chan MessageFromCluster
	logger          *utils.Logger
	listener        net.Listener
	quit            chan bool
}

func InitServer(nodeIp string, nodePort string) *TcpServer {
	nodeAddress := nodeIp + ":" + nodePort

	// Listen for incoming connections.
	l, err := net.Listen("tcp", nodeAddress)
	if err != nil {
		fmt.Println("Error starting TCP server:", err)
		os.Exit(1)
	}

	fmt.Println("Listening from cluster on " + nodeAddress)

	tcpServer := TcpServer{
		ip:              nodeIp,
		port:            nodePort,
		logger:          utils.GetLogger(),
		nodeCommChannel: make(chan MessageFromCluster),
		listener:        l,
		quit:            make(chan bool),
	}

	return &tcpServer
}

func (s *TcpServer) Run() chan MessageFromCluster {

	// go func() {
	// 	fmt.Println("RUNNING")
	// 	var handlers sync.WaitGroup
	// 	for {
	// 		fmt.Print(".")
	// 		select {
	// 		case <-s.quit:
	// 			fmt.Print("1")
	// 			s.listener.Close()
	// 			fmt.Println("waiting")
	// 			handlers.Wait()
	// 			//close(s.nodeCommChannel)
	// 			return
	// 		default:
	// 			fmt.Print("2")
	// 			conn, err := s.listener.Accept()
	// 			if err != nil {
	// 				s.logger.Error("TCP Server error:", err.Error())
	// 				os.Exit(1)
	// 			}
	// 			fmt.Print("3")
	// 			handlers.Add(1)
	// 			go func() {
	// 				s.handleRequest(conn)
	// 				handlers.Done()
	// 			}()
	// 		}
	// 	}
	// }()

	go func() {
		var handlers sync.WaitGroup
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return
				default:
					s.logger.Error().Err(err).Msg("TCP Server error")
					os.Exit(1)
				}
			} else {
				handlers.Add(1)
				go func() {
					s.handleRequest(conn)
					handlers.Done()
				}()
			}
		}
	}()

	return s.nodeCommChannel
}

func (s *TcpServer) handleRequest(conn net.Conn) {
	dec := gob.NewDecoder(conn)
	p := &Frame{}
	err := dec.Decode(p)
	if err != nil {
		if err.Error() != "EOF" {
			fmt.Println("Could not handle request from " + conn.RemoteAddr().String() + ": " + err.Error())
		}
	} else {
		replyTo := make(chan Frame)
		msg := MessageFromCluster{
			Frame:          *p,
			ReplyToChannel: replyTo,
		}
		s.nodeCommChannel <- msg
		response := <-replyTo

		encoder := gob.NewEncoder(conn)
		encoder.Encode(response)
	}
}

func (s *TcpServer) Shutdown() {
	fmt.Print("Stopping TCP server.....")
	close(s.quit)
	s.listener.Close()

	fmt.Println("DONE")
}
