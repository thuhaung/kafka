package network

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
)

type Server struct {
	listener net.Listener
	wg       sync.WaitGroup
	sem      chan struct{}

	transport Transport
	handler Handler
}

var (
	ErrCannotListenOnPort = "Cannot listen on port"
	ErrMaxConnections = "Maximum connections reached"
)

func NewServer(maxConnections int, transport Transport, handler Handler) *Server {
	return &Server{
		sem:       make(chan struct{}, maxConnections),
		transport: transport,
		handler:   handler,
	}
}

func (s *Server) Start(ctx context.Context, addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("%s %s: %w", ErrCannotListenOnPort, addr, err)
	}

	s.listener = listener
	log.Println("Server listening on address:", addr)

	go s.stop(ctx)

	s.wg.Add(1)
	go s.acceptConnections(ctx)

	return nil
}

func (s *Server) stop(ctx context.Context) {
	<-ctx.Done()
	log.Println("Server shutting down...")
	s.listener.Close()
}

func (s *Server) Wait() {
	s.wg.Wait()
}

func (s *Server) acceptConnections(ctx context.Context) {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				log.Println("Error accepting connection:", err)
				continue
			}
		}

		select {
		case s.sem <- struct{}{}:
			s.wg.Add(1)
			go s.handleConnection(ctx, conn)
		default:
			log.Println("Max connections reached, rejecting new connection")
			conn.Close()
		}
	}
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()
	defer func() { <-s.sem }()

	log.Println("Handling new connection from:", conn.RemoteAddr())

	for {
		request, err := s.transport.ReadRequest(conn)
		if err != nil {
			log.Println("Error reading request:", err)
			return
		}

		response, err := s.handler.HandleRequest(ctx, request)
		if err != nil {
			log.Println("Error handling request:", err)
			return
		}

		err = s.transport.WriteResponse(conn, response)
		if err != nil {
			log.Println("Error writing response:", err)
			return

		}
	}
}
