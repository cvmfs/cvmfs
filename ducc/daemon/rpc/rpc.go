package rpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	netRpc "net/rpc"
	"os"
	"path/filepath"

	commands "github.com/cvmfs/ducc/daemon/commands"
)

const RpcAddress string = "/tmp/cvmfs/ducc/rpc.sock"

func RunRpcServer(ctx context.Context, done chan<- any) {
	if err := os.MkdirAll(filepath.Dir(RpcAddress), 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating directory for database: %s\n", err)
		os.Exit(1)
	}

	if err := os.Remove(RpcAddress); err != nil && !errors.Is(err, os.ErrNotExist) {
		fmt.Fprintf(os.Stderr, "Error removing old socket: %s\n", err)
		os.Exit(1)
	}

	cmd := new(commands.CommandService)
	netRpc.Register(cmd)

	listener, err := net.Listen("unix", RpcAddress)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating socket: %s\n", err)
		os.Exit(1)
	}
	defer os.Remove(RpcAddress)

	go func() {
		for {
			conn, err := listener.Accept()
			if errors.Is(err, net.ErrClosed) {
				close(done)
				return
			} else if err != nil {
				fmt.Fprintf(os.Stderr, "Error accepting connection: %s\n", err)
				continue
			}
			netRpc.ServeConn(conn)
		}
	}()

	<-ctx.Done()
	listener.Close()
}
