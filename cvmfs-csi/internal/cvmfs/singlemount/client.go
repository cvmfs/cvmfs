// Copyright CERN.
//
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package singlemount

import (
	"context"
	"time"

	pb "github.com/cvmfs/cvmfs-csi/internal/cvmfs/singlemount/pb/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn *grpc.ClientConn
	cl   pb.SingleClient
}

var _ pb.SingleClient = (*Client)(nil)

func NewClient(ctx context.Context, endpoint string) (*Client, error) {
	conn, err := grpc.DialContext(
		ctx,
		endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  1.0 * time.Second,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   time.Second,
			},
		}),
		grpc.WithBlock(),
	)

	if err != nil {
		return nil, err
	}

	return &Client{
		conn: conn,
		cl:   pb.NewSingleClient(conn),
	}, nil
}

// Mounts a single CVMFS repository.
func (c *Client) Mount(ctx context.Context, in *pb.MountSingleRequest, opts ...grpc.CallOption) (*pb.MountSingleResponse, error) {
	return c.cl.Mount(ctx, in, opts...)
}

// Unmount a single CVMFS repository.
func (c *Client) Unmount(ctx context.Context, in *pb.UnmountSingleRequest, opts ...grpc.CallOption) (*pb.UnmountSingleResponse, error) {
	return c.cl.Unmount(ctx, in, opts...)
}

func (c *Client) Close() error {
	err := c.conn.Close()
	c.conn = nil
	c.cl = nil

	return err
}
