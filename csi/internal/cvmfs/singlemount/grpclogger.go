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
	"fmt"
	"sync/atomic"

	"github.com/cvmfs/csi/internal/log"

	"google.golang.org/grpc"
)

var (
	// Counter value used for pairing up GRPC call and response log messages.
	grpcCallCounter uint64
)

func fmtGRPCLogMsg(grpcCallID uint64, msg string) string {
	return fmt.Sprintf("Call-ID %d: %s", grpcCallID, msg)
}

func grpcLogger(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	grpcCallID := atomic.AddUint64(&grpcCallCounter, 1)

	log.DebugfWithContext(ctx, fmtGRPCLogMsg(grpcCallID, fmt.Sprintf("Call: %s", info.FullMethod)))
	log.DebugfWithContext(ctx, fmtGRPCLogMsg(grpcCallID, fmt.Sprintf("Request: %s", req)))

	resp, err := handler(ctx, req)
	if err != nil {
		log.ErrorfWithContext(ctx, fmtGRPCLogMsg(grpcCallID, fmt.Sprintf("Error: %v", err)))
	} else {
		log.DebugfWithContext(ctx, fmtGRPCLogMsg(grpcCallID, fmt.Sprintf("Response: %s", resp)))
	}

	return resp, err
}
