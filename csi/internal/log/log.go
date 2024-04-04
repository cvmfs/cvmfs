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

package log

import (
	"context"
	"fmt"

	"k8s.io/klog/v2"
)

const (
	// ctx.Context key for annotating log messages belonging to the same request.
	ReqIDContextKey = "Req-ID"

	// klog verbosity levels.
	LevelInfo  = 0
	LevelDebug = 4
	LevelTrace = 5
)

func tryPrependReqID(ctx context.Context, logMsg string) string {
	reqID := ctx.Value(ReqIDContextKey)
	if reqID != nil {
		return fmt.Sprintf("Req-ID: %v %s", reqID, logMsg)
	}

	return logMsg
}

func LevelEnabled(level int) bool {
	return klog.V(klog.Level(level)).Enabled()
}

func Infof(format string, args ...interface{}) {
	klog.V(LevelInfo).InfofDepth(1, format, args...)
}

func InfofWithContext(ctx context.Context, format string, args ...interface{}) {
	klog.V(LevelInfo).InfofDepth(1, tryPrependReqID(ctx, format), args...)
}

func InfofDepth(depth int, format string, args ...interface{}) {
	klog.V(LevelInfo).InfofDepth(depth, format, args...)
}

func InfofWithContextDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	klog.V(LevelInfo).InfofDepth(depth, tryPrependReqID(ctx, format), args...)
}

func Debugf(format string, args ...interface{}) {
	klog.V(LevelDebug).InfofDepth(1, format, args...)
}

func DebugfWithContext(ctx context.Context, format string, args ...interface{}) {
	if klog.V(LevelDebug).Enabled() {
		klog.V(LevelDebug).InfofDepth(1, tryPrependReqID(ctx, format), args...)
	}
}

func DebugfDepth(depth int, format string, args ...interface{}) {
	klog.V(LevelDebug).InfofDepth(depth, format, args...)
}

func DebugfWithContextDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	if klog.V(LevelDebug).Enabled() {
		klog.V(LevelDebug).InfofDepth(depth, tryPrependReqID(ctx, format), args...)
	}
}

func Tracef(format string, args ...interface{}) {
	klog.V(LevelTrace).InfofDepth(1, format, args...)
}

func TracefWithContext(ctx context.Context, format string, args ...interface{}) {
	if klog.V(LevelDebug).Enabled() {
		klog.V(LevelDebug).InfofDepth(1, tryPrependReqID(ctx, format), args...)
	}
}

func TracefDepth(depth int, format string, args ...interface{}) {
	klog.V(LevelTrace).InfofDepth(depth, format, args...)
}

func TracefWithContextDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	if klog.V(LevelDebug).Enabled() {
		klog.V(LevelDebug).InfofDepth(depth, tryPrependReqID(ctx, format), args...)
	}
}

func Warningf(format string, args ...interface{}) {
	klog.WarningfDepth(1, format, args...)
}

func WarningfWithContext(ctx context.Context, format string, args ...interface{}) {
	klog.WarningfDepth(1, tryPrependReqID(ctx, format), args...)
}

func WarningfDepth(depth int, format string, args ...interface{}) {
	klog.WarningfDepth(depth, format, args...)
}

func WarningfWithContextDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	klog.WarningfDepth(depth, tryPrependReqID(ctx, format), args...)
}

func Errorf(format string, args ...interface{}) {
	klog.ErrorfDepth(1, format, args...)
}

func ErrorfWithContext(ctx context.Context, format string, args ...interface{}) {
	klog.ErrorfDepth(1, tryPrependReqID(ctx, format), args...)
}

func ErrorfDepth(depth int, format string, args ...interface{}) {
	klog.ErrorfDepth(depth, format, args...)
}

func ErrorfWithContextDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	klog.ErrorfDepth(depth, tryPrependReqID(ctx, format), args...)
}

func Fatalf(format string, args ...interface{}) {
	klog.FatalfDepth(1, format, args...)
}

func FatalfWithContext(ctx context.Context, format string, args ...interface{}) {
	klog.FatalfDepth(1, tryPrependReqID(ctx, format), args...)
}

func FatalfDepth(depth int, format string, args ...interface{}) {
	klog.FatalfDepth(depth, format, args...)
}

func FatalfWithContextDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	klog.FatalfDepth(depth, tryPrependReqID(ctx, format), args...)
}
