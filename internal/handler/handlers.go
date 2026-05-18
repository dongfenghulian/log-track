// Package handler hosts the built-in topic handlers.
//
// Each subpackage registers itself in router via init(). This top-level package
// is imported (blank) by cmd/gateway/main.go to trigger all registrations:
//
//	import _ "github.com/dongfenghulian/log-track/internal/handler"
package handler

import (
	_ "github.com/dongfenghulian/log-track/internal/handler/app_log"
	_ "github.com/dongfenghulian/log-track/internal/handler/event"
	_ "github.com/dongfenghulian/log-track/internal/handler/inbound_http"
	_ "github.com/dongfenghulian/log-track/internal/handler/outbound_http"
	_ "github.com/dongfenghulian/log-track/internal/handler/rpc"
)
