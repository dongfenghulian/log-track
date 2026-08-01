# Makefile for log-track
#
# Usage:
#   make build          # build ./bin/<project> and inject git/build metadata
#   make build VERSION=v1.2.0
#   make run            # go run with the same injected metadata
#   make install        # install existing ./bin/<project>: backup, stop, copy, start
#   make backoff        # restore backup: default <app>.last, or COMMIT=<commit>
#   make clean
#   make version        # print metadata that will be injected
#
# Build metadata is injected through -ldflags -X into internal/version:
#   ProjectName / Version / GitCommit / GitMsg / BuildDate

PKG          := github.com/dongfenghulian/log-track
PROJECT_NAME ?= log-track
APP_NAME     ?= $(PROJECT_NAME)
CMD          := ./cmd/server
HTTP_CMD     := ./cmd/http-api
BINARY       := bin/$(PROJECT_NAME)
HTTP_BINARY  := bin/$(PROJECT_NAME)-http-api
VERSION     ?= dev
INSTALL_DIR  ?= /usr/local/go-server/$(APP_NAME)
INSTALL_PATH ?= $(INSTALL_DIR)/$(APP_NAME)
BACKUP_PATH  ?= $(INSTALL_DIR)/$(APP_NAME).last
COMMIT_BACKUP_PATH ?= $(INSTALL_DIR)/$(APP_NAME).$(GIT_COMMIT)
BACKOFF_PATH = $(if $(filter command line,$(origin COMMIT)),$(INSTALL_DIR)/$(APP_NAME).$(COMMIT),$(BACKUP_PATH))
SUPERVISOR_PROGRAM ?= $(APP_NAME)

# Git data falls back to unknown so builds also work from source archives without .git.
GIT_COMMIT  := $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
GIT_MSG_RAW := $(shell git log -1 --pretty=%s 2>/dev/null || echo unknown)
GIT_MSG     := $(shell echo "$(GIT_MSG_RAW)" | tr -d '"\\\`\r\n' | cut -c1-80)
BUILD_DATE  := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

VERSION_PKG := $(PKG)/internal/version
LDFLAGS := \
	-X '$(VERSION_PKG).ProjectName=$(PROJECT_NAME)' \
	-X '$(VERSION_PKG).Version=$(VERSION)' \
	-X '$(VERSION_PKG).GitCommit=$(GIT_COMMIT)' \
	-X '$(VERSION_PKG).GitMsg=$(GIT_MSG)' \
	-X '$(VERSION_PKG).BuildDate=$(BUILD_DATE)'

.PHONY: build run build-http run-http install backoff clean version

build:
	@echo "==> building $(BINARY)  project=$(PROJECT_NAME) version=$(VERSION) commit=$(GIT_COMMIT) date=$(BUILD_DATE)"
	@mkdir -p $(dir $(BINARY))
	go build -trimpath -ldflags "$(LDFLAGS)" -o $(BINARY) $(CMD)

run:
	@echo "==> running $(CMD)  project=$(PROJECT_NAME) version=$(VERSION) commit=$(GIT_COMMIT) date=$(BUILD_DATE)"
	go run -ldflags "$(LDFLAGS)" $(CMD) $(ARGS)

build-http:
	@echo "==> building $(HTTP_BINARY)  project=$(PROJECT_NAME) version=$(VERSION) commit=$(GIT_COMMIT) date=$(BUILD_DATE)"
	@mkdir -p $(dir $(HTTP_BINARY))
	go build -trimpath -ldflags "$(LDFLAGS)" -o $(HTTP_BINARY) $(HTTP_CMD)

run-http:
	@echo "==> running $(HTTP_CMD)  project=$(PROJECT_NAME) version=$(VERSION) commit=$(GIT_COMMIT) date=$(BUILD_DATE)"
	go run -ldflags "$(LDFLAGS)" $(HTTP_CMD) $(ARGS)

install:
	@printf "Install to $(INSTALL_PATH) and restart supervisor program $(SUPERVISOR_PROGRAM)? [y/N] "; \
	read ans; \
	case "$$ans" in y|Y|yes|YES) ;; *) echo "aborted"; exit 1;; esac
	@test -x $(BINARY) || (echo "missing executable $(BINARY), run 'make build' first"; exit 1)
	@echo "==> backing up $(INSTALL_PATH) -> $(COMMIT_BACKUP_PATH), $(BACKUP_PATH)"
	@if sudo test -f $(INSTALL_PATH); then \
		sudo cp -p $(INSTALL_PATH) $(COMMIT_BACKUP_PATH); \
		sudo cp -p $(INSTALL_PATH) $(BACKUP_PATH); \
		echo "backup: $(COMMIT_BACKUP_PATH)"; \
		echo "backup: $(BACKUP_PATH)"; \
	else \
		echo "skip backup: $(INSTALL_PATH) not found"; \
	fi
	@echo "==> stopping supervisor program $(SUPERVISOR_PROGRAM)"
	sudo supervisorctl stop $(SUPERVISOR_PROGRAM)
	@echo "==> installing $(BINARY) -> $(INSTALL_PATH)"
	sudo mkdir -p $(dir $(INSTALL_PATH))
	sudo cp $(BINARY) $(INSTALL_PATH)
	sudo chmod +x $(INSTALL_PATH)
	@echo "==> starting supervisor program $(SUPERVISOR_PROGRAM)"
	sudo supervisorctl start $(SUPERVISOR_PROGRAM)

backoff:
	@printf "Restore $(BACKOFF_PATH) to $(INSTALL_PATH) and restart supervisor program $(SUPERVISOR_PROGRAM)? [y/N] "; \
	read ans; \
	case "$$ans" in y|Y|yes|YES) ;; *) echo "aborted"; exit 1;; esac
	@sudo test -f $(BACKOFF_PATH) || (echo "missing backup $(BACKOFF_PATH)"; exit 1)
	@echo "==> stopping supervisor program $(SUPERVISOR_PROGRAM)"
	sudo supervisorctl stop $(SUPERVISOR_PROGRAM)
	@echo "==> restoring $(BACKOFF_PATH) -> $(INSTALL_PATH)"
	sudo cp $(BACKOFF_PATH) $(INSTALL_PATH)
	sudo chmod +x $(INSTALL_PATH)
	@echo "==> starting supervisor program $(SUPERVISOR_PROGRAM)"
	sudo supervisorctl start $(SUPERVISOR_PROGRAM)

clean:
	rm -rf bin

version:
	@echo "project_name = $(PROJECT_NAME)"
	@echo "version      = $(VERSION)"
	@echo "git_commit   = $(GIT_COMMIT)"
	@echo "git_msg      = $(GIT_MSG)"
	@echo "build_date   = $(BUILD_DATE)"
