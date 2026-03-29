.PHONY: test vet proto test-e2e test-e2e-external test-fuzz-cluster cluster-start cluster-stop cluster-verify pd-cluster-start pd-cluster-stop pd-cluster-verify txn-demo-start txn-demo-stop txn-demo-verify scale-demo-start scale-demo-stop scale-demo-verify pd-failover-demo-start pd-failover-demo-stop pd-failover-demo-verify txn-integrity-demo-start txn-integrity-demo-stop txn-integrity-demo-verify gookv-cli

GO_SRC := $(shell find cmd/ internal/ pkg/ -name '*.go')

CLUSTER_DIR = /tmp/gookv-cluster
CLUSTER_NODES = 5
CLUSTER_TOPOLOGY = 1=127.0.0.1:20160,2=127.0.0.1:20161,3=127.0.0.1:20162,4=127.0.0.1:20163,5=127.0.0.1:20164

PD_CLUSTER_DIR = /tmp/gookv-pd-cluster
PD_ADDR = 127.0.0.1:2379

test:
	go test ./pkg/... ./internal/... -v -count=1

test-e2e:
	go test ./e2e/... -v -count=1 -timeout 120s

test-e2e-external: build
	go test ./e2e_external/... -v -count=1 -timeout 2400s

test-fuzz-cluster: build
	go test ./e2e_external/... -run TestFuzzCluster -v -count=1 -timeout 2400s

build: gookv-server gookv-ctl gookv-pd gookv-cli

gookv-server: $(GO_SRC) go.mod go.sum
	go build -o gookv-server ./cmd/gookv-server

gookv-ctl: $(GO_SRC) go.mod go.sum
	go build -o gookv-ctl ./cmd/gookv-ctl

gookv-pd: $(GO_SRC) go.mod go.sum
	go build -o gookv-pd ./cmd/gookv-pd

gookv-cli: $(GO_SRC) go.mod go.sum
	go build -o gookv-cli ./cmd/gookv-cli

vet:
	go vet ./...

proto:
	@echo "Proto generation is not needed: gookv uses pre-generated Go code from github.com/pingcap/kvproto"

pd-cluster-start: build
	@echo "Starting PD + $(CLUSTER_NODES)-node gookv cluster..."
	@mkdir -p $(PD_CLUSTER_DIR)/pd
	@./gookv-pd \
		--addr $(PD_ADDR) \
		--cluster-id 1 \
		--data-dir $(PD_CLUSTER_DIR)/pd \
		> $(PD_CLUSTER_DIR)/pd.log 2>&1 & \
	echo $$! > $(PD_CLUSTER_DIR)/pd.pid; \
	echo "  PD: addr=$(PD_ADDR) pid=$$(cat $(PD_CLUSTER_DIR)/pd.pid)"
	@sleep 1
	@for i in $$(seq 1 $(CLUSTER_NODES)); do \
		GRPC_PORT=$$((20159 + $$i)); \
		STATUS_PORT=$$((20179 + $$i)); \
		DATA_DIR=$(PD_CLUSTER_DIR)/node$$i; \
		PID_FILE=$(PD_CLUSTER_DIR)/node$$i.pid; \
		LOG_FILE=$(PD_CLUSTER_DIR)/node$$i.log; \
		mkdir -p $$DATA_DIR; \
		./gookv-server \
			--store-id $$i \
			--addr 127.0.0.1:$$GRPC_PORT \
			--status-addr 127.0.0.1:$$STATUS_PORT \
			--data-dir $$DATA_DIR \
			--pd-endpoints $(PD_ADDR) \
			--initial-cluster $(CLUSTER_TOPOLOGY) \
			> $$LOG_FILE 2>&1 & \
		echo $$! > $$PID_FILE; \
		echo "  Node $$i: gRPC=127.0.0.1:$$GRPC_PORT status=127.0.0.1:$$STATUS_PORT pd=$(PD_ADDR) pid=$$(cat $$PID_FILE)"; \
	done
	@echo "PD cluster started. Use 'make pd-cluster-stop' to shut down."

pd-cluster-stop:
	@echo "Stopping PD cluster..."
	@for i in $$(seq 1 $(CLUSTER_NODES)); do \
		PID_FILE=$(PD_CLUSTER_DIR)/node$$i.pid; \
		if [ -f $$PID_FILE ]; then \
			PID=$$(cat $$PID_FILE); \
			if kill -0 $$PID 2>/dev/null; then \
				kill $$PID 2>/dev/null; \
				sleep 1; \
				if kill -0 $$PID 2>/dev/null; then kill -9 $$PID 2>/dev/null; fi; \
				echo "  Node $$i (pid $$PID): stopped"; \
			else \
				echo "  Node $$i (pid $$PID): already stopped"; \
			fi; \
			rm -f $$PID_FILE; \
		fi; \
	done
	@PID_FILE=$(PD_CLUSTER_DIR)/pd.pid; \
	if [ -f $$PID_FILE ]; then \
		PID=$$(cat $$PID_FILE); \
		if kill -0 $$PID 2>/dev/null; then \
			kill $$PID 2>/dev/null; \
			sleep 1; \
			if kill -0 $$PID 2>/dev/null; then kill -9 $$PID 2>/dev/null; fi; \
			echo "  PD (pid $$PID): stopped"; \
		else \
			echo "  PD (pid $$PID): already stopped"; \
		fi; \
		rm -f $$PID_FILE; \
	fi
	@rm -rf $(PD_CLUSTER_DIR)
	@echo "PD cluster stopped and data cleaned up."

pd-cluster-verify:
	@echo "Verifying PD cluster replication..."
	@go run scripts/pd-cluster-verify/main.go

TXN_DEMO_DIR = /tmp/gookv-txn-demo
TXN_DEMO_NODES = 3
TXN_DEMO_TOPOLOGY = 1=127.0.0.1:20170,2=127.0.0.1:20171,3=127.0.0.1:20172
TXN_DEMO_PD_ADDR = 127.0.0.1:2389

txn-demo-start: build
	@echo "Starting PD + $(TXN_DEMO_NODES)-node gookv cluster for txn demo..."
	@# Ensure no stale processes from a previous run.
	@for PID_FILE in $(TXN_DEMO_DIR)/*.pid; do \
		if [ -f "$$PID_FILE" ]; then \
			PID=$$(cat "$$PID_FILE"); \
			kill -9 $$PID 2>/dev/null || true; \
		fi; \
	done
	@sleep 1
	@rm -rf $(TXN_DEMO_DIR)
	@mkdir -p $(TXN_DEMO_DIR)/pd
	@./gookv-pd \
		--addr $(TXN_DEMO_PD_ADDR) \
		--cluster-id 1 \
		--data-dir $(TXN_DEMO_DIR)/pd \
		> $(TXN_DEMO_DIR)/pd.log 2>&1 & \
	echo $$! > $(TXN_DEMO_DIR)/pd.pid; \
	echo "  PD: addr=$(TXN_DEMO_PD_ADDR) pid=$$(cat $(TXN_DEMO_DIR)/pd.pid)"
	@sleep 1
	@for i in $$(seq 1 $(TXN_DEMO_NODES)); do \
		GRPC_PORT=$$((20169 + $$i)); \
		STATUS_PORT=$$((20189 + $$i)); \
		DATA_DIR=$(TXN_DEMO_DIR)/node$$i; \
		PID_FILE=$(TXN_DEMO_DIR)/node$$i.pid; \
		LOG_FILE=$(TXN_DEMO_DIR)/node$$i.log; \
		mkdir -p $$DATA_DIR; \
		./gookv-server \
			--config scripts/txn-demo/config.toml \
			--store-id $$i \
			--addr 127.0.0.1:$$GRPC_PORT \
			--status-addr 127.0.0.1:$$STATUS_PORT \
			--data-dir $$DATA_DIR \
			--pd-endpoints $(TXN_DEMO_PD_ADDR) \
			--initial-cluster $(TXN_DEMO_TOPOLOGY) \
			> $$LOG_FILE 2>&1 & \
		echo $$! > $$PID_FILE; \
		echo "  Node $$i: gRPC=127.0.0.1:$$GRPC_PORT status=127.0.0.1:$$STATUS_PORT pid=$$(cat $$PID_FILE)"; \
	done
	@echo "Txn demo cluster started. Use 'make txn-demo-verify' to run the demo."

txn-demo-verify:
	@echo "Running cross-region transaction demo..."
	@go run scripts/txn-demo-verify/main.go --pd $(TXN_DEMO_PD_ADDR)

txn-demo-stop:
	@echo "Stopping txn demo cluster..."
	@for i in $$(seq 1 $(TXN_DEMO_NODES)); do \
		PID_FILE=$(TXN_DEMO_DIR)/node$$i.pid; \
		if [ -f $$PID_FILE ]; then \
			PID=$$(cat $$PID_FILE); \
			if kill -0 $$PID 2>/dev/null; then \
				kill $$PID 2>/dev/null; \
				sleep 1; \
				if kill -0 $$PID 2>/dev/null; then kill -9 $$PID 2>/dev/null; fi; \
				echo "  Node $$i (pid $$PID): stopped"; \
			else \
				echo "  Node $$i (pid $$PID): already stopped"; \
			fi; \
			rm -f $$PID_FILE; \
		fi; \
	done
	@PID_FILE=$(TXN_DEMO_DIR)/pd.pid; \
	if [ -f $$PID_FILE ]; then \
		PID=$$(cat $$PID_FILE); \
		if kill -0 $$PID 2>/dev/null; then \
			kill $$PID 2>/dev/null; \
			sleep 1; \
			if kill -0 $$PID 2>/dev/null; then kill -9 $$PID 2>/dev/null; fi; \
			echo "  PD (pid $$PID): stopped"; \
		else \
			echo "  PD (pid $$PID): already stopped"; \
		fi; \
		rm -f $$PID_FILE; \
	fi
	@rm -rf $(TXN_DEMO_DIR)
	@echo "Txn demo cluster stopped and data cleaned up."

# --- Dynamic Node Addition Demo ---
SCALE_DEMO_DIR = /tmp/gookv-scale-demo
SCALE_DEMO_NODES = 3
SCALE_DEMO_MAX_NODES = 4
SCALE_DEMO_TOPOLOGY = 1=127.0.0.1:20270,2=127.0.0.1:20271,3=127.0.0.1:20272
SCALE_DEMO_PD_ADDR = 127.0.0.1:2399

scale-demo-start: build
	@echo "Starting PD + $(SCALE_DEMO_NODES)-node gookv cluster for scale demo..."
	@# Ensure no stale processes from a previous run.
	@for PID_FILE in $(SCALE_DEMO_DIR)/*.pid; do \
		if [ -f "$$PID_FILE" ]; then \
			PID=$$(cat "$$PID_FILE"); \
			kill -9 $$PID 2>/dev/null || true; \
		fi; \
	done
	@sleep 1
	@rm -rf $(SCALE_DEMO_DIR)
	@mkdir -p $(SCALE_DEMO_DIR)/pd
	@./gookv-pd \
		--addr $(SCALE_DEMO_PD_ADDR) \
		--cluster-id 1 \
		--data-dir $(SCALE_DEMO_DIR)/pd \
		> $(SCALE_DEMO_DIR)/pd.log 2>&1 & \
	echo $$! > $(SCALE_DEMO_DIR)/pd.pid; \
	echo "  PD: addr=$(SCALE_DEMO_PD_ADDR) pid=$$(cat $(SCALE_DEMO_DIR)/pd.pid)"
	@sleep 1
	@for i in $$(seq 1 $(SCALE_DEMO_NODES)); do \
		GRPC_PORT=$$((20269 + $$i)); \
		STATUS_PORT=$$((20289 + $$i)); \
		DATA_DIR=$(SCALE_DEMO_DIR)/node$$i; \
		PID_FILE=$(SCALE_DEMO_DIR)/node$$i.pid; \
		LOG_FILE=$(SCALE_DEMO_DIR)/node$$i.log; \
		mkdir -p $$DATA_DIR; \
		./gookv-server \
			--config scripts/txn-demo/config.toml \
			--store-id $$i \
			--addr 127.0.0.1:$$GRPC_PORT \
			--status-addr 127.0.0.1:$$STATUS_PORT \
			--data-dir $$DATA_DIR \
			--pd-endpoints $(SCALE_DEMO_PD_ADDR) \
			--initial-cluster $(SCALE_DEMO_TOPOLOGY) \
			> $$LOG_FILE 2>&1 & \
		echo $$! > $$PID_FILE; \
		echo "  Node $$i: gRPC=127.0.0.1:$$GRPC_PORT status=127.0.0.1:$$STATUS_PORT pid=$$(cat $$PID_FILE)"; \
	done
	@echo "Scale demo cluster started. Use 'make scale-demo-verify' to run the demo."

scale-demo-verify:
	@echo "Running dynamic node addition demo..."
	@go run scripts/scale-demo-verify/main.go --pd $(SCALE_DEMO_PD_ADDR) --data-dir $(SCALE_DEMO_DIR) --config scripts/txn-demo/config.toml

scale-demo-stop:
	@echo "Stopping scale demo cluster..."
	@for i in $$(seq 1 $(SCALE_DEMO_MAX_NODES)); do \
		PID_FILE=$(SCALE_DEMO_DIR)/node$$i.pid; \
		if [ -f $$PID_FILE ]; then \
			PID=$$(cat $$PID_FILE); \
			if kill -0 $$PID 2>/dev/null; then \
				kill $$PID 2>/dev/null; \
				sleep 1; \
				if kill -0 $$PID 2>/dev/null; then kill -9 $$PID 2>/dev/null; fi; \
				echo "  Node $$i (pid $$PID): stopped"; \
			else \
				echo "  Node $$i (pid $$PID): already stopped"; \
			fi; \
			rm -f $$PID_FILE; \
		fi; \
	done
	@PID_FILE=$(SCALE_DEMO_DIR)/pd.pid; \
	if [ -f $$PID_FILE ]; then \
		PID=$$(cat $$PID_FILE); \
		if kill -0 $$PID 2>/dev/null; then \
			kill $$PID 2>/dev/null; \
			sleep 1; \
			if kill -0 $$PID 2>/dev/null; then kill -9 $$PID 2>/dev/null; fi; \
			echo "  PD (pid $$PID): stopped"; \
		else \
			echo "  PD (pid $$PID): already stopped"; \
		fi; \
		rm -f $$PID_FILE; \
	fi
	@rm -rf $(SCALE_DEMO_DIR)
	@echo "Scale demo cluster stopped and data cleaned up."

# --- PD Leader Failover Demo ---
PD_FAILOVER_DIR = /tmp/gookv-pd-failover-demo
PD_FAILOVER_PD_NODES = 3
PD_FAILOVER_KVS_NODES = 3
PD_FAILOVER_INITIAL_CLUSTER = 1=127.0.0.1:2410,2=127.0.0.1:2412,3=127.0.0.1:2414
PD_FAILOVER_CLIENT_CLUSTER = 1=127.0.0.1:2409,2=127.0.0.1:2411,3=127.0.0.1:2413
PD_FAILOVER_PD_ENDPOINTS = 127.0.0.1:2409,127.0.0.1:2411,127.0.0.1:2413
PD_FAILOVER_KVS_TOPOLOGY = 1=127.0.0.1:20370,2=127.0.0.1:20371,3=127.0.0.1:20372

pd-failover-demo-start: build
	@echo "Starting 3-PD Raft cluster + 3 KVS nodes for failover demo..."
	@# Ensure no stale processes from a previous run.
	@for PID_FILE in $(PD_FAILOVER_DIR)/*.pid; do \
		if [ -f "$$PID_FILE" ]; then \
			PID=$$(cat "$$PID_FILE"); \
			kill -9 $$PID 2>/dev/null || true; \
		fi; \
	done
	@sleep 1
	@rm -rf $(PD_FAILOVER_DIR)
	@for i in 1 2 3; do \
		CLIENT_PORT=$$((2407 + $$i * 2)); \
		PEER_PORT=$$((2408 + $$i * 2)); \
		DATA_DIR=$(PD_FAILOVER_DIR)/pd$$i; \
		PID_FILE=$(PD_FAILOVER_DIR)/pd$$i.pid; \
		LOG_FILE=$(PD_FAILOVER_DIR)/pd$$i.log; \
		mkdir -p $$DATA_DIR; \
		./gookv-pd \
			--pd-id $$i \
			--initial-cluster $(PD_FAILOVER_INITIAL_CLUSTER) \
			--peer-port 127.0.0.1:$$PEER_PORT \
			--client-cluster $(PD_FAILOVER_CLIENT_CLUSTER) \
			--addr 127.0.0.1:$$CLIENT_PORT \
			--data-dir $$DATA_DIR \
			--cluster-id 1 \
			> $$LOG_FILE 2>&1 & \
		echo $$! > $$PID_FILE; \
		echo "  PD $$i: client=127.0.0.1:$$CLIENT_PORT peer=127.0.0.1:$$PEER_PORT pid=$$(cat $$PID_FILE)"; \
	done
	@echo "  Waiting for PD leader election..."
	@sleep 3
	@for i in $$(seq 1 $(PD_FAILOVER_KVS_NODES)); do \
		GRPC_PORT=$$((20369 + $$i)); \
		STATUS_PORT=$$((20389 + $$i)); \
		DATA_DIR=$(PD_FAILOVER_DIR)/node$$i; \
		PID_FILE=$(PD_FAILOVER_DIR)/node$$i.pid; \
		LOG_FILE=$(PD_FAILOVER_DIR)/node$$i.log; \
		mkdir -p $$DATA_DIR; \
		./gookv-server \
			--store-id $$i \
			--addr 127.0.0.1:$$GRPC_PORT \
			--status-addr 127.0.0.1:$$STATUS_PORT \
			--data-dir $$DATA_DIR \
			--pd-endpoints $(PD_FAILOVER_PD_ENDPOINTS) \
			--initial-cluster $(PD_FAILOVER_KVS_TOPOLOGY) \
			> $$LOG_FILE 2>&1 & \
		echo $$! > $$PID_FILE; \
		echo "  Node $$i: gRPC=127.0.0.1:$$GRPC_PORT status=127.0.0.1:$$STATUS_PORT pid=$$(cat $$PID_FILE)"; \
	done
	@echo "PD failover demo cluster started. Use 'make pd-failover-demo-verify' to run the demo."

pd-failover-demo-verify:
	@echo "Running PD leader failover demo..."
	@go run scripts/pd-failover-demo-verify/main.go --pd $(PD_FAILOVER_PD_ENDPOINTS) --data-dir $(PD_FAILOVER_DIR)

pd-failover-demo-stop:
	@echo "Stopping PD failover demo cluster..."
	@for i in $$(seq 1 $(PD_FAILOVER_KVS_NODES)); do \
		PID_FILE=$(PD_FAILOVER_DIR)/node$$i.pid; \
		if [ -f $$PID_FILE ]; then \
			PID=$$(cat $$PID_FILE); \
			if kill -0 $$PID 2>/dev/null; then \
				kill $$PID 2>/dev/null; \
				sleep 1; \
				if kill -0 $$PID 2>/dev/null; then kill -9 $$PID 2>/dev/null; fi; \
				echo "  Node $$i (pid $$PID): stopped"; \
			else \
				echo "  Node $$i (pid $$PID): already stopped"; \
			fi; \
			rm -f $$PID_FILE; \
		fi; \
	done
	@for i in 1 2 3; do \
		PID_FILE=$(PD_FAILOVER_DIR)/pd$$i.pid; \
		if [ -f $$PID_FILE ]; then \
			PID=$$(cat $$PID_FILE); \
			if kill -0 $$PID 2>/dev/null; then \
				kill $$PID 2>/dev/null; \
				sleep 1; \
				if kill -0 $$PID 2>/dev/null; then kill -9 $$PID 2>/dev/null; fi; \
				echo "  PD $$i (pid $$PID): stopped"; \
			else \
				echo "  PD $$i (pid $$PID): already stopped"; \
			fi; \
			rm -f $$PID_FILE; \
		fi; \
	done
	@rm -rf $(PD_FAILOVER_DIR)
	@echo "PD failover demo cluster stopped and data cleaned up."

# --- Transaction Integrity Demo ---
TXN_INTEGRITY_DIR = /tmp/gookv-txn-integrity-demo
TXN_INTEGRITY_NODES = 3
TXN_INTEGRITY_TOPOLOGY = 1=127.0.0.1:20470,2=127.0.0.1:20471,3=127.0.0.1:20472
TXN_INTEGRITY_PD_ADDR = 127.0.0.1:2419

txn-integrity-demo-start: build
	@echo "Starting PD + $(TXN_INTEGRITY_NODES)-node gookv cluster for txn integrity demo..."
	@# Ensure no stale processes from a previous run.
	@for PID_FILE in $(TXN_INTEGRITY_DIR)/*.pid; do \
		if [ -f "$$PID_FILE" ]; then \
			PID=$$(cat "$$PID_FILE"); \
			kill -9 $$PID 2>/dev/null || true; \
		fi; \
	done
	@sleep 1
	@rm -rf $(TXN_INTEGRITY_DIR)
	@mkdir -p $(TXN_INTEGRITY_DIR)/pd
	@./gookv-pd \
		--addr $(TXN_INTEGRITY_PD_ADDR) \
		--cluster-id 1 \
		--data-dir $(TXN_INTEGRITY_DIR)/pd \
		> $(TXN_INTEGRITY_DIR)/pd.log 2>&1 & \
	echo $$! > $(TXN_INTEGRITY_DIR)/pd.pid; \
	echo "  PD: addr=$(TXN_INTEGRITY_PD_ADDR) pid=$$(cat $(TXN_INTEGRITY_DIR)/pd.pid)"
	@sleep 1
	@for i in $$(seq 1 $(TXN_INTEGRITY_NODES)); do \
		GRPC_PORT=$$((20469 + $$i)); \
		STATUS_PORT=$$((20489 + $$i)); \
		DATA_DIR=$(TXN_INTEGRITY_DIR)/node$$i; \
		PID_FILE=$(TXN_INTEGRITY_DIR)/node$$i.pid; \
		LOG_FILE=$(TXN_INTEGRITY_DIR)/node$$i.log; \
		mkdir -p $$DATA_DIR; \
		./gookv-server \
			--config scripts/txn-integrity-demo/config.toml \
			--store-id $$i \
			--addr 127.0.0.1:$$GRPC_PORT \
			--status-addr 127.0.0.1:$$STATUS_PORT \
			--data-dir $$DATA_DIR \
			--pd-endpoints $(TXN_INTEGRITY_PD_ADDR) \
			--initial-cluster $(TXN_INTEGRITY_TOPOLOGY) \
			> $$LOG_FILE 2>&1 & \
		echo $$! > $$PID_FILE; \
		echo "  Node $$i: gRPC=127.0.0.1:$$GRPC_PORT status=127.0.0.1:$$STATUS_PORT pid=$$(cat $$PID_FILE)"; \
	done
	@echo "Txn integrity demo cluster started. Use 'make txn-integrity-demo-verify' to run the demo."

txn-integrity-demo-verify:
	@echo "Running transaction integrity demo..."
	@go run scripts/txn-integrity-demo-verify/main.go --pd $(TXN_INTEGRITY_PD_ADDR)

txn-integrity-demo-stop:
	@echo "Stopping txn integrity demo cluster..."
	@for i in $$(seq 1 $(TXN_INTEGRITY_NODES)); do \
		PID_FILE=$(TXN_INTEGRITY_DIR)/node$$i.pid; \
		if [ -f $$PID_FILE ]; then \
			PID=$$(cat $$PID_FILE); \
			if kill -0 $$PID 2>/dev/null; then \
				kill $$PID 2>/dev/null; \
				sleep 1; \
				if kill -0 $$PID 2>/dev/null; then kill -9 $$PID 2>/dev/null; fi; \
				echo "  Node $$i (pid $$PID): stopped"; \
			else \
				echo "  Node $$i (pid $$PID): already stopped"; \
			fi; \
			rm -f $$PID_FILE; \
		fi; \
	done
	@PID_FILE=$(TXN_INTEGRITY_DIR)/pd.pid; \
	if [ -f $$PID_FILE ]; then \
		PID=$$(cat $$PID_FILE); \
		if kill -0 $$PID 2>/dev/null; then \
			kill $$PID 2>/dev/null; \
			sleep 1; \
			if kill -0 $$PID 2>/dev/null; then kill -9 $$PID 2>/dev/null; fi; \
			echo "  PD (pid $$PID): stopped"; \
		else \
			echo "  PD (pid $$PID): already stopped"; \
		fi; \
		rm -f $$PID_FILE; \
	fi
	@rm -rf $(TXN_INTEGRITY_DIR)
	@echo "Txn integrity demo cluster stopped and data cleaned up."
