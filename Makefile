CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.17.1

.PHONY: test fmt vet generate manifests check

test:
	go test -race ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

# Deepcopy methods. Regenerate after changing anything in api/.
generate:
	$(CONTROLLER_GEN) object paths=./api/...

# CRD YAML.
manifests:
	$(CONTROLLER_GEN) crd paths=./api/... output:crd:dir=config/crd

# What CI runs. Regenerates and fails if the result differs from what is
# committed -- generated code drifting from its source is invisible until
# something deserializes wrongly at runtime.
check: generate manifests fmt vet test
	@test -z "$$(gofmt -l .)" || { echo "unformatted files:"; gofmt -l .; exit 1; }
	@git diff --exit-code -- api/ config/ || { echo "generated files are stale; run 'make generate manifests'"; exit 1; }
