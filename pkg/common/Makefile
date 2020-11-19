_GOPATH := $(PWD)/../../../..
PROTOC := /usr/bin/protoc
export GOPATH := $(_GOPATH)
export PATH := $(_GOPATH)/bin:$(PATH)

# all the paths to the gogoproto-generated code - currently just in the ./queues dir
GOGO_PROTO_CODEGEN_FILES = $(patsubst %.proto, %.pb.go, $(shell find ./queues -type f -name '*.proto'))

.PHONY: build
build:
	go build -v github.com/kentik/common/...

# code generation
.PHONY: codegen
codegen: $(GOGO_PROTO_CODEGEN_FILES)

$(_GOPATH)/src/github.com/gogo/protobuf/protoc-gen-gogoslick:
	go get github.com/gogo/protobuf/protoc-gen-gogoslick

$(_GOPATH)/bin/protoc-gen-gofast:
	go get github.com/gogo/protobuf/protoc-gen-gofast
	go install github.com/gogo/protobuf/protoc-gen-gofast

$(_GOPATH)/src/github.com/gogo/protobuf/proto:
	go get github.com/gogo/protobuf/proto

$(_GOPATH)/src/github.com/gogo/protobuf/jsonpb:
	go get github.com/gogo/protobuf/jsonpb

$(_GOPATH)/src/github.com/gogo/protobuf/protoc-gen-gogo:
	go get github.com/gogo/protobuf/protoc-gen-gogo

$(_GOPATH)/src/github.com/gogo/protobuf/gogoproto:
	go get github.com/gogo/protobuf/gogoproto

.PHONY: test
test:
	go test -v --race github.com/kentik/common/...

.PHONY: vendor
vendor:
	# We have to exclude kql, because during CI we don't have github creds
	# to download private kentik repos.
	# FIXME: either setup github creds in CI, or combine kentik repos.
	mv vendor/manifest vendor/manifest.orig
	< vendor/manifest.orig jq '{version: .version, dependencies: ([.dependencies[] | select(.importpath != "github.com/kentik/kql")])}' > vendor/manifest
	gvt restore
	mv vendor/manifest.orig vendor/manifest

# Ensure the gogoproto codegen files are up to date
%.pb.go: \
	$(_GOPATH)/src/github.com/gogo/protobuf/protoc-gen-gogoslick \
	$(_GOPATH)/bin/protoc-gen-gofast \
  $(_GOPATH)/src/github.com/gogo/protobuf/proto \
	$(_GOPATH)/src/github.com/gogo/protobuf/jsonpb \
	$(_GOPATH)/src/github.com/gogo/protobuf/protoc-gen-gogo \
	$(_GOPATH)/src/github.com/gogo/protobuf/gogoproto \
	%.proto

	$(PROTOC) -I=. \
	-I=$(_GOPATH)/src \
	-I=$(_GOPATH)/src/github.com/gogo/protobuf/protobuf \
	--gogoslick_out=.\
	Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,\
	Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,\
	Mgoogle/protobuf/struct.proto=github.com/gogo/protobuf/types,\
	Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,\
	Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types:. \
	  $*.proto
