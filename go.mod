module zraft

go 1.21.3

require (
	github.com/google/uuid v1.4.0
	github.com/stretchr/testify v1.8.4
	github.com/syndtr/goleveldb v1.0.0
	github.com/zhangyu0310/zlogger v0.1.7
	google.golang.org/grpc v1.59.0
	google.golang.org/protobuf v1.31.0
	zdb v0.0.0-00010101000000-000000000000
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/net v0.14.0 // indirect
	golang.org/x/sys v0.11.0 // indirect
	golang.org/x/text v0.12.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230822172742-b8732ec3820d // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace zdb => github.com/zhangyu0310/zdb v0.0.0-20231129161656-92a42e2f22be
