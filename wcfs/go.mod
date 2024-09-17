module lab.nexedi.com/nexedi/wendelin.core/wcfs

go 1.19

require (
	github.com/golang/glog v1.0.0
	github.com/hanwen/go-fuse/v2 v2.4.2 // replaced to -> kirr/go-fuse@y/nodefs-cancel
	github.com/johncgriffin/overflow v0.0.0-20211019200055-46fa312c352c
	github.com/kisielk/og-rek v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/shirou/gopsutil/v4 v4.24.8
	github.com/stretchr/testify v1.9.0
	lab.nexedi.com/kirr/go123 v0.0.0-20230822135329-95433de34faf
	lab.nexedi.com/kirr/neo/go v0.0.0-20240723085959-839ee634bd66
)

require (
	crawshaw.io/sqlite v0.3.2 // indirect
	github.com/DataDog/czlib v0.0.0-20210322182103-8087f4e14ae7 // indirect
	github.com/cznic/strutil v0.0.0-20181122101858-275e90344537 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/fsnotify/fsnotify v1.5.1 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/philhofer/fwd v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/shamaton/msgpack v1.2.1 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/someonegg/gocontainer v1.0.0 // indirect
	github.com/tinylib/msgp v1.1.6 // indirect
	github.com/tklauser/go-sysconf v0.3.12 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	golang.org/x/sys v0.24.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// we use kirr/go-fuse@y/nodefs-cancel
// see https://github.com/hanwen/go-fuse/pull/343 for details
replace github.com/hanwen/go-fuse/v2 v2.4.2 => lab.nexedi.com/kirr/go-fuse/v2 v2.4.2-0.20231211215333-9f9ad4a1c7cc
