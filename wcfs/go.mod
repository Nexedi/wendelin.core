module lab.nexedi.com/nexedi/wendelin.core/wcfs

go 1.19

require (
	github.com/golang/glog v1.2.2
	github.com/hanwen/go-fuse/v2 v2.4.2 // replaced to -> kirr/go-fuse@y/nodefs-cancel
	github.com/johncgriffin/overflow v0.0.0-20211019200055-46fa312c352c
	github.com/kisielk/og-rek v1.2.1-0.20240923165241-e691997e3596
	github.com/pkg/errors v0.9.1
	github.com/shirou/gopsutil/v4 v4.24.8
	github.com/stretchr/testify v1.9.0
	lab.nexedi.com/kirr/go123 v0.0.0-20250817102603-bdd89cab9f4a
	lab.nexedi.com/kirr/neo/go v0.0.0-20240924102820-6235fb602308
)

require (
	crawshaw.io/sqlite v0.3.2 // indirect
	github.com/DataDog/czlib v0.0.0-20210322182103-8087f4e14ae7 // indirect
	github.com/aristanetworks/gomap v0.0.0-20230726210543-f4e41046dced // indirect
	github.com/cznic/strutil v0.0.0-20181122101858-275e90344537 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/philhofer/fwd v1.1.3-0.20240612014219-fbbf4953d986 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/shamaton/msgpack v1.2.1 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/someonegg/gocontainer v1.0.0 // indirect
	github.com/tinylib/msgp v1.2.0 // indirect
	github.com/tklauser/go-sysconf v0.3.12 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go4.org/unsafe/assume-no-moving-gc v0.0.0-20231121144256-b99613f794b6 // indirect
	golang.org/x/exp v0.0.0-20230725093048-515e97ebf090 // indirect
	golang.org/x/sys v0.24.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// we use kirr/go-fuse@y/nodefs-cancel
// see https://github.com/hanwen/go-fuse/pull/343 for details
replace github.com/hanwen/go-fuse/v2 v2.4.2 => lab.nexedi.com/kirr/go-fuse/v2 v2.4.2-0.20231211215333-9f9ad4a1c7cc
