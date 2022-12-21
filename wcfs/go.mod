module lab.nexedi.com/nexedi/wendelin.core/wcfs

go 1.14

require (
	github.com/golang/glog v1.0.0
	github.com/hanwen/go-fuse/v2 v2.2.0 // replaced to -> kirr/go-fuse@y/nodefs-cancel
	github.com/johncgriffin/overflow v0.0.0-20211019200055-46fa312c352c
	github.com/kisielk/og-rek v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.8.1
	lab.nexedi.com/kirr/go123 v0.0.0-20221221144149-4def45d2dd95
	lab.nexedi.com/kirr/neo/go v0.0.0-20221104095432-3e13fa061a13
)

// we use kirr/go-fuse@y/nodefs-cancel
// see https://github.com/hanwen/go-fuse/pull/343 for details
replace github.com/hanwen/go-fuse/v2 v2.2.0 => lab.nexedi.com/kirr/go-fuse/v2 v2.1.1-0.20221221150221-643585d735fa
