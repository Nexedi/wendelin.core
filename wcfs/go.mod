module lab.nexedi.com/nexedi/wendelin.core/wcfs

go 1.14

require (
	github.com/golang/glog v1.0.0
	github.com/hanwen/go-fuse/v2 v2.4.2 // replaced to -> kirr/go-fuse@y/nodefs-cancel
	github.com/johncgriffin/overflow v0.0.0-20211019200055-46fa312c352c
	github.com/kisielk/og-rek v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.8.4
	lab.nexedi.com/kirr/go123 v0.0.0-20230822135329-95433de34faf
	lab.nexedi.com/kirr/neo/go v0.0.0-20240723085959-839ee634bd66
)

// we use kirr/go-fuse@y/nodefs-cancel
// see https://github.com/hanwen/go-fuse/pull/343 for details
replace github.com/hanwen/go-fuse/v2 v2.4.2 => lab.nexedi.com/kirr/go-fuse/v2 v2.4.2-0.20231211215333-9f9ad4a1c7cc
