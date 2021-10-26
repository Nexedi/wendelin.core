module lab.nexedi.com/nexedi/wendelin.core/wcfs

go 1.14

require (
	github.com/golang/glog v1.0.0
	github.com/hanwen/go-fuse/v2 v2.1.0 // replaced to -> kirr/go-fuse@y/nodefs-cancel
	github.com/johncgriffin/overflow v0.0.0-20211019200055-46fa312c352c
	github.com/kisielk/og-rek v1.1.1-0.20210310094122-8def3d024dac
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.7.0
	lab.nexedi.com/kirr/go123 v0.0.0-20210906140734-c9eb28d9e408
	lab.nexedi.com/kirr/neo/go v0.0.0-20211004111643-c74a5a3cd0d0
)

// we use kirr/go-fuse@y/nodefs-cancel
// see https://github.com/hanwen/go-fuse/pull/343 for details
replace github.com/hanwen/go-fuse/v2 v2.1.0 => lab.nexedi.com/kirr/go-fuse/v2 v2.0.0-20210910085851-e6ee85fd0a1e
