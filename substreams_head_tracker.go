package sinknoop

import (
	_ "embed"
	"fmt"

	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"google.golang.org/protobuf/proto"
)

//go:embed substreams-head-tracker/substreams-head-tracker-v1.0.0.spkg
var SubstreamsHeadTrackerSPKG []byte

var substreamsHeadTracker *pbsubstreams.Package

func AddHeadTrackerManifestToSubstreamsRequest(req *pbsubstreamsrpc.Request) (*pbsubstreamsrpc.Request, error) {
	if substreamsHeadTracker == nil {
		pkg := &pbsubstreams.Package{}
		err := proto.Unmarshal(SubstreamsHeadTrackerSPKG, pkg)
		if err != nil {
			return nil, fmt.Errorf("unmarshal head tracker spkg: %w", err)
		}

		substreamsHeadTracker = pkg
	}

	req.Modules = substreamsHeadTracker.Modules
	req.OutputModule = "map_blocks"

	return req, nil
}
