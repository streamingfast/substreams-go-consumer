package main

import pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"

type ModuleOutput pbsubstreams.Module_Output

func (o *ModuleOutput) TypeName() string {
	if o == nil || (*pbsubstreams.Module_Output)(o) == nil {
		return ""
	}

	return (*pbsubstreams.Module_Output)(o).Type
}

func (o *ModuleOutput) String() string {
	if o == nil || (*pbsubstreams.Module_Output)(o) == nil {
		return "<None>"
	}

	return (*pbsubstreams.Module_Output)(o).Type
}
