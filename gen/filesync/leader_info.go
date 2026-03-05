// leader_info.go — GetLeaderInfo message types and helpers.
// These are defined separately since the protobuf compiler is not available
// to regenerate from the updated .proto file. They are plain Go structs
// that are used by the GetLeaderInfo RPC handler and the client CLI.

package filesync

// GetLeaderInfoRequest queries the current leader's identity.
type GetLeaderInfoRequest struct {
	// The node ID making the request (for logging).
	RequestingNodeId int32 `protobuf:"varint,1,opt,name=requesting_node_id,json=requestingNodeId,proto3" json:"requesting_node_id,omitempty"`
}

func (x *GetLeaderInfoRequest) Reset()         {}
func (x *GetLeaderInfoRequest) String() string { return "GetLeaderInfoRequest" }
func (x *GetLeaderInfoRequest) ProtoMessage()  {}

func (x *GetLeaderInfoRequest) GetRequestingNodeId() int32 {
	if x != nil {
		return x.RequestingNodeId
	}
	return 0
}

// GetLeaderInfoResponse returns the current leader's IP address and metadata.
type GetLeaderInfoResponse struct {
	// The node ID of the current master.
	MasterNodeId int32 `protobuf:"varint,1,opt,name=master_node_id,json=masterNodeId,proto3" json:"master_node_id,omitempty"`
	// The IP:port address of the current master (e.g., "10.12.235.187:50051").
	MasterAddress string `protobuf:"bytes,2,opt,name=master_address,json=masterAddress,proto3" json:"master_address,omitempty"`
	// The current election term.
	Term int64 `protobuf:"varint,3,opt,name=term,proto3" json:"term,omitempty"`
	// Whether the responding node itself is the current master.
	IsSelfMaster bool `protobuf:"varint,4,opt,name=is_self_master,json=isSelfMaster,proto3" json:"is_self_master,omitempty"`
	// The node ID of the responding node.
	RespondingNodeId int32 `protobuf:"varint,5,opt,name=responding_node_id,json=respondingNodeId,proto3" json:"responding_node_id,omitempty"`
}

func (x *GetLeaderInfoResponse) Reset()         {}
func (x *GetLeaderInfoResponse) String() string { return "GetLeaderInfoResponse" }
func (x *GetLeaderInfoResponse) ProtoMessage()  {}

func (x *GetLeaderInfoResponse) GetMasterNodeId() int32 {
	if x != nil {
		return x.MasterNodeId
	}
	return 0
}

func (x *GetLeaderInfoResponse) GetMasterAddress() string {
	if x != nil {
		return x.MasterAddress
	}
	return ""
}

func (x *GetLeaderInfoResponse) GetTerm() int64 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *GetLeaderInfoResponse) GetIsSelfMaster() bool {
	if x != nil {
		return x.IsSelfMaster
	}
	return false
}

func (x *GetLeaderInfoResponse) GetRespondingNodeId() int32 {
	if x != nil {
		return x.RespondingNodeId
	}
	return 0
}
