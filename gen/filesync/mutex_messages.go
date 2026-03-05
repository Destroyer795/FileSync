// mutex_messages.go — MutexRequest/MutexReply message types for Ricart-Agrawala.
// These are defined separately since the protobuf compiler is not available
// to regenerate from the updated .proto file. They are plain Go structs
// that implement the protobuf message interface and are used by the
// Ricart-Agrawala mutual exclusion RPCs.

package filesync

// MutexRequestMsg is sent by a node that wants to enter the critical section.
// Per the Ricart-Agrawala algorithm, this is multicast to ALL other nodes.
type MutexRequestMsg struct {
	// The node ID requesting access to the critical section.
	NodeId int32 `protobuf:"varint,1,opt,name=node_id,json=nodeId,proto3" json:"node_id,omitempty"`
	// Lamport logical timestamp when the request was made.
	Timestamp int64 `protobuf:"varint,2,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	// Current election term for validation.
	Term int64 `protobuf:"varint,3,opt,name=term,proto3" json:"term,omitempty"`
}

func (x *MutexRequestMsg) Reset()         {}
func (x *MutexRequestMsg) String() string { return "MutexRequestMsg" }
func (x *MutexRequestMsg) ProtoMessage()  {}

func (x *MutexRequestMsg) GetNodeId() int32 {
	if x != nil {
		return x.NodeId
	}
	return 0
}

func (x *MutexRequestMsg) GetTimestamp() int64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

func (x *MutexRequestMsg) GetTerm() int64 {
	if x != nil {
		return x.Term
	}
	return 0
}

// MutexReplyMsg is sent in response to a MutexRequestMsg.
// A node sends this either immediately (if not interested in CS or lower priority)
// or after exiting the CS (deferred reply).
type MutexReplyMsg struct {
	// The node ID sending the reply.
	NodeId int32 `protobuf:"varint,1,opt,name=node_id,json=nodeId,proto3" json:"node_id,omitempty"`
	// Lamport logical timestamp of the reply.
	Timestamp int64 `protobuf:"varint,2,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
}

func (x *MutexReplyMsg) Reset()         {}
func (x *MutexReplyMsg) String() string { return "MutexReplyMsg" }
func (x *MutexReplyMsg) ProtoMessage()  {}

func (x *MutexReplyMsg) GetNodeId() int32 {
	if x != nil {
		return x.NodeId
	}
	return 0
}

func (x *MutexReplyMsg) GetTimestamp() int64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}
