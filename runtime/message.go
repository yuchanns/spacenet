package runtime

// Skynet protocol type constants (matching skynet-src/skynet.h).
const (
	PTypeText      = 0
	PTypeResponse  = 1
	PTypeMulticast = 2
	PTypeClient    = 3
	PTypeSystem    = 4
	PTypeHarbor    = 5
	PTypeSocket    = 6
	PTypeError     = 7
	PTypeQueue     = 8
	PTypeDebug     = 9
	PTypeLua       = 10
	PTypeSnax      = 11
	PTypeTrace     = 12

	PTypeTagDontCopy     = 0x10000
	PTypeTagAllocSession = 0x20000
	PTypeMask            = 0xff

	// MaxMessageSize mirrors Skynet's 24-bit MESSAGE_TYPE_MASK (16 MiB - 1).
	MaxMessageSize = 0xffffff
)

// Message is a single envelope delivered to an Actor's mailbox.
type Message struct {
	Source  uint32
	Session int32
	Type    uint8
	Data    []byte
	// Payload optionally holds a typed Go object (e.g. SocketEvent) for native protocols.
	Payload any
	// OwnedToken, when non-zero, carries a pre-allocated MsgBuf/Object token
	// transferred into the message (used by forward mode or typed events).
	OwnedToken uint64
}

// ServiceHandler is implemented by both built-in Go services (snlua, logger)
// and plugin-provided native Go services.
type ServiceHandler interface {
	Init(actor *Actor, param string) error
	Dispatch(actor *Actor, msg *Message) error
	Signal(actor *Actor, sig int)
	Close(actor *Actor)
}

// ServiceFactory constructs a fresh ServiceHandler instance on LAUNCH.
type ServiceFactory func() ServiceHandler
