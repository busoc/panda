package panda

type CCSDSPacketType int

const (
	SystemPacket CCSDSPacketType = iota
	PayloadPacket
)

type CCSDSPacketSegmentation int

const (
	ContinuationPacket CCSDSPacketSegmentation = iota
	StartPacket
	StopPacket
	UnsegmentedPacket
)

func (c CCSDSPacketSegmentation) String() string {
	switch c {
	default:
		return "***"
	case ContinuationPacket:
		return "continuation"
	case StartPacket:
		return "start"
	case StopPacket:
		return "stop"
	case UnsegmentedPacket:
		return "unsegmented"
	}
}

type ESAPacketTime int

const (
	TimeNotUsed ESAPacketTime = iota
	TimeGenerated
	TimeExecuted
	TimeInvalid
)

type ESAPacketType int

const (
	Default ESAPacketType = iota
	DataDump
	DataSegment
	EssentialHk
	SystemHk
	PayloadHk
	ScienceData
	AncillaryData
	EssentialCmd
	SystemCmd
	PayloadCmd
	DataLoad
	Response
	Report
	Exception
	Acknowledge
)

func (e ESAPacketType) Type() string {
	switch e >> 2 {
	default:
		return "***"
	case 0, 1:
		return "dat"
	case 2:
		return "cmd"
	case 3:
		return "evt"
	}
}

func (e ESAPacketType) String() string {
	switch e {
	default:
		return "***"
	case DataDump:
		return "data dump"
	case DataSegment:
		return "data segment"
	case EssentialHk:
		return "essential hk"
	case SystemHk:
		return "system hk"
	case PayloadHk:
		return "payload hk"
	case ScienceData:
		return "science data"
	case AncillaryData:
		return "ancillary data"
	case EssentialCmd:
		return "essential cmd"
	case SystemCmd:
		return "system cmd"
	case PayloadCmd:
		return "payload cmd"
	case DataLoad:
		return "data load"
	case Response:
		return "response"
	case Report:
		return "report"
	case Exception:
		return "exception"
	case Acknowledge:
		return "acknowledge"
	}
}

type UMIPacketState uint8

const (
	NoValue UMIPacketState = iota
	SameValue
	NewValue
	LatestValue
	ErrorValue
)

func (u UMIPacketState) String() string {
	switch u {
	default:
		return "***"
	case NoValue:
		return "none"
	case SameValue:
		return "same"
	case NewValue:
		return "new"
	case LatestValue:
		return "latest"
	case ErrorValue:
		return "unavailable"
	}
}

type UMIDataType uint8

const (
	Int32 UMIDataType = iota + 1
	Float64
	Binary8
	Reference
	String8
	Long
	Decimal
	Real
	Exponent
	Time
	DateTime
	StringN
	BinaryN
	Bit
)

func (u UMIDataType) String() string {
	switch u {
	default:
		return "***"
	case Int32, Long:
		return "long"
	case Float64, Real, Exponent, Decimal:
		return "double"
	case Binary8, BinaryN:
		return "binary"
	case Reference:
		return "reference"
	case String8, StringN:
		return "string"
	case DateTime, Time:
		return "time"
	case Bit:
		return "bit"
	}
}
