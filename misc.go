package mud

import (
	"time"
)

const (
	epoch  = 315964819000
	millis = 1000
)

func AdjustTime(t time.Time, g bool) time.Time {
	if g {
		return t
	}
	return t.Add(GPS.Sub(UNIX))
}

func AdjustGenerationTime(s int64) time.Time {
	d := time.Duration((epoch + (s * millis))) * time.Millisecond
	return UNIX.Add(d).UTC()
}

func GenerationTimeFromEpoch(s int64) int64 {
	return epoch + (s * millis)
}

func AdjustAcquisitionTime(s int64) time.Time {
	d := time.Duration(millis*(s+34)) * time.Millisecond
	return UNIX.Add(d).UTC()
}

func AcquisitionTimeFromEpoch(s int64) int64 {
	return millis * (s + 34)
}
