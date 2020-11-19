package changed

// IsCurrent returns true if the interface has no end time
func (i *Interface) IsCurrent() bool {
	return i.EndTimeUnixNano == 0
}
