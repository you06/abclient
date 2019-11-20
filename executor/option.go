package executor

// Option struct
type Option struct {
	Clear bool
	Log string
	Reproduce string
	Stable bool
}

// Clone option
func (o *Option) Clone() *Option {
	o1 := *o
	return &o1
}
