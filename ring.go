package chord

import (
	"bytes"
)

// Len is the number of vnodes
func (r *Ring) Len() int {
	return len(r.vnodes)
}

// Less returns whether the vnode with index i should sort
// before the vnode with index j.
func (r *Ring) Less(i, j int) bool {
	return bytes.Compare(r.vnodes[i].Id, r.vnodes[j].Id) == -1
}

// Swap swaps the vnodes with indexes i and j.
func (r *Ring) Swap(i, j int) {
	r.vnodes[i], r.vnodes[j] = r.vnodes[j], r.vnodes[i]
}
