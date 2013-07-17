package chord

import (
	"math/big"
	"testing"
)

func TestDistance(t *testing.T) {
	a := []byte{63}
	b := []byte{3}
	d := distance(a, b, 6) // Ring size of 64
	if d.Cmp(big.NewInt(4)) != 0 {
		t.Fatalf("expect distance 4! %v", d)
	}

	a = []byte{0}
	b = []byte{65}
	d = distance(a, b, 7) // Ring size of 128
	if d.Cmp(big.NewInt(63)) != 0 {
		t.Fatalf("expect distance 63! %v", d)
	}

	a = []byte{1}
	b = []byte{255}
	d = distance(a, b, 8) // Ring size of 256
	if d.Cmp(big.NewInt(2)) != 0 {
		t.Fatalf("expect distance 2! %v", d)
	}
}
