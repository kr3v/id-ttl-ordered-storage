package id_ttl_ordered_storage

import (
	"unicode/utf8"
	"unsafe"
)

// A StringBuilderUnsafe is used to efficiently build a string using Write methods.
// It minimizes memory copying. The zero value is ready to use.
// Do not copy a non-zero StringBuilderUnsafe.
type StringBuilderUnsafe struct {
	buf []byte
}

// String returns the accumulated string.
func (b *StringBuilderUnsafe) String() string {
	return *(*string)(unsafe.Pointer(&b.buf))
}

// Len returns the number of accumulated bytes; b.Len() == len(b.String()).
func (b *StringBuilderUnsafe) Len() int { return len(b.buf) }

// Cap returns the capacity of the builder's underlying byte slice. It is the
// total space allocated for the string being built and includes any bytes
// already written.
func (b *StringBuilderUnsafe) Cap() int { return cap(b.buf) }

// Reset resets the StringBuilderUnsafe to be empty.
func (b *StringBuilderUnsafe) Reset() {
	b.buf = b.buf[:0]
}

// grow copies the buffer to a new, larger buffer so that there are at least n
// bytes of capacity beyond len(b.buf).
func (b *StringBuilderUnsafe) grow(n int) {
	buf := make([]byte, len(b.buf), 2*cap(b.buf)+n)
	copy(buf, b.buf)
	b.buf = buf
}

// Grow grows b's capacity, if necessary, to guarantee space for
// another n bytes. After Grow(n), at least n bytes can be written to b
// without another allocation. If n is negative, Grow panics.
func (b *StringBuilderUnsafe) Grow(n int) {
	if n < 0 {
		panic("strings.StringBuilderUnsafe.Grow: negative count")
	}
	if cap(b.buf)-len(b.buf) < n {
		b.grow(n)
	}
}

// Write appends the contents of p to b's buffer.
// Write always returns len(p), nil.
func (b *StringBuilderUnsafe) Write(p []byte) int {
	b.buf = append(b.buf, p...)
	return len(p)
}

// WriteByte appends the byte c to b's buffer.
// The returned error is always nil.
func (b *StringBuilderUnsafe) WriteByte(c byte) {
	b.buf = append(b.buf, c)
}

// WriteRune appends the UTF-8 encoding of Unicode code point r to b's buffer.
// It returns the length of r and a nil error.
func (b *StringBuilderUnsafe) WriteRune(r rune) int {
	// Compare as uint32 to correctly handle negative runes.
	if uint32(r) < utf8.RuneSelf {
		b.buf = append(b.buf, byte(r))
		return 1
	}
	l := len(b.buf)
	if cap(b.buf)-l < utf8.UTFMax {
		b.grow(utf8.UTFMax)
	}
	n := utf8.EncodeRune(b.buf[l:l+utf8.UTFMax], r)
	b.buf = b.buf[:l+n]
	return n
}

// WriteString appends the contents of s to b's buffer.
// It returns the length of s and a nil error.
func (b *StringBuilderUnsafe) WriteString(s string) int {
	b.buf = append(b.buf, s...)
	return len(s)
}
