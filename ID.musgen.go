package id_ttl_ordered_storage

import "github.com/ymz-ncnk/musgo/errs"

// MarshalMUS fills buf with the MUS encoding of v.
func (v ID) MarshalMUS(buf []byte) int {
	i := 0
	{
		uv := uint64(v.pathIdx<<1) ^ uint64(v.pathIdx>>63)
		{
			for uv >= 0x80 {
				buf[i] = byte(uv) | 0x80
				uv >>= 7
				i++
			}
			buf[i] = byte(uv)
			i++
		}
	}
	{
		uv := uint64(v.offset<<1) ^ uint64(v.offset>>63)
		{
			for uv >= 0x80 {
				buf[i] = byte(uv) | 0x80
				uv >>= 7
				i++
			}
			buf[i] = byte(uv)
			i++
		}
	}
	{
		uv := uint64(v.length<<1) ^ uint64(v.length>>63)
		{
			for uv >= 0x80 {
				buf[i] = byte(uv) | 0x80
				uv >>= 7
				i++
			}
			buf[i] = byte(uv)
			i++
		}
	}
	return i
}

// UnmarshalMUS parses the MUS-encoded buf, and sets the result to *v.
func (v *ID) UnmarshalMUS(buf []byte) (int, error) {
	i := 0
	var err error
	{
		var uv uint64
		{
			if i > len(buf)-1 {
				return i, errs.ErrSmallBuf
			}
			shift := 0
			done := false
			for l, b := range buf[i:] {
				if l == 9 && b > 1 {
					return i, errs.ErrOverflow
				}
				if b < 0x80 {
					uv = uv | uint64(b)<<shift
					done = true
					i += l + 1
					break
				}
				uv = uv | uint64(b&0x7F)<<shift
				shift += 7
			}
			if !done {
				return i, errs.ErrSmallBuf
			}
		}
		uv = (uv >> 1) ^ uint64((int(uv&1)<<63)>>63)
		v.pathIdx = int(uv)
	}
	if err != nil {
		return i, errs.NewFieldError("pathIdx", err)
	}
	{
		var uv uint64
		{
			if i > len(buf)-1 {
				return i, errs.ErrSmallBuf
			}
			shift := 0
			done := false
			for l, b := range buf[i:] {
				if l == 9 && b > 1 {
					return i, errs.ErrOverflow
				}
				if b < 0x80 {
					uv = uv | uint64(b)<<shift
					done = true
					i += l + 1
					break
				}
				uv = uv | uint64(b&0x7F)<<shift
				shift += 7
			}
			if !done {
				return i, errs.ErrSmallBuf
			}
		}
		uv = (uv >> 1) ^ uint64((int64(uv&1)<<63)>>63)
		v.offset = int64(uv)
	}
	if err != nil {
		return i, errs.NewFieldError("offset", err)
	}
	{
		var uv uint64
		{
			if i > len(buf)-1 {
				return i, errs.ErrSmallBuf
			}
			shift := 0
			done := false
			for l, b := range buf[i:] {
				if l == 9 && b > 1 {
					return i, errs.ErrOverflow
				}
				if b < 0x80 {
					uv = uv | uint64(b)<<shift
					done = true
					i += l + 1
					break
				}
				uv = uv | uint64(b&0x7F)<<shift
				shift += 7
			}
			if !done {
				return i, errs.ErrSmallBuf
			}
		}
		uv = (uv >> 1) ^ uint64((int(uv&1)<<63)>>63)
		v.length = int(uv)
	}
	if err != nil {
		return i, errs.NewFieldError("length", err)
	}
	return i, err
}

// SizeMUS returns the size of the MUS-encoded v.
func (v ID) SizeMUS() int {
	size := 0
	{
		uv := uint64(v.pathIdx<<1) ^ uint64(v.pathIdx>>63)
		{
			for uv >= 0x80 {
				uv >>= 7
				size++
			}
			size++
		}
	}
	{
		uv := uint64(v.offset<<1) ^ uint64(v.offset>>63)
		{
			for uv >= 0x80 {
				uv >>= 7
				size++
			}
			size++
		}
	}
	{
		uv := uint64(v.length<<1) ^ uint64(v.length>>63)
		{
			for uv >= 0x80 {
				uv >>= 7
				size++
			}
			size++
		}
	}
	return size
}
