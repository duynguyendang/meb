package utils

import "unsafe"

// StringToBytes converts a string to a byte slice without copying.
// This is unsafe but significantly faster for large strings.
// The returned bytes must not be modified after conversion.
func StringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// BytesToString converts a byte slice to a string without copying.
// This is unsafe but significantly faster for large byte slices.
// The returned string shares the underlying memory with the byte slice.
func BytesToString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}
