/**
Provides API for byte slices reuse to reduce allocations.
There are a few well known sizes of the buffers which
are allocated often.
*/

package pools

var (
	// List of slice sizes. Only these values can be
	// used for GetFreeBuffer() and ReleaseBuffer() calls.
	gValidSizes = []int{1, 4, 5, 9, 13}
	// Map slice size to a channel inside gByteSlicePools array.
	gSlicesIndex = []int{
		-1, 0, -1, -1, 1,
		2, -1, -1, -1, 3,
		-1, -1, -1, 4}
	// Array of byte slice pools
	gByteSlicePools = make([]chan []byte, len(gValidSizes))
)

// Module initialisation hook.
func init() {
	maxPoolSize := 500
	for _, i := range gValidSizes {
		gByteSlicePools[gSlicesIndex[i]] =
			make(chan []byte, maxPoolSize)
	}
}

// GetFreeBuffer returns slice of given size
// from the pool or create new one if pool is empty.
func GetFreeBuffer(size int) []byte {
	select {
	case buf := <-gByteSlicePools[gSlicesIndex[size]]:
		return buf
	default:
	}
	return make([]byte, size)
}

// ReleaseBuffer returns byte slice to the pool.
// If pool is full, the slice is discarded.
func ReleaseBuffer(buf []byte) {
	select {
	case gByteSlicePools[gSlicesIndex[len(buf)]] <- buf:
	default:
	}
}
