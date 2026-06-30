//go:build !linux

package vector

func (seg *mmapSegment) sync() error {
	return nil
}
