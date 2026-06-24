package query

import (
	"context"
	"iter"
	"sync"
)

type StreamResult struct {
	ID    uint64
	Score float32
}

func ExecuteStream(ctx context.Context, vecResults iter.Seq2[StreamResult, error], lftjResults iter.Seq2[StreamResult, error], limit int) iter.Seq2[StreamResult, error] {
	return func(yield func(StreamResult, error) bool) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		results := make(chan StreamResult, 100)
		errCh := make(chan error, 2)

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			for r, err := range vecResults {
				if err != nil {
					errCh <- err
					return
				}
				select {
				case results <- r:
				case <-ctx.Done():
					return
				}
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			for r, err := range lftjResults {
				if err != nil {
					errCh <- err
					return
				}
				select {
				case results <- r:
				case <-ctx.Done():
					return
				}
			}
		}()

		go func() {
			wg.Wait()
			close(results)
		}()

		count := 0
		for r := range results {
			if count >= limit {
				cancel()
				break
			}
			if !yield(r, nil) {
				cancel()
				return
			}
			count++
		}

		select {
		case err := <-errCh:
			yield(StreamResult{}, err)
		default:
		}
	}
}
