SIFT_DIR := bench/datasets/data
SIFT_URL := http://corpus-texmex.irisa.fr

.PHONY: download-sift bench bench-sift bench-report bench-recall clean

download-sift:
	mkdir -p $(SIFT_DIR)
	cd $(SIFT_DIR) && \
	wget -q $(SIFT_URL)/sift.tar.gz -O sift.tar.gz && \
	tar -xzf sift.tar.gz --strip-components=1 && \
	rm -f sift.tar.gz
	@echo "SIFT-1M downloaded to $(SIFT_DIR)/"

bench:
	go test -bench=. -benchtime=100x -run='^$$' ./bench/...

bench-recall:
	go test -bench=BenchmarkRecall -benchtime=1x -run='^$$' ./bench/...

bench-sift:
	SIFT_DATA_DIR=$(SIFT_DIR) go test -run TestSIFT1M -v ./bench/...

bench-report:
	go run ./cmd/bench

clean:
	rm -rf bench/datasets/data