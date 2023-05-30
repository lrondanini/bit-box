# bit-box


to build set GOMAXPROCS=128 (for badger optimizations) see https://groups.google.com/g/golang-nuts/c/jPb_h3TvlKE
//go build . && GOMAXPROCS=128 ./randread --dir ~/diskfio --jobs 16 --num 2000000 --mode 1
