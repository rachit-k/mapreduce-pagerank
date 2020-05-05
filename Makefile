all:	seq mpr part2
mpr:
	g++ begin.cpp -I /usr/local/include -L /usr/local/Cellar/boost/1.72.0/lib -std=c++11 -lboost_filesystem -lboost_iostreams -o output
seq:
	g++ pagerank_seq.cpp -I /usr/local/include -L /usr/local/Cellar/boost/1.72.0/lib -std=c++11 -lboost_filesystem -lboost_iostreams -o seq
part2:
	mpic++ part2.cpp -o part2
clean:
	rm -f output seq part2