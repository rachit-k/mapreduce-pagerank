all:
	g++ begin.cpp -I /usr/local/include -L /usr/local/Cellar/boost/1.72.0/lib -std=c++11 -lboost_filesystem -lboost_iostreams -o output
clean:
	rm -f output