all:	mr-pr-cpp seq mr-pr-mpi
mr-pr-cpp:
	g++ mr-pr-cpp.cpp -I /usr/local/include -L /usr/local/Cellar/boost/1.72.0/lib -std=c++11 -lboost_filesystem -lboost_iostreams -o mr-pr-cpp.o
seq:
	g++ pagerank_seq.cpp -I /usr/local/include -L /usr/local/Cellar/boost/1.72.0/lib -std=c++11 -lboost_filesystem -lboost_iostreams -o seq
mr-pr-mpi:
	mpic++ mr-pr-mpi.cpp -o mr-pr-mpi
clean:
	rm -f mr-pr-cpp.o seq mr-pr-mpi