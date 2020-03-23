all:	cpp mpi base
OS := $(shell uname)
ifeq ($(OS), Darwin)
	CFLAGS=  -O1
else
	CFLAGS= -O1
endif
cpp:
	g++ mr-pr-cpp.cpp -o mr-pr-cpp.o
mpi:
	g++ mr-pr-mpi.cpp -o mr-pr-mpi.o
base:
	g++ mr-pr-mpi-base.cpp -o mr-pr-mpi-base.o
clean: 
	rm -f *.o *.out