# Targets

CC =		mpicc 
CPP =		mpic++ 
CCFLAGS =	-g -O -I../src -m64
LINK =		mpic++ -m64
LINKFLAGS =	-g -O -m64
USRLIB =	../src/libmrmpi_mac.a
SYSLIB =


all:	mr-pr-mpi-base

mr-pr-mpi-base:	mr-pr-mpi-base.o $(USRLIB)
	$(LINK) $(LINKFLAGS) mr-pr-mpi-base.o $(USRLIB) $(SYSLIB) -o mr-pr-mpi-base

clean:
	rm *.o mr-pr-mpi-base

# Rules

%.o:%.cpp
	$(CPP) $(CCFLAGS) -c $<

