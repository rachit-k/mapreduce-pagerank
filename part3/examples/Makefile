# Targets

CC =		mpicc 
CPP =		mpic++ 
CCFLAGS =	-g -O -I../src -m64
LINK =		mpic++ -m64
LINKFLAGS =	-g -O -m64
USRLIB =	../src/libmrmpi_mac.a
SYSLIB =


all:	pagerank

pagerank:	pagerank.o $(USRLIB)
	$(LINK) $(LINKFLAGS) pagerank.o $(USRLIB) $(SYSLIB) -o pagerank

clean:
	rm *.o pagerank

# Rules

%.o:%.cpp
	$(CPP) $(CCFLAGS) -c $<

