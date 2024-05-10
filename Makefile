# Compiler and flags
CXX = g++
CXXFLAGS = -std=c++17 -Wall

# Targets
all: clean mapreduce

# Object files
OBJS = mapreduce.o master.o worker.o

# Compile the main executable
mapreduce: $(OBJS)
	$(CXX) $(CXXFLAGS) -o mapreduce $(OBJS)

# Compile the main entry point
mapreduce.o: mapreduce.cpp config.h master.h worker.h
	$(CXX) $(CXXFLAGS) -c mapreduce.cpp

# Compile master node functionalities
master.o: master.cpp master.h config.h
	$(CXX) $(CXXFLAGS) -c master.cpp

# Compile worker node functionalities
worker.o: worker.cpp worker.h config.h
	$(CXX) $(CXXFLAGS) -c worker.cpp

# Clean up
clean:
	rm -f *.o mapreduce
	rm -rf outputdir/*
	rm -rf output/*
	rm -rf mapreduce.log

# Phony targets for clean and all to avoid conflicts with files of the same name
.PHONY: clean all

tarball: 
	tar cf - `ls -a | grep -v '^\..*' | grep -v '^proj[0-9].*\.tar\.gz'` | gzip > proj3-sw2298-my528.tar.gz