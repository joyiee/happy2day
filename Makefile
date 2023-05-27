
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall

all: correctness persistence analysis

correctness: kvstore.o correctness.o BloomFilter.o SSTable.o SkipList.o

persistence: kvstore.o persistence.o BloomFilter.o SSTable.o SkipList.o

analysis: kvstore.o analysis.o BloomFilter.o SSTable.o SkipList.o

clean:
	-rm -f correctness  *.o
