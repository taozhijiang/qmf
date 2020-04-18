IDLS=$(wildcard *.proto)
OBJS=$(IDLS:%.proto=%.pb.cc)

all: ${OBJS}
	@echo "remove *.skeleton.cpp files ..."
	@rm -fr *.skeleton.cpp

%.pb.cc: %.proto
	../../../../macOS/bin/protoc -I. --cpp_out=. $^
