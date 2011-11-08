CC=gcc
CFLAGS=-O3 -lz
TARGET=seekgzip

all: $(TARGET)

clean:
	rm $(TARGET)

seekgzip: seekgzip.c
	$(CC) $(CFLAGS) -o $@ -DBUILD_UTILITY $<
