CC=gcc
CFLAGS=-O3 -lz
TARGETS=seekgzip
PYTHON_TARGETS=export_python.cpp seekgzip.py

all: $(TARGETS)

clean:
	rm $(TARGETS)

python: $(PYTHON_TARGETS)

clean-python:
	rm $(PYTHON_TARGETS)

seekgzip: seekgzip.c
	$(CC) $(CFLAGS) -o $@ -DBUILD_UTILITY $<

$(PYTHON_TARGETS): export.h export.i
	swig -c++ -python -o export_python.cpp export.i
