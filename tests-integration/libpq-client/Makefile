CC = clang
CFLAGS = -Wall -O2
LIBS = -lpq

TARGET = client
SOURCES = client.c

all: $(TARGET)

$(TARGET): $(SOURCES)
	$(CC) $(CFLAGS) -o $(TARGET) $(SOURCES) $(LIBS)

clean:
	rm -f $(TARGET)

.PHONY: all clean
