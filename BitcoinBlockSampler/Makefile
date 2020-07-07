CC = gcc
CFLAGS = -g -Wall
CFLAGS += `pkg-config --cflags /opt/igraph/lib/pkgconfig/igraph.pc`
LDFLAGS =
LDFLAGS += `pkg-config --libs /opt/igraph/lib/pkgconfig/igraph.pc`

TARGET = build_graph

$(TARGET): $(TARGET).c
	$(CC) $(TARGET).c $(CFLAGS) $(LDFLAGS) -o $(TARGET)

clean:
	$(RM) $(TARGET)
