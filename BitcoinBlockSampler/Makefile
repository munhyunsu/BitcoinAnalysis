CC = gcc
CFLAGS = -g -Wall
CFLAGS += `pkg-config --cflags /opt/igraph/lib/pkgconfig/igraph.pc`
LDFLAGS =
LDFLAGS += `pkg-config --libs /opt/igraph/lib/pkgconfig/igraph.pc`

OBJS = build_graph \
	leiden

all: $(OBJS)

$(OBJS): %: %.c
	$(CC) $< $(CFLAGS) $(LDFLAGS) -o $@

clean:
	$(RM) $(OBJS)
