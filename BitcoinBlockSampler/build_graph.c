#include <time.h>

#include <igraph.h>

int main(int argc, char* argv[]) {
    igraph_t graph;
    FILE *input, *output;

    time_t stime, etime;
    time(&stime);
    if(argc <= 2) {
        return 0;
    }
    char *path = argv[1];
    input = fopen(path, "r");
    if(!input) {
        return 1;
    }
    igraph_read_graph_edgelist(&graph, input, 0, 0);
    fclose(input);
    time(&etime);
    printf("[%ld] Graph load complete with %li vertices and %li edges\n",
             (etime-stime), (long int) igraph_vcount(&graph),
             (long int) igraph_ecount(&graph));

    path = argv[2];
    output = fopen(path, "w");
    if(!output) {
        return 1;
    }
    igraph_write_graph_edgelist(&graph, output);
    fclose(output);

    return 0;
}
