#include <time.h>

#include <igraph.h>

int main(int argc, char* argv[]) {
    igraph_t graph;
    FILE *input;

    time_t stime, etime;
    time(&stime);
    if(argc <= 1) {
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

    time(&stime);
    igraph_vector_t modularity, membership, edges;
    igraph_matrix_t memberships;
    igraph_vector_init(&modularity, 0);
    igraph_vector_init(&membership, 0);
    igraph_matrix_init(&memberships, 0, 0);
    time(&etime);
    printf("[%ld] Initialize for community detection complete\n",
              (etime-stime));

    time(&stime);
    igraph_community_multilevel(&graph, 0, 
                                &membership, &memberships, &modularity);
    time(&etime);
    printf("[%ld] Multilevel done", (etime-stime));

    time(&stime);
    printf("Membership exporting...\n");
    int n = igraph_vector_size(&membership);
    FILE *fptr = fopen("membership_multilevel.txt", "w");
    for(int i = 0; i < n; i++) {
        fprintf(fptr, "%.0f\n", VECTOR(membership)[i]);
    }
    time(&etime);
    printf("[%ld] Membership export complete\n", (etime-stime));

    igraph_vector_destroy(&modularity);
    igraph_vector_destroy(&membership);
    igraph_vector_destroy(&edges);
    igraph_matrix_destroy(&memberships);
    igraph_destroy(&graph);

    return 0;
}
