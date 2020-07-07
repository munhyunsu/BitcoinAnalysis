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
    input = fopen(argv[1], "r");
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
    igraph_vector_t membership, degree;
    igraph_vector_init(&membership, igraph_vcount(&graph));
    igraph_vector_init(&degree, igraph_vcount(&graph));
    igraph_degree(&graph, &degree, igraph_vss_all(), IGRAPH_ALL, 1);
    igraph_integer_t nb_clusters;
    igraph_real_t quality;
    time(&etime);
    printf("[%ld] Initialize for community detection complete\n",
              (etime-stime));

    time(&stime);
    igraph_community_leiden(&graph, 
                            NULL, &degree, 
                            1.0/(2*((long int)igraph_ecount(&graph))), 0.01, 0,
                            &membership, &nb_clusters, &quality);
    time(&etime);
    printf("[%ld] Leiden found %i clusters using modularity, quality is %.4f.\n",
             (etime-stime), nb_clusters, quality);

    time(&stime);
    printf("Membership exporting...\n");
    int n = igraph_vector_size(&membership);
    FILE *fptr = fopen("membership.txt", "w");
    for(int i = 0; i < n; i++) {
        fprintf(fptr, "%.0f\n", VECTOR(membership)[i]);
    }
    time(&etime);
    printf("[%ld] Membership export complete\n", (etime-stime));

    igraph_vector_destroy(&degree);
    igraph_vector_destroy(&membership);
    igraph_destroy(&graph);

    return 0;
}
