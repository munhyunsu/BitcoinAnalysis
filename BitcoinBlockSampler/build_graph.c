#include <igraph.h>

int main(void) {
    igraph_t graph;
    FILE *input;

    input = fopen("edge.csv", "r");
    if(!input) {
        return 1;
    }
    igraph_read_graph_edgelist(&graph, input, 0, 0);
    fclose(input);

    igraph_vector_t membership, degree;
    igraph_integer_t nb_clusters;
    igraph_real_t quality;

    igraph_community_leiden(&graph, 
                            NULL, &degree, 
                            1.0/(2*igraph_ecount(&graph)), 0.01, 0, 
                            &membership, &nb_clusters, &quality);

    printf("Leiden found %i clusters using modularity, quality is %.4f.\n",
             nb_clusters, quality);

    igraph_vector_destroy(&degree);
    igraph_vector_destroy(&membership);
    igraph_destroy(&graph);

    return 0;
}
