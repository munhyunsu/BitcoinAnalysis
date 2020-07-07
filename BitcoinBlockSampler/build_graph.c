#include <igraph.h>

int main(int argc, char* argv[]) {
    igraph_t graph;
    FILE *input;

    if(argc <= 1) {
        return 0;
    }
    input = fopen(argv[1], "r");
    if(!input) {
        return 1;
    }
    igraph_read_graph_edgelist(&graph, input, 0, 0);
    fclose(input);
    printf("Graph load complete with %i vertices and %i edges\n",
             igraph_vcount(&graph), igraph_ecount(&graph));

    igraph_vector_t membership, degree;
    igraph_vector_init(&membership, igraph_vcount(&graph));
    igraph_vector_init(&degree, igraph_vcount(&graph));
    igraph_degree(&graph, &degree, igraph_vss_all(), IGRAPH_ALL, 1);
    igraph_integer_t nb_clusters;
    igraph_real_t quality;
    printf("Initialize for community detection complete\n");

    igraph_community_leiden(&graph, 
                            NULL, &degree, 
                            1.0/(2*igraph_ecount(&graph)), 0.01, 0, 
                            &membership, &nb_clusters, &quality);

    printf("Leiden found %i clusters using modularity, quality is %.4f.\n",
             nb_clusters, quality);
    igraph_vector_print(&membership);
    printf("\n");

    igraph_vector_destroy(&degree);
    igraph_vector_destroy(&membership);
    igraph_destroy(&graph);

    return 0;
}
