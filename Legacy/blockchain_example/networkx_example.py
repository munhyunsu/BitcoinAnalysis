import matplotlib.pyplot as plt
import networkx as nx


def main():
    graph = nx.DiGraph()

    data = {('A', 'B'): 4,
            ('A', 'C'): 2,
            ('B', 'C'): 5,
            ('B', 'D'): 1,
            ('C', 'E'): 3,
            ('E', 'D'): 4,
            ('D', 'F'): 11}

    for key in data.keys():
        u, v = key
        weight = data[key]
        graph.add_edge(u, v, weight=weight)

    path = [('A', 'C'),
            ('C', 'E'),
            ('E', 'D'),
            ('D', 'F')]
    non_path = list(set(graph.edges) - set(path))

    #position = nx.spring_layout(graph)
    position = nx.circular_layout(graph)
    nx.draw_networkx_nodes(graph, position)
    nx.draw_networkx_labels(graph, position)
    nx.draw_networkx_edges(graph, position, edgelist=non_path, alpha=0.5)
    nx.draw_networkx_edges(graph, position, edgelist=path, alpha=0.8, 
                           edge_color='blue', width=2)
    nx.draw_networkx_edge_labels(graph, position, edge_labels=data)

    plt.title('Title example')
    plt.axis('off')
    plt.show()


if __name__ == '__main__':
    main()

