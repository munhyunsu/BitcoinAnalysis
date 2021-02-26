import csv

with open('data/named.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        print((f'python3 main_build_graph_by_address.py --debug'
               f' --index dbv3-index.db'
               f' --core dbv3-core.db'
               f' --util dbv3-util.db'
               f' --input {row["RootAddress"]}'
               f' --output \'graphcsv/{row["ClusterName"]}.csv\''))
