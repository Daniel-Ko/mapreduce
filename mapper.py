from multiprocessing import TimeoutError
from concurrent import futures
from itertools import repeat
import argparse
from os import path
import csv

def get_key_val_from_str(key_ind, tokens):
    """ Use to get the tokens you need from a line of data """
    return tokens[key_ind]

def parse_and_map(fname, key_ind, num_processes, maps_per_node):
    """Processes the data file concurrently

        Args: 
            fname (String): filename
            key_ind (int): use to determine the column in the data to be the key
            num_processes: the amount of processes that should run concurrently to map the data
            maps_per_node: number of maps total each process should do

        Returns:
            data (dict): tuples containing key data entry mapped to the frequency of it appearing in the datafile
    """
    if num_processes <= 0:
        num_processes = None # If file size is too small, just use max processors on machine

    data = {}

    # Create repeating iterators for continuous parameters to the map function
    key_repeater = repeat(key_ind)

    with open(fname, "r") as csvf:
        reader = csv.reader(csvf)   
        try:
            with futures.ProcessPoolExecutor(int(num_processes)) as executor:
                mapper_gen = executor.map(get_key_val_from_str, key_repeater, reader, timeout=1, chunksize=maps_per_node)
                
                for key in mapper_gen:
                    if key in data:
                        data[key] += 1
                    else:
                        data[key] = 0
        except TimeoutError:
            print("Took too long")
            pass
    return data

def calculate_num_processes_needed(fname, size_of_node, maps_per_node):
    bytes_filesize = path.getsize(fname)
    mb_filesize = (bytes_filesize / 1024.0) / 1024.0
    return -(-(mb_filesize / size_of_node) // maps_per_node) # both sizes are in MB


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='File and data indices to map')
    parser.add_argument('fname', help="Need a filename to read from")
    parser.add_argument('key_ind', type=int, help="column num from data you want to be the key")
    # parser.add_argument('val_ind', type=int, help="column num from data you want to be mapped as the value")
    parser.add_argument('size_of_node', type=float, help="Block size of a single node in MB")
    parser.add_argument('maps_per_node', nargs='?', type=int, help="Maps generated per node?", default=10)
    args = parser.parse_args()

    num_processes = calculate_num_processes_needed(args.fname, args.size_of_node, args.maps_per_node)
    data = parse_and_map(args.fname, args.key_ind, num_processes, args.maps_per_node)
    
    for key, count in data.items():
        print("{}, {}".format(key, count))