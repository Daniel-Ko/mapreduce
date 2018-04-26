from multiprocessing import TimeoutError
from concurrent import futures
from itertools import repeat
import argparse
from os import path


def get_key_val_from_str(key_ind, val_ind, line):
    """ Use to get the tokens you need from a line of data """
    tokens = line.split()
    return (tokens[key_ind], tokens[val_ind])

def parse_and_map(fname, key_ind, val_ind, num_processes, maps_per_node):
    """Processes the data file concurrently

        Args: 
            fname (String): filename
            key_ind (int): use to determine the column in the data to be the key
            val_ind (int): use to determine the column in the data to be mapped as value
            num_processes: the amount of processes that should run concurrently to map the data
            maps_per_node: number of maps total each process should do

        Returns:
            data (list): tuples containing key data entry and value data entry
    """
    if num_processes <= 0:
        num_processes = None # If file size is too small, just use max processors on machine

    data = []

    # Create repeating iterators for continuous parameters to the map function
    key_repeater = repeat(key_ind)
    val_repeater = repeat(val_ind)

    with open(fname, "r") as f:   
        try:
            with futures.ProcessPoolExecutor(int(num_processes)) as executor:
                mapper_gen = executor.map(get_key_val_from_str, key_repeater, val_repeater, f, timeout=1, chunksize=maps_per_node)
                for pair in mapper_gen:
                    data.append(pair)
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
    parser.add_argument('val_ind', type=int, help="column num from data you want to be mapped as the value")
    parser.add_argument('size_of_node', type=float, help="Block size of a single node in MB")
    parser.add_argument('maps_per_node', nargs='?', type=int, help="Maps generated per node?", default=10)
    args = parser.parse_args()

    num_processes = calculate_num_processes_needed(args.fname, args.size_of_node, args.maps_per_node)
    data = parse_and_map(args.fname, args.key_ind, args.val_ind, num_processes, args.maps_per_node)
    
    for pair in data:
        print(pair)