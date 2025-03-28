#
# This script is used to check for duplicate functions in the function list and remove them from the csv file.
# It also creates a backup of the original csv file with the `.bkup` suffix.
# You can removed the backup file after you are sure that the script works as expected.
#

import argparse
import os
import wren_core

# Set up argument parsing
parser = argparse.ArgumentParser(description="Find the duplicate function in the function list")
parser.add_argument("path", help="Path to the csv file")

args = parser.parse_args()

ctx = wren_core.SessionContext()
functions = ctx.get_available_functions()
# extract the function names be a set
function_names = set()
for function in functions:
    function_names.add(function.name)
print("Default Function count: ", len(function_names))
print("Function is already in the function list:")
# read the csv file
collection = []
with open(args.path, "r") as file:  
    # rename the file with `.bkup` suffix
    backup_path = args.path + ".bkup"
    os.rename(args.path, backup_path)
    # create a new file with the same name
    with open(args.path, "w") as new_file:
        # if the function name is not in the function names, write it to the new file
        for line in file:
            if line.startswith("#"):
                new_file.write(line)
                continue
            function_name = line.split(",")[1]
            if function_name not in function_names:
                new_file.write(line)
                collection.append(function_name)
        print("valid remote function: ", ",".join(collection))
