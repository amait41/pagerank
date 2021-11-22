#!/usr/bin/env python3
import os
import json

if not os.path.isfile('results.txt'):
    os.system("./PageRank.py 'soc-Epinions1.txt' > results.txt")
    
if not os.path.isfile('results.json'):
    with open("results.txt", "r") as filein:
        with open("results.json", "w") as fileout:
            fileout.write("[")
            for line in filein:
                fileout.write("{" + line.strip().replace("\t", ":") + "},\n")
            fileout.write('{"-1" : []} \n ]')
    
with open("results.json", "r") as f:
    file = json.load(f)
    print(file)