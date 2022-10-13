'''
'''

import argparse
import os

BASE = "/nrs/neuronbridge/ppp_imagery"

def create_commands():
    files = os.listdir(BASE)
    files = [f for f in files if os.path.isfile(BASE+'/'+f)] #Filtering only the files.
    print(*files, sep="\n")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    create_commands()