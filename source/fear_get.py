'''
Short program that only gets one file
To be used as a subprocess to catch errors
'''

import sys
import pyfear

remotepath = sys.argv[1]
localpath = sys.argv[2]

with pyfear.fear() as fear:
    # This should be a non pysftp function!
    fear._connection.get(remotepath=remotepath, localpath=localpath)