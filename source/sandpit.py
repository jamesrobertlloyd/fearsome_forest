'''
Created on 23 Nov 2012

@author: James Lloyd jrl44@cam.ac.uk
'''

import pyfear
import time
    
def main():
    with pyfear.fear() as fear:
        i = 1
        while True:
            fear.copy_from('./test.txt', './test.txt')
            print i
            i = i+1
            time.sleep(5)
            
    
if __name__ == '__main__':
    main()