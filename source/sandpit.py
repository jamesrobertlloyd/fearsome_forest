'''
Created on 23 Nov 2012

@author: James Lloyd jrl44@cam.ac.uk
'''

import pyfear
import time
from sklearn.cross_validation import cross_val_score
from sklearn.datasets import make_blobs
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.tree import DecisionTreeClassifier

def rf_test():
    X, y = make_blobs(n_samples=10000, n_features=10, centers=100, random_state=0)

    
    clf = RandomForestClassifier(n_estimators=10, max_depth=None, min_samples_split=1, random_state=0)
    scores = cross_val_score(clf, X, y)
    print scores.mean()
    return clf

def copy_test():
    with pyfear.fear() as fear:
        i = 1
        while True:
            fear.copy_from('./test.txt', './test.txt')
            print i
            i = i+1
            time.sleep(5)
    
def main():
    rf_test()
            
    
if __name__ == '__main__':
    main()