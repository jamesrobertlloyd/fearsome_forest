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
from sklearn.ensemble import RandomForestRegressor
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble.base import BaseEnsemble
from sklearn.datasets import make_friedman1
import numpy as np
import os
import tempfile
import pickle

class EnsembleRegressor(BaseEnsemble):
    ''' Very basic ensembling of generic estimators'''
    def __init__(self, estimators):
        self.estimators_ = estimators

    def fit(self, X, y):
        for estimator in self.estimators_:
            estimator = estimator.fit(X, y)
        return self

    def predict(self, X):
        return np.mean([estimator.predict(X) for estimator in self.estimators_], axis=0)

def RMSE(X, y, predictor):
    return np.sqrt(np.mean(np.power(y - predictor.predict(X), 2)))

def RMSE_y(y, y_hat):
    return np.sqrt(np.mean(np.power(y - y_hat, 2)))

def rf_test():
    X, y = make_blobs(n_samples=10000, n_features=10, centers=100, random_state=0)

    
    clf = RandomForestClassifier(n_estimators=10, max_depth=None, min_samples_split=1, random_state=0)
    scores = cross_val_score(clf, X, y)
    print scores.mean()
    return clf

def rf_r_test(n=10):
    X, y = make_friedman1(n_samples=1200, random_state=0, noise=1.0)
    X_train, X_test = X[:200], X[200:]
    y_train, y_test = y[:200], y[200:]
    ens = EnsembleRegressor([RandomForestRegressor(n_estimators=1, max_depth=None, min_samples_split=1, random_state=i) for i in range(n)]).fit(X_train, y_train)
    return RMSE(X_test, y_test, ens)

tree_code = '''
import pickle
from sklearn.ensemble import RandomForestRegressor
import numpy as np
# Load data
print 'Loading data'
with open('%(data_file)s', 'r') as f:
    (X_train, y_train, X_test) = pickle.load(f)
print 'Loaded data'
# Train some trees
estimator = RandomForestRegressor(n_estimators=%(n_trees)d, max_depth=None, min_samples_split=1, random_state=%(random_state)d).fit(X_train, y_train)
print 'Trained trees'
# Predict
prediction = estimator.predict(X_test)
'Predicted'
# Output
np.savetxt('%(output_file)s', prediction, delimiter=',')
'Wrote output'
quit()
'''

# with open('%(output_file)s', 'w') as f:
#     pickle.dump((estimator, prediction), f)

def mkstemp_safe(directory, suffix):
    (os_file_handle, file_name) = tempfile.mkstemp(dir=directory, suffix=suffix)
    os.close(os_file_handle)
    return file_name

def rf_fear_test(n=10,n_trees=1000):
    # Data
    X, y = make_friedman1(n_samples=1200, random_state=0, noise=1.0)
    X_train, X_test = X[:200], X[200:]
    y_train, y_test = y[:200], y[200:]
    # Params
    local_temp_path = os.path.abspath('../temp/')
    remote_temp_path = 'python/'
    # Write data file locally
    data_file = mkstemp_safe(local_temp_path, '.p')
    with open(data_file, 'w') as f:
        pickle.dump((X_train, y_train, X_test), f)
    # Prepare code
    scripts = [tree_code % {'data_file' : os.path.split(data_file)[-1],
                            'n_trees' : n_trees,
                            'random_state' : i * n_trees,
                            'output_file' : '%(output_file)s'} for i in range(n)]
    # Submit to fear
    with pyfear.fear() as fear:
        fear.copy_to(data_file, os.path.join(remote_temp_path, os.path.split(data_file)[-1]))
        output_files = pyfear.run_python_jobs(scripts, local_temp_path, remote_temp_path, fear)
        fear.rm(os.path.join(remote_temp_path, os.path.split(data_file)[-1]))

    # Now do something with the output

    estimators = []
    predictions = []

    for output_file in output_files:
        with open(output_file, 'r') as f:
            #(estimator, prediction) = pickle.load(f)
            prediction = np.genfromtxt(output_file, delimiter=',')
        os.remove(output_file)
        #estimators.append(estimator)
        predictions.append(prediction)

    #ens = EnsembleRegressor(estimators)
    #return RMSE(X_test, y_test, ens)

    ens_pred = np.mean(predictions, axis=0)
    return RMSE_y(y_test, ens_pred)

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
