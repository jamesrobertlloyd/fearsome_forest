'''
Created on 23 Nov 2012

@author: James Lloyd jrl44@cam.ac.uk
'''

#import pyfear
import cblparallel
import cblparallel.config
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
import sys

sys.path.append('/users/jrl44/python')

import pickle
from sklearn.ensemble import RandomForestRegressor
import numpy as np
import subprocess
from subprocess_timeout import timeoutCommand
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
print "Moving output file"
if not timeoutCommand(cmd=' '.join(['scp', '-i', '/users/jrl44/.ssh/jrl44fear2sagarmatha', '%(output_file)s', 'jrl44@sagarmatha:~/Documents/Research/RF/fearsome_forest/temp/%(output_file)s', ';', 'rm', '%(output_file)s'])).run(timeout=120)[0]:
    raise RuntimeError('Copying output raised error or timed out')
print 'Writing completion flag'
with open('%(flag_file)s', 'w') as f:
    f.write('Goodbye, world')
print "Goodbye, world"
quit()
'''

reduced_tree_code = '''
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
'''

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
                            'output_file' : '%(output_file)s',
                            'flag_file' : '%(flag_file)s'} for i in range(n)]
    # Submit to fear
    with pyfear.fear() as fear:
        fear.copy_to(data_file, os.path.join(remote_temp_path, os.path.split(data_file)[-1]))
        output_files = pyfear.run_python_jobs(scripts, local_temp_path, remote_temp_path, fear)
        fear.rm(os.path.join(remote_temp_path, os.path.split(data_file)[-1]))

    # Kill local data file
    os.remove(data_file)    

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
    
def rf_fear_test2(n=10,n_trees=10):
    # Data
    X, y = make_friedman1(n_samples=1200, random_state=0, noise=1.0)
    X_train, X_test = X[:200], X[200:]
    y_train, y_test = y[:200], y[200:]
    # Params
    #local_temp_path = os.path.abspath('../temp/')
    #remote_temp_path = 'python/'
    # Write data file locally
    #data_file = mkstemp_safe(cblparallel.config.LOCAL_TEMP_PATH, '.p')
    data_file = mkstemp_safe(cblparallel.config.HOME_TEMP_PATH, '.p')
    with open(data_file, 'w') as f:
        pickle.dump((X_train, y_train, X_test), f)
    # Prepare code
    scripts = [reduced_tree_code % {'data_file' : os.path.join(cblparallel.config.REMOTE_TEMP_PATH, os.path.split(data_file)[-1]),
                            'n_trees' : n_trees,
                            'random_state' : i * n_trees,
                            'output_file' : '%(output_file)s',
                            'flag_file' : '%(flag_file)s'} for i in range(n)]
    # Submit to fear
    with cblparallel.fear(via_gate=True) as fear:
        fear.copy_to(data_file, os.path.join(cblparallel.config.REMOTE_TEMP_PATH, os.path.split(data_file)[-1]))
        output_files = cblparallel.run_batch_on_fear(scripts, via_gate=True)
        fear.rm(os.path.join(cblparallel.config.REMOTE_TEMP_PATH, os.path.split(data_file)[-1]))

    # Kill local data file
    os.remove(data_file)    

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
    
def rf_fear_test_home(n=10,n_trees=10):
    cblparallel.start_port_forwarding()
    # Data
    X, y = make_friedman1(n_samples=1200, random_state=0, noise=1.0)
    X_train, X_test = X[:200], X[200:]
    y_train, y_test = y[:200], y[200:]
    # Params
    #local_temp_path = os.path.abspath('../temp/')
    #remote_temp_path = 'python/'
    # Write data file locally
    #data_file = mkstemp_safe(cblparallel.config.LOCAL_TEMP_PATH, '.p')
    data_file = mkstemp_safe(cblparallel.config.HOME_TEMP_PATH, '.p')
    with open(data_file, 'w') as f:
        pickle.dump((X_train, y_train, X_test), f)
    # Prepare code
    scripts = [reduced_tree_code % {'data_file' : os.path.join(cblparallel.config.REMOTE_TEMP_PATH, os.path.split(data_file)[-1]),
                            'n_trees' : n_trees,
                            'random_state' : i * n_trees,
                            'output_file' : '%(output_file)s',
                            'flag_file' : '%(flag_file)s'} for i in range(n)]
    # Submit to fear
    with cblparallel.fear(via_gate=True) as fear:
        fear.copy_to(data_file, os.path.join(cblparallel.config.REMOTE_TEMP_PATH, os.path.split(data_file)[-1]))
        output_files = cblparallel.run_batch_on_fear(scripts, max_jobs=1000)
        fear.rm(os.path.join(cblparallel.config.REMOTE_TEMP_PATH, os.path.split(data_file)[-1]))

    # Kill local data file
    os.remove(data_file)    

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
    
def local_forest_test(n=10,n_trees=10):
    # Data
    X, y = make_friedman1(n_samples=1200, random_state=0, noise=1.0)
    X_train, X_test = X[:200], X[200:]
    y_train, y_test = y[:200], y[200:]
    # Params
#    local_temp_path = os.path.abspath('../temp/')
#    remote_temp_path = 'python/'
    # Write data file locally
    data_file = mkstemp_safe(cblparallel.config.LOCAL_TEMP_PATH, '.p')
    with open(data_file, 'w') as f:
        pickle.dump((X_train, y_train, X_test), f)
    # Prepare code
    scripts = [reduced_tree_code % {'data_file' : data_file,
                            'n_trees' : n_trees,
                            'random_state' : i * n_trees,
                            'output_file' : '%(output_file)s',
                            'flag_file' : '%(flag_file)s'} for i in range(n)]
    # Run bacth in parallel)
    output_files = cblparallel.run_batch_locally(scripts)

    # Kill local data file
    os.remove(data_file)    

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
    
matlab_code='''
x = pi + randn
csvwrite('%(output_file)s', x)
'''
    
def local_matlab_test(n=10):
    # Prepare code
    scripts = [matlab_code] * n
    # Run bacth in parallel
    output_files = cblparallel.run_batch_locally(scripts, language='matlab')  
    # Now do something with the output
    estimators = []

    for output_file in output_files:
        with open(output_file, 'r') as f:
            estimator = np.genfromtxt(output_file, delimiter=',')
        os.remove(output_file)
        estimators.append(estimator)

    ens_pred = np.mean(estimators)
    return ens_pred
    
def remote_matlab_test(n=10):
    cblparallel.start_port_forwarding()
    # Prepare code
    scripts = [matlab_code] * n
    # Run bacth in parallel
    output_files = cblparallel.run_batch_on_fear(scripts, language='matlab', max_jobs=1000)  
    # Now do something with the output
    estimators = []

    for output_file in output_files:
        with open(output_file, 'r') as f:
            estimator = np.genfromtxt(output_file, delimiter=',')
        os.remove(output_file)
        estimators.append(estimator)

    ens_pred = np.mean(estimators)
    return ens_pred

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
