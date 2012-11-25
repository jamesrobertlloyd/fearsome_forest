'''
Created on 23 Nov 2012

@author: jrl44
'''

import subprocess, threading

#### To think about: Could I just run a timeout function in a separate thread?

class timeoutCommand(object):
    def __init__(self, cmd, verbose=False):
        self.cmd = cmd
        self.verbose = verbose
        self.process = None

    def run(self, timeout):
        def target():
            if self.verbose:
                print 'Thread started'
            self.process = subprocess.Popen(self.cmd, shell=True)
            self.process.communicate()
            if self.verbose:
                print 'Thread finished'

        thread = threading.Thread(target=target)
        thread.start()

        thread.join(timeout)
        
        if thread.is_alive():
            if self.verbose:
                print 'Terminating process'
            self.process.terminate()
            thread.join()
            if self.verbose:    
                print self.process.returncode
            return (False, self.process.returncode)
        else:
            if self.verbose:    
                print self.process.returncode
            return (True, self.process.returncode)