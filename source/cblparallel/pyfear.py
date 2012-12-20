'''
A set of utilities to talk to the
fear computing cluster and perform
common tasks

@authors: James Robert Lloyd (jrl44@cam.ac.uk)
'''

import pysftp # Wraps up various paramiko calls
import config # Contains USERNAME etc
from util import timeoutCommand
import os
import re
import tempfile
import time

class fear(object):
    '''
    Manages communications with the fear computing cluster
    TODO - add error checking / other niceties
    '''

    def __init__(self):
        '''
        Constructor - connects to fear
        '''
        self.connect()
        
    def __enter__(self):
        return self

    def __exit__(self, _type, value, traceback):
        self.disconnect
        
    def connect(self):
        '''
        Connect to fear and store connection object
        '''
        self._connection = pysftp.Connection('fear', private_key=config.PRIVATE_KEY_FILE)
        
    def disconnect(self):
        self._connection.close()

    def command(self, cmd):
        output =  self._connection.execute(cmd)
        return output
        
    def _put(self, localpath, remotepath):
        output = self._connection.put(localpath=localpath, remotepath=remotepath)
        return output
    
    def _get(self, remotepath, localpath):
        self._connection.get(remotepath=remotepath, localpath=localpath)
        
    def copy_to(self, localpath, remotepath, timeout=10, verbose=False):
        return timeoutCommand(cmd='python fear_put.py %s %s' % (localpath, remotepath), verbose=verbose).run(timeout=timeout) 
        
    def copy_from(self, remotepath, localpath, timeout=10, verbose=False):
        return timeoutCommand(cmd='python fear_get.py %s %s' % (remotepath, localpath), verbose=verbose).run(timeout=timeout)  
        
    def rm(self, remote_path):
        output = self.command('rm %s' % remote_path)
        return output
    
    def file_exists(self, remote_path):
        #### TODO - Replace this with an ls statement?
        response = self.command('if [ -e %s ] \nthen \necho ''exists'' \nfi' % remote_path)
        return response == ['exists\n']
    
    def qsub(self, shell_file, verbose=True):
        '''
        Submit a job onto the stack.
        Currently runs jobs from the same folder as they are saved in.
        '''
        fear_string = ' '.join(['. /usr/local/grid/divf2/common/settings.sh;',
                                'cd %s;' % os.path.split(shell_file)[0],
                                'chmod +x %s;' % os.path.split(shell_file)[-1],
                                'qsub -l lr=0',
                                os.path.split(shell_file)[-1] + ';',
                                'cd ..'])
    
        if verbose:
            print 'Submitting : %s' % fear_string
            
        output_text = self.command(fear_string)
        # Return the job id
        return output_text[0].split(' ')[2]
    
    def qdel(self, job_id):
        output = self.command('. /usr/local/grid/divf2/common/settings.sh; qdel %s' % job_id)
        return output
    
    def qdel_all(self):
        output = self.command('. /usr/local/grid/divf2/common/settings.sh; qdel -u %s' % config.USERNAME)
        return output
    
    def qstat(self):
        '''Updates a dictionary with (job id, status) pairs'''
        output = self.command('. /usr/local/grid/divf2/common/settings.sh; qstat -u %s' % config.USERNAME)
        # Now process this text to turn it into a list of job statuses
        # First remove multiple spaces from the interesting lines
        without_multi_space = [re.sub(' +',' ',line) for line in output[2:]]
        # Now create a dictionary of job ids and statuses
        self.status = {key: value for (key, value) in zip([line.split(' ')[0] for line in without_multi_space], \
                                                          [line.split(' ')[4] for line in without_multi_space])}
    
    def job_terminated(self, job_id, update=False):
        '''Returns true if job not listed by qstat'''
        if update:
            self.qstat()
        return not self.status.has_key(job_id)
    
    def job_running(self, job_id, update=False):
        if update:
            self.qstat()
        if self.status.has_key(job_id):
            return self.status[job_id] == 'r'
        else:
            return False
    
    def job_queued(self, job_id, update=False):
        if update:
            self.qstat()
        if self.status.has_key(job_id):
            return self.status[job_id] == 'qw'
        else:
            return False
    
    def job_loading(self, job_id, update=False):
        if update:
            self.qstat()
        if self.status.has_key(job_id):
            return self.status[job_id] == 't'
        else:
            return False
