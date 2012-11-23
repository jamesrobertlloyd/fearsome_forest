'''
A set of utilities to talk to the
fear computing cluster and perform
common tasks

@authors: James Robert Lloyd (jrl44@cam.ac.uk)
'''

import pysftp # Wraps up various paramiko calls
import credentials # Contains USERNAME and PASSWORD
from subprocess_timeout import timeoutCommand
# import re

class fear(object):
    '''
    Manages communications with the fear computing cluster
    '''

    def __init__(self):
        '''
        Constructor - connects to fear
        TODO - add error checking
        '''
        self.connect()
        
    def __enter__(self):
        '''
        For use with with statements
        '''
        return self

    def __exit__(self, type, value, traceback):
        '''
        For use with with statements
        '''
        self.disconnect
        
    def connect(self):
        '''
        Connect to fear and store connection object
        '''
        self._connection = pysftp.Connection('fear', username=credentials.USERNAME, password=credentials.PASSWORD)
        
    def disconnect(self):
        self._connection.close()

    def command(self, cmd):
        output =  self._connection.execute(cmd)
        return output
        
    def copy_to(self, local_path, remote_path):
        output =  self._connection.put(local_path, remote_path)
        return output
        
    def copy_from(self, remotepath, localpath, timeout=10, verbose=False):
#        output = self._connection.get(remote_path, local_path)
#        return output
        #### TODO - worry about paths?
        timeoutCommand(cmd='python fear_get.py %s %s' % (remotepath, localpath), verbose=verbose).run(timeout=timeout)  
        
    def rm(self, remote_path):
        output =  self.command('rm %s' % remote_path)
        return output
    
    def file_exists(self, remote_path):
        response = self.command('if [ -e %s ] \nthen \necho ''exists'' \nfi' % remote_path)
        return response == ['exists\n']
    
#    def qsub(shell_file, verbose=True, fear=None):
#        '''Submit a job onto the stack.'''
#        
#        #### WARNING - hardcoded path 'temp'
#        fear_string = ' '.join(['. /usr/local/grid/divf2/common/settings.sh;',
#                                'cd %s;' % credentials.REMOTE_TEMP_PATH,
#                                'chmod +x %s;' % shell_file.split('/')[-1],
#                                'qsub -l lr=0',
#                                shell_file.split('/')[-1] + ';',
#                                'cd ..'])
#    
#        if verbose:
#            print 'Submitting : %s' % fear_string
#        output_text = command(fear_string, fear)
#        # Return the job id
#        return output_text[0].split(' ')[2]
#    
#    def qdel(job_id, fear=None):
#        if not fear is None:
#            srv = fear
#        else:
#            srv = connect()
#        output = srv.execute('. /usr/local/grid/divf2/common/settings.sh; qdel %s' % job_id)
#        if fear is None:
#            srv.close()
#        return output
#    
#    def qdel_all(fear=None):
#        if not fear is None:
#            srv = fear
#        else:
#            srv = connect()
#        output = srv.execute('. /usr/local/grid/divf2/common/settings.sh; qdel -u %s' % credentials.USERNAME)
#        if fear is None:
#            srv.close()
#        return output
#    
#    def qstat_status(fear=None):
#        '''Returns a dictionary with (job id, status) pairs'''
#        if not fear is None:
#            srv = fear
#        else:
#            srv = connect()
#        test_output = srv.execute('. /usr/local/grid/divf2/common/settings.sh; qstat -u %s' % credentials.USERNAME)
#        # Now process this text to turn it into a list of job statuses
#        # First remove multiple spaces from the interesting lines
#        without_multi_space = [re.sub(' +',' ',line) for line in test_output[2:]]
#        # Now create a dictionary of job ids and statuses
#        status = {key: value for (key, value) in zip([line.split(' ')[0] for line in without_multi_space], \
#                                                     [line.split(' ')[4] for line in without_multi_space])}
#        if fear is None:
#            srv.close()
#        return status 
#    
#    def job_terminated(job_id, status=None, fear=None):
#        '''Returns true if job not listed by qstat'''
#        if status is None:
#            status = qstat_status(fear)
#        return not status.has_key(job_id)
#    
#    def job_running(job_id, status=None, fear=None):
#        if status is None:
#            status = qstat_status(fear)
#        if status.has_key(job_id):
#            return status[job_id] == 'r'
#        else:
#            return False
#    
#    def job_queued(job_id, status=None, fear=None):
#        if status is None:
#            status = qstat_status(fear)
#        if status.has_key(job_id):
#            return status[job_id] == 'qw'
#        else:
#            return False
#    
#    def job_loading(job_id, status=None, fear=None):
#        if status is None:
#            status = qstat_status(fear)
#        if status.has_key(job_id):
#            return status[job_id] == 't'
#        else:
#            return False


        