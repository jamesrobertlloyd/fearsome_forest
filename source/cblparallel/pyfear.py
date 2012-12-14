'''
A set of utilities to talk to the
fear computing cluster and perform
common tasks

@authors: James Robert Lloyd (jrl44@cam.ac.uk)
'''

import pysftp # Wraps up various paramiko calls
import credentials # Contains USERNAME and PASSWORD
from subprocess_timeout import timeoutCommand
import os
import re
import tempfile
import time

class fear(object):
    '''
    Manages communications with the fear computing cluster
    TODO - add error checking
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
        self._connection = pysftp.Connection('fear', private_key=credentials.PRIVATE_KEY_FILE)#username=credentials.USERNAME, password=credentials.PASSWORD)
        
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
        output = self.command('. /usr/local/grid/divf2/common/settings.sh; qdel -u %s' % credentials.USERNAME)
        return output
    
    def qstat(self):
        '''Updates a dictionary with (job id, status) pairs'''
        output = self.command('. /usr/local/grid/divf2/common/settings.sh; qstat -u %s' % credentials.USERNAME)
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

def mkstemp_safe(directory, suffix):
    (os_file_handle, file_name) = tempfile.mkstemp(dir=directory, suffix=suffix)
    os.close(os_file_handle)
    return file_name

def run_python_jobs(scripts, local_temp_path, remote_temp_path, my_fear, verbose=True, re_submit_wait=30):
    '''
    Receives a list of python code to run

    Assumes the code has an output file that will be managed by this function
    
    Returns a list of local file names where the code has presumably stored output
    '''
    if my_fear is None:
        fear = fear()
    else:
        fear = my_fear
    
    script_files = []
    shell_files = []
    output_files = []
    flag_files = []
    job_ids = []    

    # Create files and submit jobs

    for (i, code) in enumerate(scripts):
        # Create necessary files in local path
        script_files.append(mkstemp_safe(local_temp_path, '.py'))
        shell_files.append(mkstemp_safe(local_temp_path, '.sh'))
        output_files.append(mkstemp_safe(local_temp_path, '.dat'))
        flag_files.append(mkstemp_safe(local_temp_path, '.flag'))
        # Customise code
        code = code % {'output_file': os.path.split(output_files[i])[-1],
                       'flag_file' : os.path.split(flag_files[i])[-1]}
        # Write code and shell file
        with open(script_files[i], 'w') as f:
            f.write(code)
        with open(shell_files[i], 'w') as f:
            f.write('python ' + os.path.split(script_files[i])[-1] + '\n') # Could change this to an absolute path
        # Transfer files to fear
        fear.copy_to(script_files[i], os.path.join(remote_temp_path, os.path.split(script_files[i])[-1]))
        fear.copy_to(shell_files[i], os.path.join(remote_temp_path, os.path.split(shell_files[i])[-1]))
        # Submit the job to fear
        print 'Job %d of %d' % (i + 1, len(scripts))
        job_ids.append(fear.qsub(os.path.join(remote_temp_path, os.path.split(shell_files[i])[-1]), verbose=verbose))
        
    # Wait for and read in results

    fear_finished = False
    job_finished = [False] * len(output_files)
    results = [None] * len(output_files)

    while not fear_finished:
        for (i, flag_file) in enumerate(flag_files):
            if not job_finished[i]:
                # Update job status
                fear.qstat()
                if fear.job_terminated(job_ids[i]):
                    if not fear.file_exists(os.path.join(remote_temp_path, os.path.split(flag_files[i])[-1])):
                        # Job has finished but no output - re-submit
                        print 'Shell script %s job_id %s failed, re-submitting...' % (shell_files[i], job_ids[i])
                        job_ids[i] = fear.qsub(os.path.join(remote_temp_path, os.path.split(shell_files[i])[-1]), verbose=verbose)
                    else:
                        # Job has finished successfully
                        job_finished[i] = True
                        # Copy output file from fear - this can fail
                        #### TODO - package this up nicely
#                        file_copied = False
#                        attempts = 0
#                        while (not file_copied) and (attempts < 5):
#                            file_copied = fear.copy_from(os.path.join(remote_temp_path, os.path.split(output_files[i])[-1]), output_files[i])[0]
#                            attempts += 1
                        file_copied = True
                        if file_copied:
                            # Tidy up fear
                            fear.rm(os.path.join(remote_temp_path, os.path.split(script_files[i])[-1]))
                            fear.rm(os.path.join(remote_temp_path, os.path.split(shell_files[i])[-1]))
                            #fear.rm(os.path.join(remote_temp_path, os.path.split(output_files[i])[-1]))
                            fear.rm(os.path.join(remote_temp_path, os.path.split(flag_files[i])[-1]))
                            fear.rm(os.path.join(remote_temp_path, os.path.split(shell_files[i])[-1]) + '*') # Kills temporary output files
                            os.remove(script_files[i])
                            os.remove(shell_files[i])
                            os.remove(flag_files[i])
                            # Tell the world
                            if verbose:
                                print '%d / %d jobs complete' % (sum(job_finished), len(job_finished))
#                        else:
#                            # Cannot copy file for some reason - delete it and try again
#                            print 'Could not copy %s from job id %s, re-submitting...' % (os.path.join(remote_temp_path, os.path.split(output_files[i])[-1]), job_ids[i])
#                            job_finished[i] = False
#                            fear.rm(os.path.join(remote_temp_path, os.path.split(output_files[i])[-1]))
#                            job_ids[i] = fear.qsub(os.path.join(remote_temp_path, os.path.split(shell_files[i])[-1]), verbose=verbose)                            
                        
                elif not (fear.job_queued(job_ids[i]) or fear.job_running(job_ids[i]) \
                          or fear.job_loading(job_ids[i])):
                    # Job has some status other than running or queuing - something is wrong, delete and re-submit
                    fear.qdel(job_ids[i])
                    print 'Shell script %s job_id %s stuck, deleting and re-submitting...' % (shell_files[i], job_ids[i])
                    job_ids[i] = fear.qsub(os.path.join(remote_temp_path, os.path.split(shell_files[i])[-1]), verbose=verbose)
        
        if all(job_finished):
            fear_finished = True    
        if not fear_finished:
            # Count how many jobs are queued
            n_queued = len([1 for job_id in job_ids if fear.job_queued(job_id)])
            # Count how many jobs are running
            n_running = len([1 for job_id in job_ids if fear.job_running(job_id)])
            if verbose:
                print '%d jobs running' % n_running
                print '%d jobs queued' % n_queued
                print 'Sleeping for %d seconds' % re_submit_wait
                time.sleep(re_submit_wait)

    return output_files
