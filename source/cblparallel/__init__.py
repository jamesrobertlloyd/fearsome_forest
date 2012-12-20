"""
A module to make it much easier to send code to the CBL computing cluster.

Contributions encouraged

@authors:
James Robert Lloyd (jrl44@cam.ac.uk)

"""

import pyfear
from pyfear import fear
from util import mkstemp_safe

from config import *

def setup():
    '''
    Run an interactive script to setup various prelminaries e.g.
     - RSA key pairs
     - Fear .profile including script that makes qsub, qstat etc available
     - Local directory on fear with python scripts
     - Local directory on machine where temporary files are stored
     - Local directory on fear where temporary files are stored
    '''
    pass

def run_batch_on_fear(scripts, language='python', job_check_sleep=30, file_copy_timeout=120, verbose=True):
    '''
    Receives a list of python scripts to run

    Assumes the code has an output file that will be managed by this function
    
    Returns a list of local file names where the code has presumably stored output
    '''
    # Define some code constants
    #### TODO - this path adding code should accept an optional list of paths
    python_path_code = '''
import sys
sys.path.append('%s')
''' % REMOTE_PYTHON_PATH

    #### This will be deprecated in future MATLAB - hopefully the -singleCompThread command is sufficient
    matlab_single_thread = '''
maxNumCompThreads(1);
'''

    matlab_path_code = '''
addpath(genpath('%s'))
''' % REMOTE_MATLAB_PATH
    
    python_transfer_code = '''
from util import timeoutCommand
print "Moving output file"
if not timeoutCommand(cmd='scp -i %(rsa_key)s %(output_file)s %(username)s@%(local_host)s:%(local_temp_path)s; rm %(output_file)s').run(timeout=%(timeout)d)[0]:
    raise RuntimeError('Copying output raised error or timed out')
''' % {'rsa_key' : REMOTE_TO_LOCAL_KEY_FILE,
       'output_file' : '%(output_file)s',
       'username' : USERNAME,
       'local_host' : LOCAL_HOST,
       'local_temp_path' : LOCAL_TEMP_PATH,
       'timeout' : file_copy_timeout}
    
    #### TODO - does this suffer from the instabilities that lead to the verbosity of the python command above
    matlab_transfer_code = '''
system('scp -i %(rsa_key)s %(output_file)s %(username)s@%(local_host)s:%(local_temp_path)s; rm %(output_file)s')
''' % {'rsa_key' : REMOTE_TO_LOCAL_KEY_FILE,
       'output_file' : '%(output_file)s',
       'username' : USERNAME,
       'local_host' : LOCAL_HOST,
       'local_temp_path' : LOCAL_TEMP_PATH}
       
    python_completion_code = '''
print 'Writing completion flag'
with open('%(flag_file)s', 'w') as f:
    f.write('Goodbye, World')
print "Goodbye, World"
quit()
'''
  
    #### TODO - Is this completely stable       
    matlab_completion_code = '''
fprintf('\nWriting completion flag\n');
ID = fopen('%(flag_file)s', 'w');
fprintf(ID, 'Goodbye, world');
fclose(ID);
fprintf('\nGoodbye, World\n');
quit()
'''
    
    # Open a connection to fear
    fear = pyfear.fear()
    
    # Initialise lists of file locations job ids
    script_files = []
    shell_files = []
    output_files = []
    flag_files = []
    job_ids = []    

    # Create files and submit jobs
    for (i, code) in enumerate(scripts):
        # Create necessary files in local path (avoids collisions)
        if language == 'python':
            script_files.append(mkstemp_safe(LOCAL_TEMP_PATH, '.py'))
        elif language == 'matlab':
            script_files.append(mkstemp_safe(LOCAL_TEMP_PATH, '.mat'))
        shell_files.append(mkstemp_safe(LOCAL_TEMP_PATH, '.sh'))
        output_files.append(mkstemp_safe(LOCAL_TEMP_PATH, '.out'))
        flag_files.append(mkstemp_safe(LOCAL_TEMP_PATH, '.flag'))
        # Customise code
        #### TODO - make path and output_transfer optional
        if language == 'python':
            code = python_path_code + code + python_transfer_code + python_completion_code
        elif language == 'matlab':
            code = matlab_single_thread + matlab_path_code + code + matlab_transfer_code + matlab_completion_code
        code = code % {'output_file': os.path.join(REMOTE_TEMP_PATH, os.path.split(output_files[i])[-1]),
                       'flag_file' : os.path.join(REMOTE_TEMP_PATH, os.path.split(flag_files[i])[-1])}
        # Write code and shell file
        with open(script_files[i], 'w') as f:
            f.write(code)
        with open(shell_files[i], 'w') as f:
            #### TODO - is os.path.join always correct - what happens if this program is being run on windows?
            if language == 'python':
                f.write('python ' + os.path.join(REMOTE_TEMP_PATH, os.path.split(script_files[i])[-1]) + '\n')
            elif language == 'matlab':
                f.write('/usr/local/apps/matlab/matlabR2011b/bin/matlab -nosplash -nojvm -nodisplay -singleCompThread -r ' + \
                        os.path.join(REMOTE_TEMP_PATH, os.path.split(script_files[i])[-1].split('.')[0]) + '\n')
        # Transfer files to fear
        fear.copy_to(script_files[i], os.path.join(REMOTE_TEMP_PATH, os.path.split(script_files[i])[-1]))
        fear.copy_to(shell_files[i], os.path.join(REMOTE_TEMP_PATH, os.path.split(shell_files[i])[-1]))
        # Submit the job to fear
        print 'Submitting job %d of %d' % (i + 1, len(scripts))
        job_ids.append(fear.qsub(os.path.join(REMOTE_TEMP_PATH, os.path.split(shell_files[i])[-1]), verbose=verbose))
        
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
                    if not fear.file_exists(os.path.join(REMOTE_TEMP_PATH, os.path.split(flag_files[i])[-1])):
                        # Job has finished but no output - re-submit
                        #### TODO - Record the output and error file - and potentially remove them
                        print 'Shell script %s job_id %s failed, re-submitting...' % (os.path.split(shell_files[i])[-1], job_ids[i])
                        job_ids[i] = fear.qsub(os.path.join(REMOTE_TEMP_PATH, os.path.split(shell_files[i])[-1]), verbose=verbose)
                    else:
                        # Job has finished successfully
                        job_finished[i] = True
                        # Tidy up fear
                        fear.rm(os.path.join(REMOTE_TEMP_PATH, os.path.split(script_files[i])[-1]))
                        fear.rm(os.path.join(REMOTE_TEMP_PATH, os.path.split(shell_files[i])[-1]))
                        fear.rm(os.path.join(REMOTE_TEMP_PATH, os.path.split(flag_files[i])[-1]))
                        #### TODO - record the output and error files for future reference
                        fear.rm(os.path.join(REMOTE_TEMP_PATH, os.path.split(shell_files[i])[-1]) + '*') # Kills temporary output files
                        # Tidy up local temporary directory
                        os.remove(script_files[i])
                        os.remove(shell_files[i])
                        os.remove(flag_files[i])
                        # Tell the world
                        if verbose:
                            print '%d / %d jobs complete' % (sum(job_finished), len(job_finished))                          
                        
                elif not (fear.job_queued(job_ids[i]) or fear.job_running(job_ids[i]) \
                          or fear.job_loading(job_ids[i])):
                    # Job has some status other than running or queuing - something is wrong, delete and re-submit
                    #### TODO - Record the output and error file - and potentially remove them
                    fear.qdel(job_ids[i])
                    print 'Shell script %s job_id %s stuck, deleting and re-submitting...' % (os.path.split(shell_files[i])[-1], job_ids[i])
                    job_ids[i] = fear.qsub(os.path.join(REMOTE_TEMP_PATH, os.path.split(shell_files[i])[-1]), verbose=verbose)
        
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
                print 'Sleeping for %d seconds' % job_check_sleep
                time.sleep(job_check_sleep)

    #### TODO - return job output and error files as applicable (e.g. there may be multiple error files associated with one script)
    return output_files


