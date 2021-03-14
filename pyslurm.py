import os, subprocess, re
from io import StringIO 
from datetime import datetime

import pandas as pd

class Slurm:
    '''
    Calling some slurm commands from python and a scrapper for some slurm outputs. 
    Only tested in umich great-lakes.
    '''
    def __init__(self, user = 'enes', path = 'default', account = 'stats_dept1'):
        self.user = user
        if path == 'default':
            self.path = os.path.join('/scratch/stats_dept_root/', account, user)       
        else:
            self.path = path
        self.account = account
        
    def queue(self, name = None):
        '''
        Returns your job queue as a pandas dataframe, if <name> is given that it will only returns
        the jobs with the tame <name>
        '''
        user = self.user
        df = pd.read_csv(StringIO(subprocess.check_output('squeue -u '+ user,
                                                          stderr=subprocess.STDOUT,
                                                          shell=True,
                                                          encoding = 'utf-8')), sep = '\s+')
        if name is not None:
            df = df[df['NAME'] == name]
        
        return df
    
    def jobids(self):
        '''
        Returns list of jobids
        '''
        queue = self.queue()
        return queue['JOBID'].to_list()
    
    def jobids_by_name(self):
        '''
        Returns a dictionary of jobids by job names: Dictionary[jobname] = [list of jobids]
        '''
        queue = self.queue()
        names = queue['NAME'].unique()
        jobs = {}
        for name in names:
            jobs[name] = queue[queue['NAME'] == name]['JOBID'].to_list()
        return jobs
        
    def jobids_by_time(self, mins, above = True):
        '''
        Returns a list of jobids if its runtime greater then mins (reverse if above = False). 
        Ex1: Slurm.jobids_by_time(15) <- Jobs whose runtime is greater then 15 mins
        Ex2: Slurm.jobids_by_time(16, False) <- Jobs whose runtime is less then 16 mins
        '''
        queue = self.queue() 
        jobs = queue['JOBID'].to_list()          
        
        def calc_min(x):
            smh = [1/60, 1, 60]
            x = x.split(":")
            x.reverse()
            t = 0
            for i in range(len(x)):
                t += smh[i]*int(x[i])
            return t
        
        t = queue['TIME']
        t = [calc_min(x) for x in t]
        
        if above:
            tjobs = [jobs[i] for i in range(len(jobs)) if t[i] >= mins]
        else:
            tjobs = [jobs[i] for i in range(len(jobs)) if t[i] <= mins]
        
        return tjobs    
    
    def cancel(self, jobids):
        '''
        Cancels jobs by jobids
        Ex: Slurm.cancel([4815, 4816, 2342])
        '''
        if not isinstance(jobids, list):
            jobids = [jobids]  
        job = 'scancel ' + ' '.join([str(i) for i in jobids])
        subprocess.check_output(job,
                                stderr=subprocess.STDOUT,
                                shell=True,
                                encoding = 'utf-8')
        print('Done!')
        return None
    
    def cancel_all(self):
        '''
        Cancels all of your jobs in the queue
        '''
        i = input('You will cancel all of your jobs, continue?')
        if i in ['y', 'Y']:
            return self.cancel(self.jobids())
        else:
            return None
    
    def cancel_by_name(self, name):
        '''
        Cancels all the jobs whose jobname == name
        Ex: Slurm.cancel('mysimulations')
        '''
        return self.cancel(self.jobids_by_name()[name])
    
    def cancel_by_time(self, mins, above = True):
        '''
        Cancels the jobs which their runtime are greater then mins (reverse if above = False). 
        Ex1: Slurm.cancel_by_time(15) <- Jobs whose runtime is greater then 15 mins
        Ex2: Slurm.cancel_by_time(16, False) <- Jobs whose runtime is less then 16 mins
        '''
        return self.cancel(self.jobids_by_time(mins, above))   
    
    def batch(self, *args, sbat_file = 'default'):      
        '''
        Write the ettings and add the modules fo sending a job to the HPC. 
        Ex: Slurm.batch('#job-name="mysimulation"', '#mem-per-cpu=1000', 'module load anaconda', 'source activate myenv')
        Parameters
        ----------
        sbat_file : str, optional
            If you want to save your sbat file somewhere else.
        *args : strings
            User can enter 2 different type of commands. Scrapper look at the
            first chrachter if it is '#' then it is a batch command otherwise
            it is an os command
            (1) : If you wish to enter SBATCH command, shorten them as:
                '#<slurmvar>=<itsvalue>'
                Ex: '#job-name="mysimulation"'
                    '#dependency=afterok:4815162342'
            (2) : You can simply add your other commands, such as loading modules:
                Ex: 'module load anaconda'
                    'source activate myenv'
                    
        Returns
        -------
        Function to run a batch job
        '''
        account = self.account        
        path = self.path
        
        if sbat_file == 'default':
            sbat_file = os.path.join(path, 'tmp.sbat')
        
        logpath = os.path.join(path, '%x-%j.log')
            
        defaults=['output='+logpath,
                  'time=0-2:0:00',
                  'mem-per-cpu=1000',
                  'cpus-per-task=1',
                  'account="'+account+'"',
                  'job-name="myjob"',
                  'ntasks=1']
                
        L = ['#!/bin/bash',
             '#SBATCH -p standard']
        
        Settings = {} # The default sbatch settings
        for opt in defaults:
            k, v = opt.split('=')
            Settings[k] = v

        JandC = [] # Job and Commands
        for arg in args:
            if arg[0] == '#':
                k, v = arg[1:].split('=')
                Settings[k] = v # Override the default settings or add new ones
            else:
                JandC.append(arg)

        for setting in Settings:
            L.append('#SBATCH --'+setting+'='+Settings[setting])
        L = L + JandC
        
        class Job:
            def __init__(self):
                self.lines = L
                self.sbat_file = sbat_file
            def run(self, job):
                L = self.lines.copy()
                sbat_file = self.sbat_file
                L.append(job)
                
                with open(sbat_file, 'w') as filehandle:
                    filehandle.writelines("%s\n" %line for line in L)

                jobname = subprocess.check_output('sbatch ' + sbat_file,
                                                  stderr=subprocess.STDOUT,
                                                  shell=True,
                                                  encoding = 'utf-8')
        
                return int(re.findall('\d+', jobname)[0])
        
        return Job()

    def my_job_stats(self, jobid, raw = False):
        '''
        Returns the job statistics as a dictionary
        '''
        mjs = subprocess.check_output('my_job_statistics ' + str(jobid),
                              stderr=subprocess.STDOUT,
                              shell=True,
                              encoding = 'utf-8')
    
        if raw:
            return print(mjs)
        
        mjs = mjs.split('\n')
        ret = {}
        for s in mjs:
            stat = re.split(':\s+', s)
            if len(stat) == 2:
                k, v = stat
                ret[k] = v
                
        return ret
    
    def read_out(self, jobid = None, logpath = 'default'):    
        '''
        Read a job output. If jobid is given, then it will look at the default location.
        Otherwise user must specify the logpath.
        '''
        
        if jobid is not None:
            if logpath == 'default':
                name = self.my_job_stats(jobid)['Job name']
                logpath = os.path.join(self.path, name+'-'+str(jobid)+'.log')
        
        f = open(logpath, "r")
        print(f.read())
    
    def monthly_usage(self, account='default', raw = False):
        '''
        Monthly usage stats for the user. If raw is true then it will return the os output
        '''
        user = self.user
        
        if account == 'default':
            account = self.account
        
        start = datetime.now().strftime("%Y-%m-01")
        end = datetime.now().strftime("%Y-%m-31")
        
        job = 'sreport cluster AccountUtilizationByUser Accounts='+account+' -T billing Start='+start+' End='+end       
        mu = subprocess.check_output(job,
                                     stderr=subprocess.STDOUT,
                                     shell=True,
                                     encoding = 'utf-8')
        if raw:
            print(mu)
        else:
            for i in mu.split('\n'):
                if bool(re.search(account+'\s+billing', i)):
                    total = int(re.findall('\d{2,}', i)[0])

                if bool(re.search('\s'+user+'\s', i)):
                    my_out = i
                    myusage = int(re.findall('\d{2,}', i)[0]) 

            print(my_out)
            print('\nTotal usage is', total, 'your usage is', myusage, 'this is', str(round(100*myusage/total, 2))+'%','of the total')   
    