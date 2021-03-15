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
        
    def queue(self, by_name = None, by_time = None):
        '''
        Returns your job queue as a pandas dataframe, if <name> is given that it will only returns
        the jobs with the tame <name>
        '''
        user = self.user
        df = pd.read_csv(StringIO(subprocess.check_output('squeue -u '+ user,
                                                          stderr=subprocess.STDOUT,
                                                          shell=True,
                                                          encoding = 'utf-8')), sep = '\s+')



        if by_name is not None:
            df = df[df['NAME'] == by_name]

        if by_time is not None:

            def calc_min(x):
                smh = [1/60, 1, 60]
                x = x.split(":")
                x.reverse()
                t = 0
                for i in range(len(x)):
                    t += smh[i]*int(x[i])
                return t

            t = df['TIME']
            t = [calc_min(x) for x in t] # calc the time in mins        

            symbol, ltime = by_time[:1], int(by_time[1:])

            if symbol == '>':
                df = df[[x>ltime for x in t]]
            elif symbol == '<':
                df = df[[x<ltime for x in t]]

        return df
    
    def jobids(self, by_name = None, by_time = None):
        '''
        Returns list of jobids
        by_name = <name of your batch job>
        by_time = If you want to cancel jobs that runs more, less than x mins, then by_time='>x', by_time='<x'. 
        Ex: Slurm.jobids(by_name = 'mysims'), Slurm.jobids(by_name = 'mysims', by_time = '>10'), Slurm.jobids()
        '''
        queue = self.queue(by_name = by_name, by_time = by_time)
        jids = queue['JOBID'].to_list()
        return jids
        
    def cancel(self, jids):
        '''
        Cancels jobs by jobids
        Ex: Slurm.cancel([4815, 4816, 2342])
        '''
        if not isinstance(jids, list):
            jids = [jids]  
        job = 'scancel ' + ' '.join([str(i) for i in jids])
        subprocess.check_output(job,
                                stderr=subprocess.STDOUT,
                                shell=True,
                                encoding = 'utf-8')
        print('Done!')
        return None
    
    def cancel_by(self, by_name = None, by_time = None):
        '''
        see Slurm.jobids, if you call it empty: Slurm.cancel_by(), it will cancel all of your jobs
        '''
        jids = self.jobids(by_name = by_name, by_time = by_time)
        
        if (by_name is None) & (by_time is None):
            i = input('You will cancel all of your jobs, continue?(y)')
            if i not in ['y', 'Y']:
                print('Nothing happened')
                return None
                
        self.cancel(jids)
    
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
                if v == 'None':
                    del Settings[k] # if it is set to None, remove it
                else:
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

    def my_job_stats(self, jid, raw = False):
        '''
        Returns the job statistics as a dictionary
        '''
        mjs = subprocess.check_output('my_job_statistics ' + str(jid),
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
    
    def read_out(self, jid = None, logpath = 'default'):    
        '''
        Read a job output. If jid is given, then it will look at the default location.
        Otherwise user must specify the logpath.
        '''
        
        if jid is not None:
            if logpath == 'default':
                name = self.my_job_stats(jid)['Job name']
                logpath = os.path.join(self.path, name+'-'+str(jid)+'.log')
        
        f = open(logpath, "r")
        print(f.read())
    
    def monthly_usage(self, account='default', raw = False, quota = None):
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
            
            ret = 'Total usage is', str(total), 'your usage is', str(myusage), 'this is', str(round(100*myusage/total, 2))+'%','of the total.'
            ret = ' '.join(ret)
            if quota is not None:
                ret2 = ' You are using', str(round(100*myusage/quota, 2))+'%', 'of your quota.'
                ret2 = ' '.join(ret2)
                ret = ret+ret2

            print(ret)
               
            return None
    