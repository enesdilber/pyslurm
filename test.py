import sys
from time import sleep
from numpy.random import randn
import numpy as np

if __name__ == '__main__':
    args = sys.argv[1:]
    
    if args[0] == 'test':
        print('this is a test')
        
    if args[0] == 'sleep':
        sec = int(args[1])
        print('sleep for', sec, 'seconds')
        sleep(sec)
        print('done')
        
    if args[0] == 'normal':
        n = int(args[1])
        mu = float(args[2])
        sigma = float(args[3])
        out = args[4]
        
        x = mu+randn(n)*sigma # array

        np.save(out, x)