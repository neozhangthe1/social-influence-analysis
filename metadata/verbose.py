'''
Created on Dec 19, 2012

@author: Yutao
'''

def debug(out):
    try:
        print "[DEBUG]"+str(out)
    except Exception,e:
        print e
        
def index(out, step = 1000):
    if out % step == 0:
        print "[INDEX]"+str(out)