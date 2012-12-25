'''
Created on Dec 22, 2012

@author: Yutao
'''

def get_linkedin_id(url):
    find_index = url.find("linkedin.com/")
    if find_index >= 0:
        return url[find_index + 13:].replace('/', '-')
    return None