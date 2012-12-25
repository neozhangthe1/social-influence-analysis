'''
Created on Dec 18, 2012

@author: Yutao
'''
from src.metadata import settings
import pymongo
from bs4 import BeautifulSoup

class Mongo(object):
    def __init__(self):
        self.con = pymongo.Connection(settings.MONGO_HOST)
        self.db = self.con[settings.MONGO_NAME]
        self.person_profiles = self.db["person_profiles"]   

    def get_person_linkedin_profile(self, id):
        profile_str=""
        res = self.db['temp_person_profiles'].find({'_id':id})
        if res.count()>0:
            item = res.next()
            if item.has_key('interests'):
                profile_str+=(item['interests']+'\n')
            if item.has_key('education'):
                for e in item['education']:
                    profile_str+=(e['name']+'\n')
                    if e.has_key('desc'):
                        profile_str+=(e['desc']+'\n')
            if item.has_key('group'):
                if item['group'].has_key('member'):
                    profile_str+=(item['group']['member']+'\n')
                if item['group'].has_key('affilition'):
                    for a in item['group']['affilition']:
                        profile_str+=(a+'\n')
            profile_str+=(item['name']['family_name']+' '+item['name']['given_name'])
            if item.has_key('overview_html'):
                soup = BeautifulSoup(item['overview_html'])
                profile_str+=(' '.join(list(soup.strings))+'\n')
            if item.has_key('locality'):
                profile_str+=(item['locality']+'\n')
            if item.has_key('skills'):
                for s in item['skills']:
                    profile_str+=(s+'\n')
            if item.has_key('industry'):
                profile_str+=(item['industry']+'\n')
            if item.has_key('experience'):
                for e in item['experience']:
                    if e.has_key('org'):
                        profile_str+=(e['org']+'\n')
                    if e.has_key('title'):
                        profile_str+=(e['title']+'\n')
            if item.has_key('summary'):
                profile_str+=(item['summary']+'\n')
            profile_str+=('url')
            if item.has_key('specilities'):
                profile_str+=(item['specilities']+'\n')
            if item.has_key('homepage'):
                for k in item['homepage'].keys():
                    for h in item['homepage'][k]:
                        profile_str+=(h+'\n')
            if item.has_key('honors'):
                for h in item['honors']:
                    profile_str+=(h+'\n')
        return profile_str

class Mongo61(object):
    def __init__(self):
        self.con = pymongo.Connection(settings.MONGO_HOST61)
        self.db = self.con[settings.MONGO_NAME]