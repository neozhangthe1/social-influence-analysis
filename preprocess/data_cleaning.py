'''
Created on Dec 21, 2012

@author: Yutao
'''
from src.database import mongo
from src.database.mysql import Mysql
from src.metadata import verbose
from src.metadata import utils
from bs4 import UnicodeDammit
import codecs
import matplotlib.pyplot as plt

import pickle
    
mongo = mongo.Mongo()
mysql = Mysql()


def get_also_view_urls():
    col = mongo.db['person_profiles_1221']
    urls = []
    index = 0
    for item in col.find() == 0:
        if index % 1000:
            print index
        index+=1
        try:
            for a in item['also_view']:
                urls.append(a['linkedin_id'].strip())
        except Exception,e:
            print e
    out = codecs.open('alsoview_urls','w','utf-8')
    for u in urls:
        out.write(u+'\n')
    out.close()
    
def get_labeled_data_aminer_profile():
    index = 0
    col = mongo.db['labeled_data']
    for item in col.find():
        verbose.index(index)
        index+=1
        item['aminer_profile_str']=mysql.get_person_aminer_profile(item['aminer'])
        col.save(item)
    
def get_labeled_data_linkedin_profile():
    index = 0
    col = mongo.db['labeled_data']
    for item in col.find():
        verbose.index(index)
        index+=1
        item['linkedin_profile_str']=mongo.get_person_linkedin_profile(item['linkedin'])
        col.save(item)
        
def get_labeled_data_name():
    col = mongo.db['labeled_data']
    for item in col.find():
        name = mysql.get_person_name(item['aminer'])
        item['aminer_name']=UnicodeDammit(name).markup
        col.save(item)    
    
def gen_labeled_dataset():
    col = mongo.db["lenin_label_data"]
    labeled_col = mongo.db["labeled_data"]
    for item in col.find():
        if 'e+' not in item['aminer'] and 'view' not in item['linkedin'] and item['flag']=='1':
            print item['aminer']
            labeled_col.save({'_id':int(item['aminer']),
                              'aminer':int(item['aminer']),
                              'linkedin':utils.get_linkedin_id(item['linkedin']),
                              'url':item['linkedin'],
                              'rank':item['rank'],
                              'rel':item['rel']})

def check_werid_url():
    col = mongo.db["lenin_label_data"]
    aid = []
    for item in col.find():
        if 'view' in item['linkedin']:
            if item['rank']<40000 and item['flag']=='1':
                verbose.debug(item['aminer'])
                aid.append(item['aminer'])
    

    
def check_urls():
    labeled = mongo.db["labeled_data"]
    temp = mongo.db['temp_person_profiles']
    l_urls = []
    t_urls = []
    index = 0
    count = 0
    for item in labeled.find():
        verbose.index(index, 1000)
        index+=1
        l_urls.append(item['url'])
    for item in temp.find():
        t_urls.append(item['url'])
            
            
def check_if_labeled_data_exist():
    col = mongo.db["labeled_data"]
    col61 = mongo.db['temp_person_profiles']
    not_in_db = []
    ids = []
    index = 0
    count = 0
    for item in col.find():
        verbose.index(index, 1000)
        index+=1
        query = col61.find({"_id":item['linkedin']})
        if query.count() == 0:
            url_q = col61.find({'url':item['url']})
            if url_q.count() == 0:
                verbose.debug("not in")
                verbose.debug(item['linkedin'])
                not_in_db.append(item['url'])
    out = codecs.open('urls','w','utf-8')
    for u in not_in_db:
        out.write(u+'\n')
    out.close()
            
def filter_werid_data():
    col = mongo.db["aminer_linkedin_filtered_1124"]
    count = 0
    index = 0
    for item in col.find():
        print "INDEX "+str(index)
        index+=1
        #print len(item['aminer'].keys())
        if len(item['aminer'].keys())<=2:
            rel = mysql.get_person_relation(item['aminer']['id'])
            print len(rel)
            if len(rel) <5: 
                print item['aminer']['id']
                count+=1
                print count
    print count
    
def check_labeled_data():
    col = mongo.db["labeled_data"]
    count = 0
    index = 0
    data = {"id":[],"rel":[],"rank":[]}
    for item in col.find():
        print "INDEX "+str(index)
        index+=1
        rel = mysql.get_person_relation(item['aminer'])
        print len(rel)
        item['rel']={"count":len(rel), 
                     "rel":[{"pid1":r[1],"pid2":r[2],"similarity":r[3],"rel_type":r[4]} for r in rel],
                     }
        col.save(item)
    print count
    
def dump_lenin_data():
    col = mongo.db['lenin_label_data']
    data = []
    for item in col.find():
        data.append(item)

    dump = open("lenin_data",'w')
    pickle.dump(data, dump)
    dump.close()
    
def dump_mongo(col_name):
    col = mongo.db[col_name]
    data = []
    for item in col.find():
        data.append(item)

    dump = open(col_name,'w')
    pickle.dump(data, dump)
    dump.close()
    
def compare1():
    col = mongo.db["aminer_linkedin_labeled_1208"]
    aid = []
    index = 0
    for item in col.find():
        if index % 1000:
            print index
        index+=1
        aid.append(item['aminer']['id'])
    dump = open("E:\\My Projects\\Eclipse Workspace\\CrossLinking\\src\\preprocess\\lenin_data")
    data = pickle.load(dump)
    lid = []
    for person in data:
        lid.append(person['aminer'])

def compare():
    col = mongo.db["aminer_linkedin_1123"]
    aid = []
    lid = []
    index = 0
    for item in col.find():
        if index % 1000 == 0:
            print index
        index+=1
        if not item.has_key('aminer'):
            verbose.debug('aminer '+item['name'])
        else:
            for a in item['aminer']:
                aid.append(a['id'])
        if not item.has_key('linkedin'):
            verbose.debug('linkedin '+item['name'])
        else:
            for l in item['linkedin']:
                lid.append(l['url'])
    dump_aid = open('aminer_linkedin_1123_aminer_id','w')
    pickle.dump(aid,dump_aid)
    dump_lid = open('aminer_linkedin_1123_linkedin_url','w')
    pickle.dump(lid,dump_lid)
    col = mongo.db['aminer_linkedin_labeled_1124']
    labeled_aid = []
    labeled_lid = []
    labeled_data = col.find({'$or':[{'labels.homepage_match':True},
                                    {'labels.domain_match':True},
                                    {'labels.aff_match':True}]})
    index = 0
    print "start"
    for item in labeled_data:
        if index % 1000 == 0:
            print index
        index+=1
        verbose.debug(item['name']+' '+str(item['_id']))
        labeled_aid.append(item['aminer']['id'])
        labeled_lid.append(item['linkedin']['url'])
        
    x = []
    for a in labeled_aid:
        x.append(str(a))
        
    dump = open("E:\\My Projects\\Eclipse Workspace\\CrossLinking\\src\\preprocess\\lenin_data")
    data = pickle.load(dump)
    xaid= []
    xlid = []
    for person in data:
        xaid.append(person['aminer'])
        xlid.append(person['linkedin'])
        
def get_alsoview_url():
    urls = []
    index = 0
    for item in mongo.db['temp_alsoview_person_profiles'].find():
        verbose.index(index)
        index+=1
        if item.has_key('also_view'):
            for al in item['also_view']:
                x = mongo.db['temp_alsoview_person_profiles'].find({'_id':al['url']})
                if x.count()==0:
                    urls.append(al['linkedin_id'])
    out = open('urls','w')
    for u in set(urls):
        out.write(u+'\n')
    out.close()

def check_alsoview_in_db():
    index = 0
    count = 0
    for item in mongo.person_profiles.find(limit=10):
        verbose.index(index)
        index+=1
        if item.has_key('also_view'):
            for alsoview in item['also_view']:
                x = mongo.person_profiles.find({"_id":alsoview['url']})
                if x.count()>0:
                    alsoview['crawled']=True
                    verbose.debug(alsoview['url'])
                    verbose.debug(count)
                else:
                    alsoview['crawled']=False
            mongo.person_profiles.save(item)
        
    
def main():
    get_labeled_data_linkedin_profile()

if __name__ == "__main__":
    main()