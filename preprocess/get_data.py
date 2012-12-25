'''
Created on Dec 18, 2012

@author: Yutao
'''
from src.metadata import settings
from src.metadata import verbose
from src.database.mongo import Mongo
from src.database.mysql import Mysql
from bs4 import BeautifulSoup
from bs4 import UnicodeDammit
import codecs # this is a workaround for writing unicode string to file.

SQL_GET_PERSON = "SELECT * FROM na_person"
SQL_GET_PROFILE = "SELECT * FROM person_profile_ext"


    

def get_aminer_profile():
    path = ""
    mysql = Mysql()
    mysql.cur.execute(SQL_GET_PERSON)
    people = mysql.cur.fetchall()
    for row in people:
        out = codecs.open(path+str(row[0]), 'w', encoding="utf-8")
        print str(row[0])
        out.write(UnicodeDammit(row[1]).markup)
        if row[2]!= -1:
            mysql.cur.execute("SELECT * FROM contact_info c WHERE c.id = '"+str(row[2])+"'")
            contact = mysql.cur.fetchall()
            for c in contact:
                for i in [1,4,5,7,8,14,15,17,18,20,21,22,24]:
                    if c[i]!=None:
                        try:
                            out.write(str(c[i]))
                        except:
                            try:
                                out.write(UnicodeDammit(c[i]).markup)
                            except Exception, e:
                                print e
        mysql.cur.execute("SELECT * FROM na_person_organization o WHERE o.aid = "+str(row[0]))
        organization = mysql.cur.fetchall()
        for o in organization:
            out.write(UnicodeDammit(o[4]).markup)
        out.close()
        
def split_linkedin_dump():
    skip = 2100000
    count = 0
    log = codecs.open("C:\\data\\log"+str(skip)+".txt",'w', encoding="utf-8") 
    id_map = codecs.open("C:\\data\\idmap"+str(skip)+".txt",'w', encoding="utf-8") 
    linkedin_dump = codecs.open('D:\\result.csv', encoding="utf-8")
    out = ""
    linkedin_dump.next()
    for line in linkedin_dump:
        x = 0
        if count < skip:
            count+=1
            if count % 10000 == 0:
                print count
            continue
        print str(count)+':'+str(len(line))
        log.write(str(count)+' '+str(len(line)))
        if line[0] == '"':
            x = line.find('",')
            log.write(str(count)+' '+line[1:x]+'\n')
            verbose.debug(str(count)+' '+line[1:x])
            id_map.write(str(count)+' '+line[1:x]+'\n')
            count+=1
            try:
                out = codecs.open("C:\\data\\linkedin\\"+line[1:x].strip().replace('"'," ").split('?')[0],'w', encoding="utf-8")
            except Exception, e:
                print e
        else:
            log.write("[EXCEPTION]"+str(count)+":"+line+'\n')
        out.write(line[x:])
        
def get_linkedin_profile():
    path = settings.DATA_PATH+"\\linkedin\\"
    mongo = Mongo()
    col = mongo.db['person_profiles']
    index = 0
    res = col.find(skip = index)
    id_map = codecs.open(settings.DATA_PATH+"\\idmap"+str(index)+".txt",'w', encoding="utf-8") 
    for item in res:
        id_map.write(str(index)+' '+item['_id']+'\n')
        index+=1
        out = codecs.open(path+item['_id'].strip().replace('"',' ').split('?')[0], 'w', encoding="utf-8")
        print str(index)
        try:
            print item['_id']+'\n'
        except Exception, e:
            print e
        if item.has_key('interests'):
            out.write(item['interests']+'\n')
        else:
            print '[DEBUG]No Interests'
        if item.has_key('education'):
            for e in item['education']:
                out.write(e['name']+'\n')
                if e.has_key('desc'):
                    out.write(e['desc']+'\n')
        else:
            print '[DEBUG]No Education'
        if item.has_key('group'):
            if item['group'].has_key('member'):
                out.write(item['group']['member']+'\n')
            if item['group'].has_key('affilition'):
                for a in item['group']['affilition']:
                    out.write(a+'\n')
        else:
            print '[DEBUG]No Group'
        out.write(item['name']['family_name']+' '+item['name']['given_name'])
        if item.has_key('overview_html'):
            soup = BeautifulSoup(item['overview_html'])
            out.write(' '.join(list(soup.strings))+'\n')
        else:
            print '[DEBUG]No Overview'
        if item.has_key('locality'):
            out.write(item['locality']+'\n')
        else:
            print '[DEBUG]No Locality'
        if item.has_key('skills'):
            for s in item['skills']:
                out.write(s+'\n')
        else:
            print "[DEBUG]No Skills"
        if item.has_key('industry'):
            out.write(item['industry']+'\n')
        else:
            print "[DEBUG]No Industry"
        if item.has_key('experience'):
            for e in item['experience']:
                if e.has_key('org'):
                    out.write(e['org']+'\n')
                if e.has_key('title'):
                    out.write(e['title']+'\n')
        else:
            print "[DEBUG]No Experience"  
        if item.has_key('summary'):
            out.write(item['summary']+'\n')
        else:
            print "[DEBUG]No Summary"
        out.write('url')
        if item.has_key('specilities'):
            out.write(item['specilities']+'\n')
        else:
            print "[DEBUG]No Specilities"
        if item.has_key('homepage'):
            for k in item['homepage'].keys():
                for h in item['homepage'][k]:
                    out.write(h+'\n')
        else:
            print "[DEBUG]No Homepage"
        if item.has_key('honors'):
            for h in item['honors']:
                out.write(h+'\n')
        else:
            print "[DEBUG]No Honors"
        out.close()
    
def main():
    split_linkedin_dump()


if __name__ == "__main__":
    main()