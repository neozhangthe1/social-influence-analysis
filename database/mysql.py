'''
Created on Dec 18, 2012

@author: Yutao
'''
from src.metadata import settings
import MySQLdb

from bs4 import UnicodeDammit

SQL_GET_PERSON = "SELECT * FROM na_person"
SQL_GET_PERSON_INFO = "SELECT * FROM na_person WHERE id = %s"
SQL_GET_PERSON_CONTECT = "SELECT * FROM contact_info c WHERE c.id = %s"
SQL_GET_PROFILE = "SELECT * FROM person_profile_ext"
SQL_GET_PERSON_RELATION = "SELECT * FROM `na_person_relation` r WHERE r.pid1 = %s or r.pid2 = %s"
SQL_GET_PERSON_RANK = "SELECT rank FROM person_ext WHERE person_id = %s and type = 2 and topic = -1"
SQL_GET_PERSON_NAME = "SELECT names FROM na_person WHERE id = %s"
SQL_GET_RELATIONS = "SELECT pid1, pid2 FROM `na_person_relation` r"

class Mysql(object):
    def __init__(self, dbname = settings.DB_NAME):
        self.db = MySQLdb.connect(host=settings.DB_HOST,
                                       user=settings.DB_USER,
                                       passwd=settings.DB_PASS,
                                       db=dbname)
        self.cur = self.db.cursor()
    
    def get_relations(self):
        self.cur.execute(SQL_GET_RELATIONS)
        return self.cur.fetchall()
    
    def fetch_person(self):
        self.cur.execute(SQL_GET_PERSON)
        return self.cur.fetchall()
    
    def get_person_relation(self, pid):
        self.cur.execute(SQL_GET_PERSON_RELATION % (pid, pid))
        return self.cur.fetchall()
    
    def get_person_rank(self, pid):
        self.cur.execute(SQL_GET_PERSON_RANK % pid)
        return self.cur.fetchall()
    
    def get_person_name(self, pid):
        self.cur.execute(SQL_GET_PERSON_NAME % pid)
        return self.cur.fetchall()[0][0]
    
    def get_missing_data(self):
        title = ["email",'homepage','affiliation','address',"university","bio"]
        data_type = [4, 5, 7, 8, 20, 21]
        missing_data = {}
        person_missing_data = {}
        for t in title:
            missing_data[t] = 0
        missing_data['org'] = 0
        self.cur.execute(SQL_GET_PERSON)
        index = 0
   
        for row in self.cur.fetchall():
            if index%1000 == 0:
                print index
            index+=1
            count = 0
            try:
                if row[2]!= -1 and row[2]!=None:
                    self.cur.execute(SQL_GET_PERSON_CONTECT % str(row[2]))
                    contact = self.cur.fetchall()
                    for c in contact:
                        for i in range(len(data_type)):
                            if c[data_type[i]]==None:
                                missing_data[title[i]]+=1
                                count+=1
                else:
                    count+=6
                    for c in title:
                        missing_data[c]+=1
                self.cur.execute("SELECT * FROM na_person_organization o WHERE o.aid = "+str(row[0]))
                organization = self.cur.fetchall()
                if len(organization) == 0:
                    missing_data['org']+=1
                    count+=1
                person_missing_data[row[0]] = count
            except Exception,e:
                print e
        
        return missing_data, person_missing_data
        

    
    def get_person_aminer_profile(self, pid):
        profile_str = ""
        self.cur.execute(SQL_GET_PERSON_INFO % pid)
        row = self.cur.fetchall()[0]
        print str(row[0])
        if row[2]!= -1:
            self.cur.execute(SQL_GET_PERSON_CONTECT % str(row[2]))
            contact = self.cur.fetchall()
            for c in contact:
                for i in [4,5,7,8,14,
                          15,17,18,20,21,
                          22,24]:
                    if c[i]!=None:
                        try:
                            profile_str+=(str(c[i])+' ')
                        except:
                            try:
                                profile_str+=(UnicodeDammit(c[i]).markup+' ')
                            except Exception, e:
                                print e
        self.cur.execute("SELECT * FROM na_person_organization o WHERE o.aid = "+str(row[0]))
        organization = self.cur.fetchall()
        for o in organization:
            try:
                profile_str+=(UnicodeDammit(o[4]).markup+' ')
            except Exception,e:
                print e
        return UnicodeDammit(profile_str).markup