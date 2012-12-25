'''
Created on Dec 20, 2012

@author: Yutao
'''
from src.database.mysql import Mysql

if __name__ == "__main__":
    mysql = Mysql()
    missing_data, person_missing_data = mysql.get_missing_data()
    import pickle
    m_dump = open("missing_data1", 'w')
    p_dump = open("person_missing_data1", 'w')
    pickle.dump(missing_data, m_dump)
    pickle.dump(person_missing_data, p_dump)
    
    