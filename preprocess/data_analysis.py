'''
Created on Dec 23, 2012

@author: Yutao
'''
import networkx as nx
from src.database.mysql import Mysql
from src.database.mongo import Mongo
from src.metadata import verbose
from src.algorithm import levenshtein
import pickle

import matplotlib.pyplot as plt

mongo = Mongo()
mysql = Mysql("arnet_db_12_23")

def plot_duplicated_names():
    dup = {}
    index = 0
    for item in mongo.db['aminer_linkedin_1123'].find():
        verbose.index(index)
        index+=1
        count = 0
        if item.has_key('aminer'):
            count+=len(item['aminer'])
        if item.has_key('linkedin'):
            count+=len(item['linkedin'])
        dup[item['name']]=count
    
    dup_frq = {}
    for i in dup:
        if not dup_frq.has_key(dup[i]):
            dup_frq[dup[i]] = 0
        dup_frq[dup[i]]+=1
        

def plot_missing_data():
    d = open("attr_counter")
    profile_counter = pickle.load(d)
    attr_counter = pickle.load(d)
    missing = {}
    for key in attr_counter:
        missing[key]=float(2985414-attr_counter[key])/2985414
    from collections import OrderedDict
    from pylab import arange
    ordered_missing = OrderedDict(sorted(missing.items(), key=lambda k :k[1]))
    plt.bar(range(len(ordered_missing.values())),sorted(missing.values()))
    plt.xticks(arange(len(missing))+0.4, ordered_missing.keys(), rotation = "vertical")
    plt.plot(missing)
    plt.savefig("missing_data.png")
    profile_missing = {}
    for i in profile_counter.keys():
        profile_missing[16-i] = profile_counter[i]
    plt.bar(profile_missing.keys(),profile_missing.values())
    plt.xticks(arange(len(profile_missing))+0.4, profile_missing.keys())
    
    m_dump = open("missing_data1")
    p_dump = open("person_missing_data1")
    missing_data = pickle.load(m_dump)
    person_missing_data = pickle.load(p_dump)
        
def profile_to_bag_of_words():
    pass

def plot_profile_similarity():
    col = mongo.db['labeled_data']
    idx = 0
    sim = {}
    sim['levenshtein'] = {}
    sim['levenshtein_ratio'] = {}
    sim['jaro'] = {}
    sim['jaro_winkler'] = {}
    sim['unigrams'] = {}
    sim['bigrams'] = {}
    sim['trigrams'] = {}
    sim['cosine'] = {}
    sim['words_in_common'] = {}
    sim['words_in_common_ratio'] = {}
    for item in col.find():
        verbose.index(idx)
        idx+=1
        if item.has_key('similarity'):
            if item.has_key('valid'):
                if item['valid'] == True:
                    sim['levenshtein'][item['_id']] = item['similarity']['levenshtein']
                    sim['levenshtein_ratio'][item['_id']] = item['similarity']['levenshtein_ratio']
                    sim['jaro'][item['_id']] = item['similarity']['jaro']
                    sim['jaro_winkler'][item['_id']] = item['similarity']['jaro_winkler']
                    sim['unigrams'][item['_id']] = item['similarity']['unigrams']
                    sim['bigrams'][item['_id']] = item['similarity']['bigrams']
                    sim['trigrams'][item['_id']] = item['similarity']['trigrams']
                    sim['cosine'][item['_id']] = item['similarity']['trigrams']
                    sim['words_in_common'][item['_id']] = item['similarity']['words_in_common']
                    sim['words_in_common_ratio'][item['_id']] = item['similarity']['words_in_common_ratio']
    measures = ['levenshtein','levenshtein_ratio','jaro','jaro_winkler','unigrams',
              'bigrams','trigrams','cosine','words_in_common','words_in_common_ratio']
    lines = []
    for s in measures:
        fig = plt.figure(figsize=(30,10))
        plt.xlabel(s)
        plt.ylabel("similarity")
        plt.plot(sim[s].values())            
        plt.savefig(s+".png")
        plt.close()
        
    for s in measures:
        fig = plt.figure(figsize=(20,10))
        plt.xlabel(s)
        plt.hist(sim[s].values(),100,facecolor='green')            
        plt.savefig(s+"-hist.png")
        plt.close()
    
def get_profile_similarity():
    col = mongo.db['labeled_data']
    idx = 0
    for item in col.find():
        verbose.index(idx)
        idx+=1
        try:
            aminer = item['aminer_profile_str']
            linkedin = item['linkedin_profile_str']
            sim = {}
            sim['levenshtein'] = levenshtein.distance_levenshtein_distance(aminer, linkedin)
            sim['levenshtein_ratio'] = levenshtein.distance_levenshtein_ratio(aminer, linkedin)
            sim['jaro'] = levenshtein.distance_jaro(aminer, linkedin)
            sim['jaro_winkler'] = levenshtein.distance_levenshtein_jaro_winkler(aminer, linkedin)
            sim['unigrams'] = levenshtein.distance_unigrams_same(aminer, linkedin)
            sim['bigrams'] = levenshtein.distance_bigrams_same(aminer, linkedin)
            sim['trigrams'] = levenshtein.distance_trigrams_same(aminer, linkedin)
            sim['cosine'] = levenshtein.distance_cosine_measure(aminer, linkedin)
            sim['words_in_common'] = levenshtein.number_of_words_in_common(aminer, linkedin)
            sim['words_in_common_ratio'] = float(sim['words_in_common'])/(len(aminer)+len(linkedin))
            sim['valid'] = True
            if aminer == "" or linkedin == "":
                sim['valid'] = False
            item['similarity'] = sim
            
            col.save(item)
        except Exception, e:
            print e
            print item['_id']

def plot_labeled_data_degree():
    col = mongo.db['labeled_data']
    degree = []
    degree_dict = {}
    for item in col.find():
        degree.append(item['rel']['count'])
        degree_dict[item['_id']]=item['rel']['count']
    sorted_degree = sorted(degree_dict.items(), key=lambda x:x[1], reverse=True)
    filtered = []
    for i in degree:
        if i!=0:
            filtered.append(i)
    import math
    plt.hist([math.log(x) for x in filtered],100, facecolor='green', log=True)
    degree_frq = {}
    his = plt.hist()
    plt.loglog(his[0][0],his[1][0])
    plt.show()

def plot_labeled_linkedin_degree():
    col = mongo.db['temp_person_profiles']
    degree = []
    missed = []
    for item in col.find():
        if item.has_key('also_view'):
            deg = 0
            for i in item['also_view']:
                x = mongo.db['temp_person_profiles'].find({'_id':i['url']})
                if x.count()>0:
                    deg +=1
                else:
                    x = mongo.db['temp_alsoview_person_profiles'].find({'_id':i['url']})
                    if x.count()>0:
                        deg +=1
                        missed.append(i['linkedin_id'])
                        verbose.debug(item['_id'])
            degree.append(deg)
        else:
            degree.append(0)
    plt.hist(degree,10, facecolor='green')
    plt.show()

def generate_aminer_network():
    g = nx.Graph()
    for rels in mysql.get_relations():
        for rel in rels:
            g.addedge(rel[0],rel[1])
            
def plot_degree_distribution(degrees):
    fig = plt.figure(figsize=(30,10))
    plt.xlabel("degree")
    plt.hist(degrees,200, facecolor='green', log=True)
    plt.savefig('degrees.png')
    plt.close()
            
def check_connected_component(g):
    connected_components = nx.connected_component_subgraphs(g)
    degrees = []
    number_of_nodes = []
    for c in connected_components:
        degrees.append(c.degree())
        number_of_nodes.append(c.number_of_nodes())
    sorted_nodes = sorted(number_of_nodes)
            
def generate_aminer_network_from_file():
    input = open('na_person_relation_online.txt')
    g = nx.Graph()
    index = 0
    for line in input:
        if index % 10000 == 0:
            print index
        index+=1
        x = line.strip().split('\t')
        try:
            g.add_edge(x[0],x[1])
        except Exception,e:
            print e
    g.remove_node('-1')
    
def plot_hindex():
    import numpy as np
    import matplotlib.mlab as mlab
    import matplotlib.pyplot as plt
    dump = open("E:\\My Projects\\Eclipse Workspace\\CrossLinking\\src\\preprocess\\lenin_data")
    data = pickle.load(dump)
    ranks = []    
    verbose.debug("data loaded")
    for d in data:
        ranks.append(d['rank'])
    
    sranks = sorted(ranks)
    mu, sigma = 100, 15
    #x = mu + sigma*np.random.randn(10000)
    x = range(0,len(sranks))
    
    # the histogram of the data
    n, bins, patches = plt.hist(x, 50, normed=1, facecolor='green', alpha=0.75)
    
    # add a 'best fit' line
    y = sranks#mlab.normpdf( bins, mu, sigma)
    l = plt.plot(bins, y, 'r--', linewidth=1)
    
    plt.xlabel('Smarts')
    plt.ylabel('Probability')
    plt.title(r'$\mathrm{Histogram\ of\ IQ:}\ \mu=100,\ \sigma=15$')
    plt.axis([40, 160, 0, 0.03])
    plt.grid(True)

    verbose.debug("show")
    plt.show()

        
if __name__ == "__main__":
    get_profile_similarity()
        
    