#!/usr/bin/python
#encoding:utf-8

''' Mongodb python tools   by Sheng.Du   @2015.1.7  Version: 1.0
    
    Class CollectionOp Functions:
        insert,remove,find,update,count operations of a colletion.
'''

from pymongo import MongoClient

class CollectionOp():

    def __init__(self, host='localhost', port=27017, db=None, collection=None):
        ''' A MongoClient object 
        '''
        client = MongoClient(host,port)
        self.collection = client[db][collection]
    
    def insert(self,doc_or_docs):
        ''' Insert a docment(s) into this collection
            param:
                doc: can be a dict or a dicts list/tuple
            return:
                True: all docs insert
                False: some docs are duplicate
        '''
        try:
            self.collection.insert(doc_or_docs,continue_on_error=True)
            return True
        except Exception,e:
            if "E11000" in str(e):
                pass
            else:
                raise
    
    def count(self):
        ''' Get the number of docment in this collection
        '''
        return self.collection.count()
    
    def remove(self,**kwags):
        ''' Remove a docment(s) from this collection
            param:
                **kwags: conditions to specilize the docment(s)
                e.g.: Call remove("name"="Jerry","gender"="male") will remove
                      all docments which name is Jerry and gender is male.
        '''
        return self.collection.remove(kwags)
    
    def update(self,update_part,**kwags):
        ''' Update a certain docment of this collection.
            param:
                update_part: a dict to update into the certain docment(s)
                **kwags: conditions to specilize the docment(s)
        '''
        return self.collection.update(update_part,{"$set":kwags})

    def find_one(self,**kwags):
        ''' Find one docment from this collection
            **kwags: conditons to specilize the docment(s)
        '''
        return self.collection.find_one(kwags)
    
    def find_all(self,**kwags):
        ''' Find all docment satisfy the conditions
            **kwags: conditions to specilize the docment(s)
        '''
        return list(self.collection.find())
    
if __name__ == "__main__":
    mmc = CollectionOp(db="test",collection="test")
    doc = {"name":"Jerry","age":23,"gender":"male"}
    doc1 = {"name":"ds"}
    print mmc.insert((doc,doc,doc,doc1))
    #print mmc.count()
    #print mmc.find_all()
    #print mmc.remove()