"""
Contain context of term occurrence in a document
docid is the id of document, from 0 to n, n is the number of total documents
tfidf is the relevance number
"""

import re
import json

class Posting:
    def __init__(self, docid=-1, tf=0, tfidf=0.0):
        self.docid = docid
        self.tf = tf
        self.tfidf = tfidf  # use word frequency here for now
    
    '''
    Read string format posting
    '''
    
    @classmethod
    def read(cls, str_representation: str):
        data = re.findall("[0-9.]+", str_representation)
        return Posting(int(data[0]), int(data[1]), float(data[2]))

    
    '''
    read posting list
    postings should be separated by ;
    '''
    @classmethod
    def read_posting_list(cls, posting_list: str):
        postings = re.split(";", posting_list.strip())
        return [cls.read(p) for p in postings if len(p) > 2]
    
    
    def __str__(self):
        return str([self.docid, self.tf, self.tfidf])
    
    def __lt__(self, other):
        return self.tfidf < other.tfidf
    
    def __le__(self, other):
        return self.tfidf <= other.tfidf
    
    def __eq__(self, other):
        return self.tfidf == other.tfidf
    
    def __ge__(self, other):
        return self.tfidf >= other.tfidf
    
    def __gt__(self, other):
        return self.tfidf > other.tfidf
    
    def __ne__(self, other):
        return self.tfidf != other.tfidf


if __name__ == '__main__':
    postings = []
    print(postings[1])
