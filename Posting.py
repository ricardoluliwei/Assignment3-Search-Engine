"""
Contain context of term occurrence in a document
docid is the id of document, from 0 to n, n is the number of total documents
tfidf is the relevance number
"""

import re
import json

class Posting:
    def __init__(self, docid=-1, tfidf=-1.0):
        self.docid = docid
        self.tfidf = tfidf  # use word frequency here for now
    
    '''
    Read string format posting
    ex. [1, 12, [1,2,3]] will be read as posting(docid = 1, tfidf = 12)
    '''
    
    @classmethod
    def read(cls, str_representation: str):
        data = re.findall("[0-9.]+", str_representation)
        return Posting(int(data[0]), float(data[1]))

    
    '''
    read posting list
    postings should be separated by ;
    '''
    @classmethod
    def read_posting_list(cls, posting_list: str):
        postings = re.split(";", posting_list.strip())
        return [cls.read(p) for p in postings if len(p) > 2]
    
    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__,sort_keys=True, indent=4)
    
    def __str__(self):
        return str([self.docid, self.tfidf])
    
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
