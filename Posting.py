"""
Contain context of term occurrence in a document
docid is the id of document, from 0 to n, n is the number of total documents
tfidf is the relevance number
"""


class Posting:
    def __init__(self, docid: int, tfidf: int, position: list):
        self.docid = docid
        self.tfidf = tfidf  # use word frequency here for now
        self.position = position
    
    def to_list(self):
        return [self.docid, self.tfidf, self.position]
