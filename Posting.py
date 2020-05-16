"""
Contain context of term occurrence in a document
docid is the id of document, from 0 to n, n is the number of total documents
tfidf is the relevance number
"""

import re


class Posting:
    def __init__(self, docid=-1, tfidf=-1, position=list(),
                 str_representation=""):
        self.docid = docid
        self.tfidf = tfidf  # use word frequency here for now
        self.position = position
        self.str_representation = str_representation
        self.deserialize()
    
    def __str__(self):
        return [self.docid, self.tfidf, self.position].__str__()
    
    def deserialize(self):
        if self.str_representation:
            data = re.findall("[0-9]+", self.str_representation)
            self.docid = int(data[0])
            self.tfidf = int(data[1])
            self.position = [int(p) for p in data[2:]]

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
    for i in range(100):
        postings.append(Posting(i, i, [1,2,3]))
        
    with open("test.txt", "w+", encoding="utf-8") as file:
        for posting in postings:
            file.write(posting.__str__() + "\n")
    
    with open("test.txt", "r+", encoding="utf-8") as file:
        postings = []
        for line in file:
            postings.append(Posting(str_representation=line))

        postings = sorted(postings, reverse=True)
        for posting in postings:
            print(posting.__str__())
        