"""
Author:
Liwei Lu 20294187
Xinyi Ai  37489204
Yiming Tang 60899836
Jiadong Zhang 51693147

"""

from tokenizer import tokenize
import os
from Posting import Posting
import json
from nltk.stem import PorterStemmer
from collections import defaultdict
import math
import time


docid_to_url_path = "/Users/ricardo/Downloads/Assginment3_data/log" \
                   "/docid_to_url.json"
index_path = "/Users/ricardo/Downloads/Assginment3_data/Index"
term_to_idf_path = "/Users/ricardo/Downloads/Assginment3_data/log/term_to_idf" \
                   ".json"
TOTAL_DOCUMENTS = 55393

with open(term_to_idf_path, "r") as file:
    term_to_idf = json.load(file)


class Query:
    def __init__(self, query: str):
        ps = PorterStemmer()
        self.query_list = [ps.stem(token) for token in tokenize(query.strip())]
        self.posting = self._find_all_posting()
        self.idf = term_to_idf
    
    def get_result(self) -> list:
        scores = defaultdict(lambda: float())
        doc_length = defaultdict(lambda: float())
        query_length = 0
        for t, p in self.posting.items():
            query_tfidf = self._tfidf(t)
            query_length += query_tfidf ** 2
            for post in p:
                scores[post.docid] += query_tfidf * post.tfidf
                doc_length[post.docid] += post.tfidf ** 2
        
        result = defaultdict(lambda: float())
        
        query_length = math.sqrt(query_length)
        for k, y in scores.items():
            result[k] = y / (math.sqrt(doc_length[k]) * query_length)
        
        if len(result) >= 20:
            return [k for k, v in
                    sorted(result.items(), key=lambda x: -x[1])[0:20]]
        
        else:
            return [k for k, v in sorted(result.items(), key=lambda x: -x[1])]
    
    def _find_all_posting(self) -> dict:
        posting = defaultdict()
        limit = 100
        for token in self.query_list:
            token_path = index_path + "/" + token[0] + "/" + token + ".txt"
            
            try:
                with open(str(token_path), "r", encoding="utf-8") as file:
                    posting_list = Posting.read_posting_list(file.read())
                    if len(posting_list) > limit:
                        posting[token] = posting_list[:limit]
            except FileNotFoundError:
                print(f"typo: {token}")
        
        return posting
    
    def _tfidf(self, q) -> float:
        return (1 + math.log(self.query_list.count(q))) * self.idf[q]


if __name__ == "__main__":
    
    while True:
        query = input("Enter the query or enter nothing to exit: ")
        
        if not query:
            break
        start_time = time.time()
        result = Query(query).get_result()
        
        urls = []
        
        with open(docid_to_url_path, "r", encoding="utf-8") as file:
            docidtoURl = json.load(file)
        
        for docid in result:
            urls.append(docidtoURl[str(docid)])
        end_time = time.time()
        print(f"Time spent: {end_time - start_time}s")
        for i in range(len(urls)):
            print("{}.".format(i + 1), urls[i])
    
    print("---search engine shut down---!")
