from tokenizer import tokenize
import os
from Posting import Posting
import json
from nltk.stem import PorterStemmer
from collections import defaultdict
import math

index_path = "/Users/mac/Desktop/INF121/proj3/Assginment3_data/Index"
TOTAL_DOCUMENTS = 55393


class Query:
    def __init__(self, query: str):
        ps = PorterStemmer()
        self.query_list = [ps.stem(token) for token in tokenize(query)]
        self.posting = self._find_all_posting()
        self.idf = dict
    
    def get_result(self) -> list:
        scores = defaultdict(lambda: float())
        length = defaultdict(lambda: float())
        
        for t, p in self.posting.items():
            for post in p:
                scores[post.docid] += self._tfidf(t) * post.tfidf
                length[post.docid] += post.tfidf ** 2
        
        result = defaultdict(lambda: float())
        for k, y in scores.items():
            result[k] = y / math.sqrt(length[k])
        
        return [k for k, v in sorted(result.items(), key=lambda x: -x[1])[0:5]]
    
    def _find_all_posting(self) -> dict:
        posting = defaultdict()
        limit = 100
        for token in self.query_list:
            token_path = index_path + "/" + token[0] + "/" + token + ".txt"

            with open(str(token_path), "r", encoding = "utf-8") as file:
                posting_list = Posting.read_posting_list(file.read())
                if len(posting_list) > limit:
                    posting[token] = posting_list[:limit]
                    
        return posting

    def _tfidf(self, q) -> float:
        return (1 + math.log(self.query_list.count(q))) * math.log(TOTAL_DOCUMENTS/len(self.posting[q]))


if __name__ == "__main__":
    
    while True:
        query = input("Enter the query or enter nothing to exit: ")
        
        if not query:
            break

        result = Query(query).get_result()

        urls = []

        with open("/Users/mac/Desktop/INF121/proj3/Assginment3_data/log/docid_to_url.json", "r", encoding="utf-8") as file:
            docidtoURl = json.load(file)
        
        for docid in result:
            urls.append(docidtoURl[str(docid)])
                
        for url in urls:
            print(url)

    print("---search engine shut down---!")
        
