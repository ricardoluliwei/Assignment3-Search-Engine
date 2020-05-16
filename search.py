from tokenizer import tokenize
import os
import Posting

class Director:
    def __init__(self, indexDEV: str):
        self.indexDEV = indexDEV
    
    #return a list of posting of the token
    def lookup_token(self, token: str) -> list():
        pass

    #return top 5 of urls list
    def loopup_query(self, query: str) -> list():
        return list()


if __name__ == '__main__':
    
    indexdev = os.path.join("index")
    director = Director(indexdev)
    
    while True:
        query = input("Enter the query or enter nothing to exit: ")
        if not query:
            break
            
        urls = director.loopup_query(query)
        for url in urls:
            print(url)

