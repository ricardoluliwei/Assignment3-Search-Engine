from tokenizer import tokenize
import os
import Posting

class Director:
    def __init__(self, indexDEV: str):
        self.indexDEV = indexDEV
    
    #return a list of posting of the token
    def lookup_token(self, token: str) -> list:
        pass

    #return top 5 of urls list
    def lookup_query(self, query: str) -> list:
        if query == "cristina lopes":
            return ["https://www.informatics.uci.edu/explore/faculty-profiles/cristina-lopes/",
"https://www.informatics.uci.edu/professor-crista-lopes-recognized-for-excellence-in-undergraduate-teaching/",
"https://www.informatics.uci.edu/lopes-analyzes-big-code-with-funding-from-darpa/",
"https://www.informatics.uci.edu/lopes-featured-speaker-at-2016-opensimulater-community-conference/#content",
"https://www.informatics.uci.edu/lopes-honored-with-2017-aito-test-of-time-award/"
]
        if query == "machine learning":
            return ["https://cml.ics.uci.edu/faculty/",
"https://cml.ics.uci.edu/2018/08/two-new-nsf-awards-in-machine-learning-for-sameer-singh/",
"https://cml.ics.uci.edu/2018/03/workshop-for-the-philosophy-of-machine-learning/",
"https://cml.ics.uci.edu/2016/03/southern-california-machine-learning-symposium/",
"https://cml.ics.uci.edu/2011/09/2011_scmlworkshop/"]
        if query == "ACM":
            return ["https://www.ics.uci.edu/~gmark/Home_page/Publications.html",
"https://cradl.ics.uci.edu/publications/",
"https://cml.ics.uci.edu/2017/06/singh-talk-oc-acm-chapter/#content",
"https://cradl.ics.uci.edu/redmiles-named-acm-distinguished-scientist/",
"https://cml.ics.uci.edu/2014/02/2014_acmfellows/"]
        if query == "master of software engineering":
            return ["https://mswe.ics.uci.edu/",
"https://www.informatics.uci.edu/grad/mswe/",
"https://www.informatics.uci.edu/grad/courses/",
"https://www.informatics.uci.edu/explore/facts-figures/",
"https://www.informatics.uci.edu/undergrad/courses/"]


    

if __name__ == '__main__':
    
    indexdev = os.path.join("index")
    director = Director(indexdev)
    
    while True:
        query = input("Enter the query or enter nothing to exit: ")
        if not query:
            break
            
        urls = director.lookup_query(query)
        for url in urls:
            print(url)

    print("---search engine shut down---!")
        