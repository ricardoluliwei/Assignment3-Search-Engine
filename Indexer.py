#from multiprocessing import Pool, Queue, Lock
from multiprocessing.dummy import Pool as ThreadPool
from pathlib import Path
import os
import re
import json
from bs4 import BeautifulSoup
from tokenizer import compute_word_frequencies
from tokenizer import tokenize_a_file
import string
import nltk

from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
import math
import hashlib

from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet as wn
from nltk import pos_tag
from collections import defaultdict



class Indexer:
    def __init__(self, dir_path: str, config_path: str):
        self.dir_path = dir_path
        self.config_path = config_path
        
    
    def construct_index(self):
        pass
    
    '''
    Return a dictionary, key is docid, value is the absolute path of the
    input file
    Also construct a json file docHashTable.json, map docid to [absolute
    path, url, status of index(True or false, finished index or not)]
    Store docHashTable.json into config_path
    '''
    def open_source_dir(self, src_dir: str) -> dict:
        pass
    
    '''
    Read in certain number of source json file, construct a inverted index
    dictionary, key is the term, value is the posting.
    Also return a list of docid that has been read in
    '''
    def read_batch(self, src_files: dict, start: int, end: int) -> [dict, list]:
        pass
    
    '''
    Write the in-memory inverted index into disk, and mark the docid as
    complete
    '''
    def write_batch(self, index: dict, docs: list):
        pass
    
    

def construct_index(directoryPath: str):
    #posting: docID: frequency,
    index = defaultdict(lambda: defaultdict(lambda: int()))
    file_paths = getAllFilePaths(directoryPath)
    count = 0
    
    if not Path("index").exists():
        Path("index").mkdir()
        
    for path in file_paths:
        count += 1
        index = defaultdict(lambda: list())
        print(f"dumped file index/index.txt")
            
        tokens = [PorterStemmer().stem(token) for token in tokenize_a_file(
            path[1])]
        
        frequencies = compute_word_frequencies(tokens)
        
        for k,v in frequencies:
            index[k]
        
        print(f"DocID: {path[0]}")

    with open(f"index/index", "w", encoding="utf-8") as file:
        json.dump(index, file)
        
# def merge_file(dirPath: str):
#     if not Path(dirPath).exists():
#         return
#
#     file_list = Path(dirPath).iterdir()
#     os.rename(file_list[0], "index.json")
#     for file in file_list[1:]:


# Gets all filepaths
def getAllFilePaths(directoryPath: str) -> list:
    listFilePaths = list()
    hashTableIDToUrl = dict()

    # create list of all subdirectories that we need to process
    pathParent = Path(directoryPath)
    directory_list = [(pathParent / dI) for dI in os.listdir(directoryPath) if
                      Path(Path(directoryPath).joinpath(dI)).is_dir()]

    iDocID = 0
    # getting all the .json file paths and adding them to a list to be processed by threads calling tokenize()
    # also creates a hashtable that maps docID => filepath urls
    for directory in directory_list:
        for files in Path(directory).iterdir():
            if files.is_file():
                fullFilePath = directory / files.name
                listFilePaths.append([int(iDocID), str(fullFilePath)])
                with open(files, "r", encoding="utf-8") as file:
                    data = json.load(file)
                    url = data["url"]
                    print(url)
                hashTableIDToUrl[int(iDocID)] = url
                iDocID += 1

    # Writes "hashtable" file that maps the docIDs to filepaths of those documents.
    with open(os.path.join("index", "hashtable.txt"), "w+") as hash:
        hash.write(json.dumps(hashTableIDToUrl))
    
    return listFilePaths




if __name__ == '__main__':
    path = Path(os.path.pardir) / "DEV"
    getAllFilePaths(path)
    print("-----DONE!-----")