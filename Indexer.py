#from multiprocessing import Pool, Queue, Lock
from multiprocessing.dummy import Pool as ThreadPool
from pathlib import Path
import os
import re
import json
from bs4 import BeautifulSoup
from tokenizer import compute_word_frequencies
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



def construct_index(directoryPath: str):
    #posting: docID: frequency,
    index = defaultdict(lambda: defaultdict(lambda: int()))
    file_paths = getAllFilePaths(directoryPath)
    limit = 10000
    count = 0
    
    if not Path("index").exists():
        Path("index").mkdir()
        
    for path in file_paths:
        count += 1
        if (count < 49000):
            continue
            
        if (path[0] == 49112):
            print(path[1])
            
        if (count % 10000 == 0):
            with open(f"index/index_{count/limit}.txt", "w", encoding="utf-8") \
                as file:
                json.dump(index, file)
            index = defaultdict(lambda: defaultdict(lambda: int()))
            print(f"dumped file index/index_{count/limit}.txt")
            
        tokens = [PorterStemmer().stem(token) for token in tokenize_a_file(
            path[1]) if len(token) > 1]

        for token in tokens:
            index[token][path[0]] += 1
        
        print(f"DocID: {path[0]}")

    with open(f"index/index_{count / limit + 1}.txt", "w", encoding="utf-8") as file:
        json.dump(index, file)
        
# def merge_file(dirPath: str):
#     if not Path(dirPath).exists():
#         return
#
#     file_list = Path(dirPath).iterdir()
#     os.rename(file_list[0], "index.json")
#     for file in file_list[1:]:
    

def tokenize_a_file(path: str):
    with open(path, "r", encoding="utf-8") as file:
        data = json.load(file)
        content = data["content"]
        soup = BeautifulSoup(content, features="html.parser")
        text = soup.get_text()
        if len(text) == 0:
            return list()
        tokens = word_tokenize(text)
    return tokens



# Gets all filepaths
def getAllFilePaths(directoryPath: str) -> list:
    listFilePaths = list()

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
                iDocID += 1
    
    print(iDocID)
    return listFilePaths




if __name__ == '__main__':
    path = Path(os.path.pardir) / "DEV"
    construct_index(path)
    print("-----DONE!-----")