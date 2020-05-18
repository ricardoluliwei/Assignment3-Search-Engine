# from multiprocessing import Pool, Queue, Lock
from multiprocessing.dummy import Pool as ThreadPool
from pathlib import Path
import os
import re
import json
from bs4 import BeautifulSoup
import string
import nltk

from nltk.stem import PorterStemmer
import math
import hashlib

from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet as wn
from nltk import pos_tag
from collections import defaultdict

from tokenizer import compute_word_frequencies
from tokenizer import tokenize
from Posting import Posting

class Indexer:
    '''
    src_dir is the directory of web contents json file: DEV
    dest_dir is the index directory: Index
    Index should contain directories 0, 1, 2, 3, ... , 9 and a, b, c, ... , z
    In each directory, every single term is a file. (ex.  apple.txt)
    each file contains the posting list of that term, one posting in a line
    '''
    
    def __init__(self, src_dir: Path, index_dir: Path, log_dir: Path):
        self.src_dir = src_dir
        self.index_dir = index_dir
        self.log_dir = log_dir
    
    def construct_index(self):
        pass
    
    '''
    Create empty directory for index and log
    '''
    
    def create_dir(self):
        if not self.index_dir.exists():
            self.index_dir.mkdir()
        
        for char in list(string.ascii_lowercase):
            index_path = self.index_dir / char
            if not index_path.exists():
                index_path.mkdir()
        
        for num in list("0123456789"):
            index_path = self.index_dir / num
            if not index_path.exists():
                index_path.mkdir()
        
        if not self.log_dir.exists():
            self.log_dir.mkdir()
    
    '''
    Return a list of the absolute path of the input file
    Also construct json files:
    docid_path.json, map docid to absolutepath
    Store docid_path.json into log_dir
    '''
    
    def open_source_dir(self) -> list:
        docid_to_path = dict()
        
        # create list of all subdirectories that we need to process
        pathParent = Path(self.src_dir)
        directory_list = [dir for dir in self.src_dir.iterdir() if dir.is_dir()]
        
        docid = 0
        # getting all the .json file paths and adding them to a list to be processed by threads calling tokenize()
        # also creates a hashtable that maps docID => filepath urls
        for directory in directory_list:
            for file in Path(directory).iterdir():
                if file.is_file():
                    docid_to_path[int(docid)] = str(file.resolve())
                    docid += 1
        
        # Writes "hashtable" file that maps the docIDs to filepaths of those documents.
        with open(str(self.log_dir / "docid_to_path.json"), "w+") as file:
            json.dump(docid_to_path, file)
        
        return [docid_to_path[k] for k in sorted(docid_to_path.keys())]
    
    '''
    Read in certain number of source json file, return a inverted index
    dictionary, key is the term, value is the posting.
    Also return a dict, key is docid, value is it's url
    '''
    
    def read_batch(self, src_files_paths: list, start: int, limit: int) -> [
        dict, dict]:
        partial_index = defaultdict(lambda: list)
        url_dic = defaultdict(lambda: str)
        try:
            with open(str(self.log_dir / "content_hash.json"), "r") as file:
                #key is content hash value, value is list of docid
                content_hash = json.load(file)
        
        except FileNotFoundError:
            content_hash = defaultdict(lambda: list)

        i = start
        for src_file_path in src_files_paths[start: start + limit]:
            with open(src_file_path, "r", encoding="utf_8") as file:
                json_data = json.load(file)
                url = json_data["url"]
                content = json_data["content"]
                
            url_dic[i] = url
            text = BeautifulSoup(content, features="html.parser").get_text()
            
            #do similarity test here
            
            
            
            tokens = tokenize(text)
            term_to_positions = defaultdict(lambda: list)
            j = 0
            for token in tokens:
                term_to_positions[token].append(j)
            
            for k, v in term_to_positions:
                partial_index[k].append(Posting(i, len(v), v))
            
            i += 1
            
        return [partial_index, url_dic]
    
    
    def get_sim_hash(self, word_frequency: dict):
    
    '''
    Write the in-memory inverted index into disk, and mark the docid as
    complete
    Also construct json files:
    docid_url.json, map docid to it's url, indicate the document has been
    indexed
    docid_caculated_tfidf.json, map docid to the status of it's tfidf score
    '''
    
    def write_batch(self, index: dict, docs: list):
        pass
    
    '''
    
    '''
    
    def caculate_tfidf_score(self):
        pass
    
    '''
    
    '''
    
    def read_index_file(self, term: str) -> list:
        pass


def construct_index(directoryPath: str):
    # posting: docID: frequency,
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
        
        for k, v in frequencies:
            index[k]
        
        print(f"DocID: {path[0]}")
    
    with open(f"index/index", "w", encoding="utf-8") as file:
        json.dump(index, file)


if __name__ == '__main__':
    path = Path("../Assginment3_data")
    if not path.exists():
        path.mkdir()
    srcPath = path / ".." / "DEV"
    destPath = path / "Index"
    logDir = path / "log"
    indexer = Indexer(srcPath, destPath, logDir)
    indexer.create_dir()
    indexer.open_source_dir()
    print("-----DONE!-----")
