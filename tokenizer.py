import re
from collections import defaultdict
from bs4 import BeautifulSoup
import json

pattern = re.compile("[\W_]+")


def tokenize(text: str) -> list:
    tokens = []
    try:
        tokens += pattern.sub(' ', text).lower().split()
    except Exception as err:
        print(err)
    finally:
        return tokens


def compute_word_frequencies(tokens: list) -> dict:
    count = defaultdict(lambda: list())
    try:
        i = 0
        for token in tokens:
            count[token].append(i)
            i += 1
    except:
        pass
    finally:
        return count


