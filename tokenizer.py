import re
from collections import defaultdict
from bs4 import BeautifulSoup
import json

pattern = re.compile("[a-z0-9]+")


def tokenize(text: str) -> list:
    tokens = []
    try:
        tokens += re.findall(pattern, text.lower())
    except Exception as err:
        print(err)
    finally:
        return tokens


def compute_word_frequencies(tokens: list) -> dict:
    count = defaultdict(lambda: int())
    try:
        i = 0
        for token in tokens:
            count[token] += 1
            i += 1
    except:
        pass
    finally:
        return count


