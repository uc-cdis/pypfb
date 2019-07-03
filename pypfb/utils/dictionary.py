from dictionaryutils import DataDictionary, dictionary


def init_dictionary(url):
    if "http" in url:
        d = DataDictionary(url=url)
    else:
        d = DataDictionary(root_dir=url)
    dictionary.init(d)
    return d
