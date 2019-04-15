from dictionaryutils import DataDictionary, dictionary


def init_dictionary(url):
    if 'http' in url:
        d = DataDictionary(url=url)
    else:
        d = DataDictionary(root_dir=url)
    dictionary.init(d)
    # the gdcdatamodel expects dictionary initiated on load, so this can't be
    # imported on module level
    from gdcdatamodel import models as md
    return d, md
