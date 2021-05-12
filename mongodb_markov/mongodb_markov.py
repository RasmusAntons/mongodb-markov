import random
import re

import pymongo


class Triple:
    def __init__(self, w1, p1, w2, p2, w3):
        self.w1 = w1
        self.w2 = w2
        self.w3 = w3
        self.p1 = p1
        self.p2 = p2

    def as_dict(self):
        return {'w1': self.w1, 'w2': self.w2, 'w3': self.w3, 'p1': self.p1, 'p2': self.p2}


class MongodbMarkov:
    delimiters = r'([ !"#$%&\'()*+,-./:;<=>?\[\\\]^_`{|}~]+)'

    def __init__(self, db_client=None, db_url='localhost:27017', db_name='mongodb_markov'):
        self.db_url = db_url
        self.db_name = db_name
        self.db_client = db_client or pymongo.MongoClient(self.db_url)
        self.db = self.db_client[self.db_name]
        self.words = self.db['words']
        self.triples = self.db['triples']
        for index in ('w1', 'w2', 'w3', 'p1', 'p2', 'tags'):
            self.triples.create_index([(index, pymongo.HASHED)])

    def insert_text(self, text, tag=None):
        for triple in self._split_text(text):
            self._insert_triple(triple, tag=tag)

    def delete_text(self, text, tag=None):
        for triple in self._split_text(text):
            self._delete_triple(triple, tag=tag)

    def generate_forwards(self, start='', tag=None):
        init = self._find_random(w1=start, tag=tag)
        if init is None:
            return None
        res = [init['w1'], init['p1'], init['w2'], init['p2'], init['w3']]
        while res[-1] != '':
            update = self._find_random(w1=res[-3], p1=res[-2], w2=res[-1], tag=tag)
            res.extend((update['p2'], update['w3']))
        return ''.join(res)

    def generate_backwards(self, end='', tag=None):
        init = self._find_random(w3=end, tag=tag)
        if init is None:
            return None
        res = [init['w3'], init['p2'], init['w2'], init['p1'], init['w1']]
        while res[-1] != '':
            update = self._find_random(w3=res[-3], p2=res[-2], w2=res[-1], tag=tag)
            res.extend((update['p1'], update['w1']))
        return ''.join(reversed(res))

    def generate_from_mid(self, mid, tag=None):
        left = self.generate_backwards(mid, tag=tag) or ''
        right = (self.generate_forwards(mid, tag=tag) or mid)[len(mid):]
        return left + right if left or right else None

    def least_common_words(self, text, limit=1):
        words = [triple.w2 for triple in self._split_text(text)]
        return [e['word'] for e in self.words.find({'word': {'$in': words}}).sort('count').limit(limit)]

    def generate_multiple_from_least_common(self, text, limit=1):
        least_common = self.least_common_words(text, limit=limit)
        for i in range(limit):
            if i < len(least_common):
                result = self.generate_from_mid(least_common[i])
            else:
                result = self.generate_forwards()
            if result is not None:
                yield result

    def _split_text(self, text):
        parts = re.split(self.delimiters, text)
        if len(parts) == 0:
            return
        if parts[0] != '':
            parts.insert(0, '')
            parts.insert(0, '')
        if parts[-1] != '':
            parts.extend(('', ''))
        yield from (Triple(*parts[i: i + 5]) for i in range(0, len(parts) - 4, 2))

    def _insert_triple(self, triple, tag=None):
        self.words.update_one({'word': triple.w2}, {'$inc': {'count': 1}}, upsert=True)
        obj = self.triples.find_one_and_update(triple.as_dict(), {'$inc': {'count': 1}, '$setOnInsert': {'tags': {}}},
                                               upsert=True,
                                               return_document=pymongo.ReturnDocument.AFTER)
        if tag is not None:
            tags = obj.get('tags')
            tags[tag] = tags.get(tag, 0) + 1
            obj['tags'] = tags
            self.triples.replace_one({'_id': obj['_id']}, obj)

    def _delete_triple(self, triple, tag=None):
        word_obj = self.words.find_one_and_update({'word': triple.w2}, {'$inc': {'count': -1}},
                                                  return_document=pymongo.ReturnDocument.AFTER)
        if word_obj and word_obj['count'] < 1:
            self.words.delete_one({'word': triple.w2})
        obj = self.triples.find_one_and_update(triple.as_dict(), {'$inc': {'count': -1}},
                                               return_document=pymongo.ReturnDocument.AFTER)
        if obj:
            if obj['count'] < 1:
                self.triples.delete_one(triple.as_dict())
            elif tag is not None:
                tags = obj.get('tags')
                if tag in tags:
                    tags[tag] = tags.get(tag) - 1
                    if tags[tag] < 1:
                        del tags[tag]
                obj['tags'] = tags
                self.triples.replace_one({'_id': obj['_id']}, obj)

    def _find_random(self, *, w1=None, w2=None, w3=None, p1=None, p2=None, tag=None):
        match = {} if tag is None else {'tags.' + tag: {'$exists': True}}
        for k, v in (('w1', w1), ('w2', w2), ('w3', w3), ('p1', p1), ('p2', p2)):
            if v is not None:
                match[k] = v
        results = tuple(self.triples.find(match))
        if len(results) == 0:
            return None
        if tag is None:
            weights = map(lambda r: r['count'], results)
        else:
            weights = map(lambda r: r['tags'][tag], results)
        return random.choices(results, weights)[0]
