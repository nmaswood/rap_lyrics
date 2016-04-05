
from pymongo import MongoClient
from urllib.parse import unquote
import json

class extract():

    def __init__(self):
        self.client    = MongoClient('localhost',27017)['RAP']
        self.db        = self.client['LYRICS']

    def gen(self):

        data = []

        for _id, unit in enumerate(self.db.find()):
                song_name = unquote(unit['song_name']).split(":")[-1]
                artist    = unit['artist']
                album     = unit['album']
                year      = unit['year']
                language  = unit['language']
                lyrics    = unit['lyrics']
                data.append([artist, song_name, album, year,language, lyrics])

        with open ('data.json','w') as jsonout:
            json.dump(data, jsonout)


if __name__ == '__main__':
    
    run = extract()
    run.gen()
