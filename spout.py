# -*- coding: utf-8 -*-
# credit : https://qiita.com/kk6/items/8351a6541598cf7151ef
from bs4 import BeautifulSoup
import operator
from urllib.parse import urljoin
import json
import sys
import unicodedata
import time

import requests
# for http/2 streamjob
import storm
# main json formatter
import queue
# local queue
import logging
import os
import redis

import re

from datetime import datetime


class MstdnStream:
    """Mastodon Steam Class

    Usage::

        >>> from mstdn import MstdnStream, MstdnStreamListner
        >>> listener = MstdnStreamListner()
        >>> stream = MstdnStream('https://pawoo.net', 'your-access-token', listener)
        >>> stream.public()

    """
    def __init__(self, base_url, access_token, listener):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({'Authorization': 'Bearer ' + access_token})
        self.listener = listener

    def public(self):
        url = urljoin(self.base_url, '/api/v1/streaming/public')
        resp = self.session.get(url, stream=True)
        resp.raise_for_status()
        event = {}
        
        start_time = time.time()
        cnt = 0
        
        for line in resp.iter_lines():
            line = line.decode('utf-8')
            
            
                

            if not line:
                # End of content.
                #cnt = cnt + 1
                method_name = "on_{event}".format(event=event['event'])
                f = operator.methodcaller(method_name, event['data'])
                f(self.listener)
                # refreash
                event = {}
                continue

            if line.startswith(':'):
                # TODO: Handle heatbeat
                #print('startwith ":" {line}'.format(line=line))
                #pass
                pass
            else:
                key, value = line.split(': ', 1)
                if key in event:
                    event[key] += value
                else:
                    event[key] = value


class MstdnStreamListner:
    
    def __init__(self, theR):
        
        self.r = theR
        self.ts = 1
        self.start_time = time.time()
    
    def _remove_attrs(self, soup, tag):
        try:
            for _ in soup.find_all(tag):
                _.extract()

        except AttributeError:
            pass
        
    def _kana(self, txt):
        hiragana = "ぁあぃいぅうぇえぉおかがきぎくぐけげこごさざしじすずせぜそぞただちぢっつづてでとどなにぬねのはばぱひびぴふぶぷへべぺほぼぽまみむめもゃやゅゆょよらりるれろゎわゐゑをん"
        katakana = "ァアィイゥウェエォオカガキギクグケゲコゴサザシジスズセゼソゾタダチヂッツヅテデトドナニヌネノハバパヒビピフブプヘベペホボポマミムメモャヤュユョヨラリルレロヮワヰヱヲンヴ"
        for _ in txt:
            
            if _ in hiragana or _ in katakana:
                return True
        return False

    def on_update(self, data):
        k = json.loads(data)['content']
        soup = BeautifulSoup(k, "html.parser")

        self._remove_attrs(soup, "a")
        #_remove_attrs(soup, "p")
        self._remove_attrs(soup, "span")
        self._remove_attrs(soup, "br")
        
        #print("", flush=True)
        
        tmpstr = str(soup.prettify())[3:-5]
        tmpstr = tmpstr.replace("<p>","").replace("</p>","")
        
        tmpstr = unicodedata.normalize("NFKC", tmpstr)
        
        #print(tmpstr)
        
        if self._kana(tmpstr):
        
            '''
            print(tmpstr)
            print("")
            res = self.m.parse(tmpstr).splitlines()[:-1]
            for _ in res:
                fa = _.split('\t')
                if u"名詞" in fa[3] and not fa[0] in self.stopwords:
                    print(fa[0], end=' ')
                    r.zincrby('count',fa[0])
            
            print("")
            print("-----------------------")
            #pass
            '''
            #if time.time() - self.start_time > 300:
            #    self.ts = self.ts + 1
            #    self.start_time = time.time()
            emoji_pattern = re.compile("["
                u"\U0001F600-\U0001F64F"  # emoticons
                u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                u"\U0001F680-\U0001F6FF"  # transport & map symbols
                u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                "]+", flags=re.UNICODE)
            tmpstr = emoji_pattern.sub(r'', tmpstr)
            self.r.lpush('msgpool', tmpstr)
            tw = r.get('timewindow')
            tw = int(float(str(tw)[2:-1]))
            if int(time.time()) - tw >= 300:
                r.incr('twno',1)
                r.getset('timewindow',int(time.time()))
            logging.debug("emitting sentence : " + tmpstr)
        

    def on_notification(self, data):
        print(data)
        

    def on_delete(self, data):
        #print("Deleted: {id}".format(id=data))
        pass


pid = os.getpid()
base_path = '/var/log/takatoshi/'
logging.basicConfig(filename=base_path+__file__+str(pid)+'.log', level=logging.DEBUG)
logging.debug(datetime.now())
logging.debug("abs path of py file: " + os.path.abspath(__file__))

r = redis.Redis(host='localhost', port=6379, db=0)  
#r.flushall()      
r.delete('msgpool')
k = r.get('timewindow')
if k is None:
    
    r.getset('timewindow',int(time.time()))
    r.getset('twno',1)
    print("setting time window no. to 1")

listener = MstdnStreamListner(r)
stream = MstdnStream('https://pawoo.net', '785639d26ddb1f0aa3ddac884f9cdf64c0748ae7394388a322b44fa69ec596df', listener)
stream.public()

        
