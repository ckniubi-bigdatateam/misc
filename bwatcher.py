import time
import redis
import math
import os
import random
import json

alpha = 0.2

s = set()
single = {}
accu = {}
upd = {}

l = []
r = redis.Redis(host='localhost', port=6379, db=0)

def conv(x):
    if x is None:
        return 0.0
    ret = 0.0
    try:
        ret = float(str(x)[2:-1])
    except ValueError:
        ret = 0.0
    return ret

def getd(d,word):
    if word in d:
        return d[word]
    return 0

def calc(u0,u1,a0,bnow):
    # Calculate EWMA
    a = a0
    u = u0
    while u < u1:
        a *= 1 - alpha
        u += 1
    a = alpha * bnow + (1-alpha) * a
    return a

def getout(word):
    s.remove(word)
    try:
        single.pop(word)
    except KeyError:
        pass
    try:
        accu.pop(word)
    except KeyError:
        pass
    try:
        upd.pop(word)
    except KeyError:
        pass

# main

while True:

    keys = r.keys("pb_*")
    for _ in keys:
        #print(type(_.decode('utf-8')))
        tmp = r.get(_)
        tmp = conv(tmp)
        if tmp > 0:
            s.add(_.decode('utf-8')[3:])
    tw = r.get('twno')
    tw = int(conv(tw))
    for _ in s.copy():
        word = _
        print(word)
        u = r.get("lastupd_" + word)
        u = conv(u)
        u0 = getd(upd, word)
        a0 = getd(accu, word)
        newa = a0
        print(u,u0)
        if u0 < u:
            print("should")
            newb = r.get("pb_" + word)
            newb = conv(newb)
            newa = calc(u0,u,a0,newb)

        if u < tw:
            newa = calc(u,tw,newa,0)

        print(newa)
        if newa < 0.01:
            getout(word)
        else :
            try :
                single[word] is None
            except KeyError:
                single[word] = []
                
            has = False
            for kkk in single[word]:
                if kkk[0] == u:
                    has = True
            if not has:
                single[word] = single[word] + [(u,newb),]
            upd[word] = tw
            accu[word] = newa
        
    res = sorted(accu.items(), key=lambda x: x[1])

    lll = min(10, len(res))

    filez = open('thelist.json', 'w')
    towrite = {}
    thelist = []
    cnt = 0
    for _ in range(0,lll):
        cnt += 1
        nowwrite = {}
        nowwrite['rank'] = cnt
        #
        word = res[_][0]
        print(single[word])
        nowwrite['word'] = word
        u = r.get("lastupd_" + word)
        u = int(conv(u))
        nowwrite['lastupd'] = str((tw - u)*5) + " mins ago"
        u = r.get("n_" + word)
        u = int(conv(u))
        nowwrite['num'] = u
        nowwrite['chart'] = {}
        nowwrite['chart']['labels'] = ['Item 1', 'Item 2', 'Item 3', 'Item 4', 'Item 5', 'Item 5', 'Item 5', 'Item 5', 'Item 5', 'Item 5']
        dtlist = {}
        dtlist1 = []
        dtlist['label'] = 'Component 1'
        dtlist['fill'] = 'false'
        iter1 = tw - 9
        while iter1 <= tw:
            theval = 0.0
            for __ in single[word]:
                if __[0] == iter1:
                    theval = __[1]
            dtlist1.append(theval)
            iter1 += 1
        dtlist['data'] = dtlist1
        nowwrite['chart']['datasets'] = [dtlist]
        thelist.append(nowwrite)

    towrite['tableData'] = thelist
    json.dump(towrite, filez, ensure_ascii=False, indent=4, sort_keys=True, separators=(',', ': '))
    filez.close()
    print(len(res))
    time.sleep(30)
