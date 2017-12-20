import redis

sum=0.0
sum1=0.0
cnt = 0
r = redis.Redis(host='localhost', port=6379, db=0)
a = r.keys("pb_*")
for _ in a:
    x = r.get(_)
    try:
        y = float(str(x)[2:-1])
    except ValueError:
        continue
    aa = "n_" + _.decode('utf-8')[3:]
    z = r.get(aa)
    try:
        z = float(str(z)[2:-1])
    except ValueError:
        continue
    aa = "lastupd_" + _.decode('utf-8')[3:]
    zz = r.get(aa)
    try:
        zz = float(str(zz)[2:-1])
    except ValueError:
        continue
    #assert y <= 1.0 and y >= 0.0, "error "+_.decode('utf-8') + ' ' + str(y)
    sum1+=y
    if z > 2 and y > 0:
        sum+=y
        cnt += 1
        print(_.decode('utf-8')[3:]+str(z)+' '+str(y)+' '+str(zz))

print(r.get('twno'))