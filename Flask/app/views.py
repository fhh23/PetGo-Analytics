from app import app
from flask import render_template
import rethinkdb as rdb
import redis

def getKey(item):
    return -item[1]

@app.route('/')
@app.route('/index')
def index(chartID = 'chart_ID', chart_type = 'bar', chart_height = 350):
    redis_server = 'ec2-52-11-94-102.us-west-2.compute.amazonaws.com'
    red = redis.StrictRedis(host=redis_server, port=6379, db=0)
    keys = []
    counts = []
    f = open('db.out', 'w')
    print("HI \n", f)
    for key in red.scan_iter():
        count = red.get(key)
	keys.append(key)
        counts.append(count)
    conn = rdb.connect(host='localhost', port=28015, db='test')
    cursor = rdb.table('itemsets').run(conn)
    mySet = []
    myCount = []
    for document in cursor:
        docSet = document["set"]
        docCount = document["count"]
        docZip = zip(docSet, docCount)
        docZip = sorted(docZip, key=getKey)
        print(docZip, f)
        counter = 0
        for x in docZip[:3]:
            mySet.append(x[0])
            myCount.append(x[1])
            
    f.close()
    keys = [ 23, 23, 23 ]
    chart = {"renderTo": chartID, "type": chart_type, "height": chart_height,}
    series = [{"name": 'Label1', "data": [1,2,3]}, {"name": 'Label2', "data": [4, 5, 6]}]
    title = {"text": 'My Title'}
    xAxis = {"categories": keys}
    yAxis = {"title": {"text": 'yAxis Label'}}
    xAxis2 = {"categories": mySet}
    yAxis2 = {"data": myCount}
    return render_template('index.html', chartID=chartID, chart=chart, series=series, title=title, xAxis=xAxis, yAxis=yAxis, xAxis2=xAxis2, yAxis2=yAxis2)
