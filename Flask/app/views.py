from app import app
from flask import render_template
import rethinkdb as rdb
import redis

@app.route('/')
@app.route('/index')
def index(chartID = 'chart_ID', chart_type = 'bar', chart_height = 350):
    redis_server = 'ec2-52-11-94-102.us-west-2.compute.amazonaws.com'
    red = redis.StrictRedis(host=redis_server, port=6379, db=0)
    keys = []
    counts = []
    for key in red.scan_iter():
        count = red.get(key)
	keys.append(key)
        counts.append(count)
    
    keys = [ 23, 23, 23 ]
    chart = {"renderTo": chartID, "type": chart_type, "height": chart_height,}
    series = [{"name": 'Label1', "data": [1,2,3]}, {"name": 'Label2', "data": [4, 5, 6]}]
    title = {"text": 'My Title'}
    xAxis = {"categories": keys}
    yAxis = {"title": {"text": 'yAxis Label'}}
    return render_template('index.html', chartID=chartID, chart=chart, series=series, title=title, xAxis=xAxis, yAxis=yAxis)
