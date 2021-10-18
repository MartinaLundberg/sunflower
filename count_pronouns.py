import json
import os
import io
from flask import Flask, jsonify, Response
from celery import Celery
import matplotlib
matplotlib.use('Agg')
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import matplotlib.pyplot as plt

#from https://flask.palletsprojects.com/en/2.0.x/patterns/celery/
def make_celery(app):
    celery = Celery(app.import_name, backend='rpc://',
                    broker='pyamqp://guest@localhost//')
    celery.conf.update(app.config)

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)
            
    celery.Task = ContextTask
    return celery

flask_app = Flask(__name__)
celery = make_celery(flask_app)

# Flask methods
#Method for pronoum counting, methods=['GET'] 
@flask_app.route('/results')
def get_count():
    result = find_pronouns.delay()
    rest = result.get()
    rest.pop('total')
    return jsonify(rest)

#Method for bar plot
@flask_app.route('/barplot')
def plot_bar():
    result1 = find_pronouns.delay()
    rest1 = result1.get()
    resultplot = make_plot(rest1)
    output = io.BytesIO()
    FigureCanvas(resultplot).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')

#barplot function
def make_plot(pron):
    
    total = pron['total']
    pron.pop('total')
    #Normalize pronouns with total amount of non retweeted tweets
    new = {k: v / total for k, v in pron.items()}
    
    stats = pron.keys()
    count = new.values()
    
    #create barplot
    fig, figbar = plt.subplots(figsize = (8,6))
    fig.patch.set_facecolor('#FFFAFA')
    figbar.bar(stats, count, color = "#BC8F8F")
    fig.legend(loc='best', fontsize=12)

    plt.xticks(rotation = 20, size = 12)
    plt.ylabel("Normalized pronouns", size = 12)
    
    return(fig)
    
@celery.task(name='make_celery.find_prounouns')
#pronoun & total counting function
def find_pronouns():
    
    stat = {'han': 0,'hon': 0,'den': 0,'det': 0,'denna': 0,'denne': 0,'hen': 0,'total': 0}
    data = os.listdir("data_small")
   
    for file in data:
        
        data_files = 'datans/' + file
        save_text = []
        
        #open file in data
        with open(data_files) as multiple_tweets:
            
            for line in multiple_tweets:
                
                #skip empty lines, else load json
                if len(line.split()) == 0:
                    continue
                load_tweet = json.loads(line)
                    
                #check so that the tweet is not retweeted        
                if not 'retweeted_status' in load_tweet:
                    save_text.append(load_tweet['text'])
                    stat['total'] += 1
        
        #look for pronouns in tweet text, add count if yes
        for load_tweet in save_text:
            
            if 'han' in load_tweet:
                stat['han'] += 1
            elif 'hon' in load_tweet:
                stat['hon'] += 1
            elif 'den' in load_tweet:
                stat['den'] += 1
            elif 'det' in load_tweet:
                stat['det'] += 1
            elif 'denna' in load_tweet:
                stat['denna'] += 1
            elif 'denne' in load_tweet:
                stat['denne'] += 1
            elif 'hen' in load_tweet:
                stat['hen'] += 1

    return stat
    #return json.dumps(stat)

if __name__ == '__main__':
    flask_app.run(host='0.0.0.0',debug=True)
