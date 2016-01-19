import bz2
import json
import sqlite3
import time
from datetime import datetime

class Progress_tracker:
    def __init__(self,total,start_position=0,decay=0.001):
        self.total = total
        self.latest_time = time.time()
        self.latest_position = start_position
        self.steps_per_second = -1
        self.decay = decay
    def report_progress(self,position):
        latest_speed = (position-self.latest_position) / (time.time()-self.latest_time)
        if(self.steps_per_second == -1):
            self.steps_per_second = latest_speed
        else:
            self.steps_per_second = latest_speed * self.decay + self.steps_per_second *(1-self.decay)
        self.latest_position = position
        self.latest_time = time.time()
    def estimate_remaining_seconds(self):
        #print("total: {0} latest position: {1} steps per second: {2}".format(self.total,self.latest_position,self.steps_per_second))
        return (self.total-self.latest_position)/self.steps_per_second
    def estimate_end_time(self):
        return time.time()+self.estimate_remaining_seconds()
    def estimate_end_timestamp(self):
        return datetime.fromtimestamp(self.estimate_end_time()).isoformat()
    def speed(self):
        return self.steps_per_second
    def percentage(self):
        return 100*self.latest_position/self.total
conn = sqlite3.connect('reddit_posts.db')
c = conn.cursor()
c.execute('CREATE TABLE reddit_posts(parent_id VARCHAR(15), name VARCHAR(15), link_id VARCHAR(15), subreddit_name TEXT, score INTEGER, body TEXT)')
count_parents = 0
count_lines = 0


with bz2.open('/home/user/shared/reddit_data/2015/RC_2015-05.bz2') as comp_file:
    pending_inserts = []
    progress = Progress_tracker(53000000)
    for line in comp_file:
        count_lines += 1
        post = json.loads(line.decode('utf-8'))
        #parent_id,name,link_id,subreddit_name,score, content
        pending_inserts.append((post['parent_id'],post['name'],post['link_id'],post['subreddit'],post['score'],post['body']))
        if (post['parent_id'] != post['link_id']):
            count_parents += 1
        if(count_lines % 2000 == 0):
            progress.report_progress(count_lines)
            c.executemany('INSERT INTO reddit_posts VALUES (?,?,?,?,?,?)', pending_inserts)
            conn.commit()
            pending_inserts = []
            print("processed up to line {0}, parents so far: {1}, ETA: {2} processed per second: {3} ({4}%)".format(count_lines,count_parents,progress.estimate_end_timestamp(),progress.speed(),progress.percentage()))

