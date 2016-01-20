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
first_step = 2
conn = sqlite3.connect('reddit_comments.db')
c = conn.cursor()

if (first_step <= 0):
    print('step 0, creating SQLite3 database and the initial table')
    c.execute('CREATE TABLE reddit_comments(parent_id VARCHAR(15), name VARCHAR(15), link_id VARCHAR(15), subreddit_name TEXT, score INTEGER, body TEXT)')
#ballbark estimation of "parented" comments
count_parents = 33000000

if (first_step <= 1):
    print('step 1, importing the reddit comments inside the DB...')
    count_lines = 0
    with bz2.open('/home/user/shared/reddit_data/2015/RC_2015-05.bz2') as comp_file:
        pending_inserts = []
        #estimation of total comments
        progress = Progress_tracker(55000000)
        for line in comp_file:
            count_lines += 1
            comment = json.loads(line.decode('utf-8'))
            #parent_id,name,link_id,subreddit_name,score, content
            pending_inserts.append((comment['parent_id'], comment['name'], comment['link_id'], comment['subreddit'], comment['score'], comment['body']))
            if (comment['parent_id'] != comment['link_id']):
                count_parents += 1
            if(count_lines % 3000 == 0):
                progress.report_progress(count_lines)
                c.executemany('INSERT INTO reddit_comments VALUES (?,?,?,?,?,?)', pending_inserts)
                conn.commit()
                pending_inserts = []
                if(count_lines % 6000 == 0):
                    print("processed up to line {0}, parents so far: {1}, ETA: {2} processed per second: {3} ({4}%)".format(count_lines,count_parents,progress.estimate_end_timestamp(),progress.speed(),progress.percentage()))
        c.executemany('INSERT INTO reddit_comments VALUES (?,?,?,?,?,?)', pending_inserts)
        conn.commit()
    print('processed {0} comments, {1} were not top-level and very likely to be replies to in the same file'.format(count_lines,count_parents))
if (first_step <= 2):
    print('step 2, extracting comment bodies and corresponding replies and saving them in a JSONs file')
    c.execute('create index if not exists parent_idx ON reddit_comments(parent_id)')
    print('created index on parent_id')
    c.execute('create index if not exists name_idx ON reddit_comments(name)')
    print('created index on name')
    progress = Progress_tracker(count_parents)
    written_rows = 0
    with open('qa.jsons', 'w',50000) as f:
        c.execute("select rf.body AS q, rs.body AS a,rf.subreddit_name AS sub, rs.name AS id FROM reddit_comments rf JOIN reddit_comments rs ON rf.name = rs.parent_id")
        print('processing query results...')
        for row in c:
            written_rows += 1
            f.write(json.dumps(row)+'\n')
            if(written_rows % 1000 == 0):
                progress.report_progress(written_rows)
                print("written up to line {0}, of max {1}, ETA: {2} processed per second: {3} ({4}%)".format(written_rows,count_parents,progress.estimate_end_timestamp(),progress.speed(),progress.percentage()))


