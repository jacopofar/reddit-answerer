import bz2
import json
import sqlite3
conn = sqlite3.connect('reddit_posts.db')
c = conn.cursor()
c.execute('CREATE TABLE reddit_posts(parent_id VARCHAR(15), name VARCHAR(15), link_id VARCHAR(15), subreddit_name TEXT, score INTEGER, body TEXT)')
count_parents = 0
count_lines = 0


with bz2.open('/home/user/shared/reddit_data/2015/RC_2015-05.bz2') as comp_file:
    pending_inserts = []
    for line in comp_file:
        count_lines += 1
        post = json.loads(line.decode('utf-8'))
        #parent_id,name,link_id,subreddit_name,score, content
        pending_inserts.append((post['parent_id'],post['name'],post['link_id'],post['subreddit'],post['score'],post['body']))
        if (post['parent_id'] != post['link_id']):
            count_parents += 1
        if(count_lines % 1000 == 0):
            c.executemany('INSERT INTO reddit_posts VALUES (?,?,?,?,?,?)', pending_inserts)
            conn.commit()
            pending_inserts = []
            print("processed up to line {0}, parents so far: {1}".format(count_lines,count_parents))

