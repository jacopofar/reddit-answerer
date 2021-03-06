import argparse
import bz2
import json
import sqlite3
from progress import Progress_tracker
import http.client

parser = argparse.ArgumentParser(description='Load one-month reddit dump dump into comment/reply pairs and load them on Elasticsearch')
parser.add_argument('first_step', choices=[0,1,2,3], default=0, help='initial step. 0 to initialize SQLite DB, 1 to populate it, 2 to export comment pairs in a file and 3 to load it on Elasticsearch')
parser.add_argument('sqlite_db_file', default='reddit_comments.db', help='name of the SQLite file to create or use')
parser.add_argument('comments_bz2_file', default='/home/user/shared/reddit_data/2015/RC_2015-05.bz2', help='path of the bz2 compressed JSONs file with reddit comments')
parser.add_argument('elasticearch_address', default='127.0.0.1:9200', help='Elasticsearch address')

args = parser.parse_args()
first_step = args.first_step

conn = sqlite3.connect(args.sqlite_db_file)
c = conn.cursor()

if (first_step <= 0):
    print('step 0, creating SQLite3 database and the initial table')
    c.execute('CREATE TABLE reddit_comments(parent_id VARCHAR(15), name VARCHAR(15), link_id VARCHAR(15), subreddit_name TEXT, score INTEGER, body TEXT)')
#ballbark estimation of "parented" comments
#originally, 32967681, actually 32967939
count_parents = 33000000

if (first_step <= 1):
    print('step 1, importing the reddit comments inside the DB...')
    count_lines = 0
    with bz2.open(args.comments_bz2_file) as comp_file:
        pending_inserts = []
        #estimation of total comments
        progress = Progress_tracker(55000000)
        for line in comp_file:
            count_lines += 1
            comment = json.loads(line.decode('utf-8'))
            #skip deleted comments or replies
            if(comment['body'] =='[deleted]'):
                continue
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

if(first_step <=3):
    print("step 3, loading comments and replies in Elasticsearch")
    count_lines = 0
    docs = ""
    progress = Progress_tracker(count_parents)

    with open('qa.jsons') as qa_file:
            for line in qa_file:
                count_lines += 1
                document = json.loads(line)
                docs +=  json.dumps({"index":{"_id":count_lines}})+ "\n" + json.dumps({"question":document[0],"answer":document[1],"sub":document[2]})+"\n"
                if (count_lines % 200 == 0):
                    conn = http.client.HTTPConnection(args.elasticearch_address)
                    conn.request("POST", "/my_index/my_type/_bulk", docs)
                    docs = ""
                    res = conn.getresponse()
                    progress.report_progress(count_lines)
                    if(res.status == 200):
                        print("inserted {0} lines, ending at {1}, speed {2} row/s, percentage {3} %".format(count_lines,progress.estimate_end_timestamp(),progress.speed(),progress.percentage()))
                    else:
                        data = res.read()
                        print(data.decode("utf-8"))


