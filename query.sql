create index parent_idx ON reddit_posts(parent_id);
create index name_idx ON reddit_posts(name);
select replace(rf.body,x'0A', '\n'), replace(rs.body,x'0A','\n'),rf.subreddit_name FROM reddit_posts rf JOIN reddit_posts rs ON rf.name = rs.parent_id;
