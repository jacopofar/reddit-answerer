# reddit-answerer
Reddit comment dump parser to build a Question/answers model.

Currently, this script process a reddit dump in bz2 format (see [here](https://github.com/jacopofar/reddit-wordcount) for more details about it), extract all of the father-son comments pair and store them in an Elasticsearch instance. This allow for later full-text search. A month of comments is about 30-35 millions of comment-reply couples (of about 50M comments in a month).

One day, I may try to build a sequence-to-sequence model from it. (Note: seeing the time and resources necessay to build a seq2seq model probably it will never be done :( )
