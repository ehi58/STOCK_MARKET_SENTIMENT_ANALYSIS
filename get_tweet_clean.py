#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from time import sleep
import numpy as np
import pandas as pd


# In[ ]:


# Importing GetOldTweets3
import GetOldTweets3 as got


# In[ ]:


class my_twitter:
    import time
    from datetime import datetime, date, timedelta

    def DownloadTweets(Query, SinceDate, UntilDate):
        '''
        Downloads all tweets from a certain month in three sessions in order to avoid sending too many requests. 
        Date format = 'yyyy-mm-dd'. 
        Query=string.
        '''

        since = datetime.strptime(SinceDate, '%Y-%m-%d')
        until= datetime.strptime(UntilDate, '%Y-%m-%d')
        tenth = since + timedelta(days = 10)
        twentieth = since + timedelta(days=20)

        print ('starting first download')
        print(timedelta(days = 10), timedelta(days=20))
        first = got.manager.TweetCriteria().setQuerySearch(Query).setSince(since.strftime('%Y-%m-%d')).setUntil(tenth.strftime('%Y-%m-%d')).setTopTweets(True).setMaxTweets(5)
        firstdownload = got.manager.TweetManager.getTweets(first)
        firstlist=[[tweet.id,
                    tweet.permalink,
                    tweet.to,
                    tweet.username,
                    tweet.text,
                    tweet.date,
                    tweet.retweets,
                    tweet.favorites,
                    tweet.mentions,
                    tweet.hashtags,
                    tweet.geo] for tweet in firstdownload]
        df_1 = pd.DataFrame.from_records(firstlist, columns = ['id', 'permalink','to','username', 'text',
                                                       'date', 'retweets','favorites','mentions', 'hashtags','geo'])

        df_1.to_csv("%s_1.csv" % SinceDate)

        time.sleep(600)

        print ('starting second download')
        second = got.manager.TweetCriteria().setQuerySearch(Query).setSince(tenth.strftime('%Y-%m-%d')).setUntil(twentieth.strftime('%Y-%m-%d')).setTopTweets(True)
        seconddownload = got.manager.TweetManager.getTweets(second)
        secondlist=[[tweet.id,
                    tweet.permalink,
                    tweet.to,
                    tweet.username,
                    tweet.text,
                    tweet.date,
                    tweet.retweets,
                    tweet.favorites,
                    tweet.mentions,
                    tweet.hashtags,
                    tweet.geo] for tweet in seconddownload]

        df_2 = pd.DataFrame.from_records(secondlist, columns = ['id', 'permalink','to','username', 'text',
                                                       'date', 'retweets','favorites','mentions', 'hashtags','geo'])
        #df_2.to_csv("%s_2.csv" % SinceDate)

        time.sleep(600)

        print ('starting third download')
        third = got.manager.TweetCriteria().setQuerySearch(Query).setSince(twentieth.strftime('%Y-%m-%d')).setUntil(until.strftime('%Y-%m-%d')).setTopTweets(True)
        thirddownload = got.manager.TweetManager.getTweets(third)
        thirdlist=[[tweet.id,
                    tweet.permalink,
                    tweet.to,
                    tweet.username,
                    tweet.text,
                    tweet.date,
                    tweet.retweets,
                    tweet.favorites,
                    tweet.mentions,
                    tweet.hashtags,
                    tweet.geo] for tweet in seconddownload]

        df_3 = pd.DataFrame.from_records(thirdlist, columns = ['id', 'permalink','to','username', 'text',
                                                       'date', 'retweets','favorites','mentions', 'hashtags','geo'])
        #df_3.to_csv("%s_3.csv" % SinceDate)

        df=pd.concat([df_1,df_2,df_3])
        df.to_csv("%s.csv" % SinceDate)

        return df


# In[ ]:




