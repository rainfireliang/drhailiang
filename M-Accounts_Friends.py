# Crawling combing multiple accounts
import twitter
import sys
import time
from urllib2 import URLError
from httplib import BadStatusLine
import json
import io
from functools import partial
from sys import maxint
import numpy as np
import multiprocessing

#---------------------------Define Functions----------------------------------#
def oauth_login((CONSUMER_KEY,CONSUMER_SECRET,OAUTH_TOKEN,OAUTH_TOKEN_SECRET)):
    auth = twitter.oauth.OAuth(OAUTH_TOKEN, OAUTH_TOKEN_SECRET,
                               CONSUMER_KEY, CONSUMER_SECRET)
    twitter_api = twitter.Twitter(auth=auth)
    return twitter_api

def make_twitter_request(twitter_api_func, max_errors=10, *args, **kw):
    def handle_twitter_http_error(e, wait_period=2, sleep_when_rate_limited=True):
        if wait_period > 3600: # Seconds
            print >> sys.stderr, 'Too many retries. Quitting.'
            raise e
        if e.e.code == 401:
            with open("badcases_friends_unauth.txt", "a") as infile:
                infile.write(str(i)+'\r\n')
            print >> sys.stderr, 'Encountered 401 Error (Not Authorized)'
            return None
        elif e.e.code == 404:
            print >> sys.stderr, 'Encountered 404 Error (Not Found)'
            return None
        elif e.e.code == 429:
            print >> sys.stderr, 'Encountered 429 Error (Rate Limit Exceeded)'
            if sleep_when_rate_limited:
                print >> sys.stderr, "Retrying in 15 minutes...ZzZ..."
                sys.stderr.flush()
                time.sleep(60*15 + 5)
                print >> sys.stderr, '...ZzZ...Awake now and trying again.'
                return 2
            else:
                raise e # Caller must handle the rate limiting issue
        elif e.e.code in (500, 502, 503, 504):
            print >> sys.stderr, 'Encountered %i Error. Retrying in %i seconds' % \
                (e.e.code, wait_period)
            time.sleep(wait_period)
            wait_period *= 1.5
            return wait_period
        else:
            raise e

    # End of nested helper function

    wait_period = 2
    error_count = 0

    while True:
        try:
            return twitter_api_func(*args, **kw)
        except twitter.api.TwitterHTTPError, e:
            error_count = 0
            wait_period = handle_twitter_http_error(e, wait_period)
            if wait_period is None:
                return
        except URLError, e:
            error_count += 1
            print >> sys.stderr, "URLError encountered. Continuing."
            if error_count > max_errors:
                print >> sys.stderr, "Too many consecutive errors...bailing out."
                raise
        except BadStatusLine, e:
            error_count += 1
            print >> sys.stderr, "BadStatusLine encountered. Continuing."
            if error_count > max_errors:
                print >> sys.stderr, "Too many consecutive errors...bailing out."
                raise

def get_friends_followers_ids(twitter_api, screen_name=None, user_id=None,
                              friends_limit=maxint, followers_limit=maxint):

    # Must have either screen_name or user_id (logical xor)
    assert (screen_name != None) != (user_id != None), \
    "Must have screen_name or user_id, but not both"

    # See https://dev.twitter.com/docs/api/1.1/get/friends/ids and
    # https://dev.twitter.com/docs/api/1.1/get/followers/ids for details
    # on API parameters

    get_friends_ids = partial(make_twitter_request, twitter_api.friends.ids,
                              count=5000)
    get_followers_ids = partial(make_twitter_request, twitter_api.followers.ids,
                                count=5000)

    friends_ids, followers_ids = [], []

    for twitter_api_func, limit, ids, label in [
                    [get_friends_ids, friends_limit, friends_ids, "friends"],
                    [get_followers_ids, followers_limit, followers_ids, "followers"]
                ]:

        if limit == 0: continue

        cursor = -1
        while cursor != 0:

            # Use make_twitter_request via the partially bound callable...
            if screen_name:
                response = twitter_api_func(screen_name=screen_name, cursor=cursor)
            else: # user_id
                response = twitter_api_func(user_id=user_id, cursor=cursor)

            if response is not None:
                ids += response['ids']
                cursor = response['next_cursor']

            print >> sys.stderr, 'Fetched {0} total {1} ids for {2}'.format(len(ids),
                                                    label, (user_id or screen_name))

            # XXX: You may want to store data during each iteration to provide an
            # an additional layer of protection from exceptional circumstances

            if len(ids) >= limit or response is None:
                break

    # Do something useful with the IDs, like store them to disk...
    return friends_ids[:friends_limit], followers_ids[:followers_limit]

accounts=[]
with open('APIs.txt') as f:
    for line in f:
        line=line.rstrip()
        accounts.append(tuple(line.split('\t')))

def UserFollow((i,j)):
    twitter_api = oauth_login(accounts[j])
    try:
        friends_ids, followers_ids = get_friends_followers_ids(twitter_api, user_id=i,followers_limit=5000)
        for l in friends_ids:
            with open("friends"+str(j)+".txt", "a") as infile:
                infile.write(str(i)+'\t'+str(l)+'\r\n')
        for k in followers_ids:
            with open("followers"+str(j)+".txt", "a") as infile:
                infile.write(str(i)+'\t'+str(k)+'\r\n')
        with open('completed.txt','a') as f:
            f.write(str(i)+'\r\n')
            
    except:
        with open("badcases_friends.txt", "a") as infile:
            infile.write(str(i)+'\r\n')

# do the task it now with multiprocessing
if __name__ == '__main__':
    pool = multiprocessing.Pool(processes=307)

    accounts=[]
    with open('APIs.txt') as f:
        for line in f:
            line=line.rstrip()
            accounts.append(tuple(line.split('\t')))

    ids=[]
    with open('f_ids_alters.txt', 'rb') as f:
        for line in f:
            line=line.rstrip()
            ids.append(line)
    print len(ids)

    res=pool.map(UserFollow,zip(ids,np.random.choice(range(len(accounts)),len(ids))))

