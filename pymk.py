import re
import sys
import itertools
# Write code to create a Spark context.
from pyspark import SparkConf, SparkContext
conf = SparkConf()
sc = SparkContext(conf=conf)
# inputFile = sys.argv[1]
# outputDir = sys.argv[2]
inputFile = 'soc-LiveJournal1Adj.txt'
outputDir = 'social'
users = sc.textFile(inputFile)
users.take(5)

def friend_pairs(line):
    split = line.split()
    user_id = int(split[0])
    if len(split) == 1:
        friends = []
    else:
        friends = list(map(lambda x: int(x), split[1].split(',')))
    return user_id, friends

def user_connected_friends(friendships):
    user_id = friendships[0]
    friends = friendships[1]
    user_connections = []
    for user in friends:
        key = (user_id, user)
        if user_id > user:
            key = (user, user_id)
        user_connections.append((key, 0))
    for pairs in itertools.combinations(friends, 2):
        f1 = pairs[0]
        f2 = pairs[1]
        key = (f1, f2)
        if f1 > f2:
            key = (f2, f1)
        user_connections.append((key, 1))
    return user_connections


def user_friend_recommendation_pairs(mutual):
    mutual_friend_pair = mutual[0]
    mutual_friends_count = mutual[1]
    f1 = mutual_friend_pair[0]
    f2 = mutual_friend_pair[1]
    rec1 = (f1, (f2, mutual_friends_count))
    rec2 = (f2, (f1, mutual_friends_count))
    return [rec1, rec2]

def sort_recommendations(recs):
    recs.sort(key = lambda x: (-int(x[1]), int(x[0])))
    return list(map(lambda x: x[0], recs))[:10]





friendship_pairs = users.map(friend_pairs)
friendship_pairs.take(5)
# friendship_pairs.saveAsTextFile(outputDir)



friend_connections = friendship_pairs.flatMap(user_connected_friends)
friend_connections.take(5)
# [((0, 1), 0), ((0, 2), 0), ((0, 3), 0), ((0, 4), 0), ((0, 5), 0)]; ((friend_pair), 0/1), 0 if connected 1 if not connected but share a mutual friend
# friend_connections.saveAsTextFile(outputDir)

# finds the number of mutual friends between users who are not already friends
mutual_connections = friend_connections.groupByKey().filter(lambda pair: 0 not in pair[1]).map(lambda pair:(pair[0], sum(pair[1])))
mutual_connections.take(3)
# [((20985, 20989), 1), ((4522, 17246), 1), ((47454, 47456), 3)];  ((friend_pair), sum of mutual friends the friend_pair has in common)
mutual_connections.cache()

# get pairs of recommended friends  
recommendations = mutual_connections.flatMap(user_friend_recommendation_pairs)
# recommendations.take(5)
# [(41381, (45545, 1)), (45545, (41381, 1)), (34212, (34364, 19)), (34364, (34212, 19)), (19032, (44828, 1))]; (user_id, (friend_id, mutual friend count between user_id and friend_id))

friend_recommendations = recommendations.groupByKey().map(lambda mf: (mf[0], sort_recommendations(list(mf[1]))))
# friend_recommendations.take(5)
# [(24582, [9607, 13478, 24546, 24686, 35071, 4687, 7506, 7639, 14204, 24530]), (32780, [4425, 32440, 32761, 32775, 1412, 4339, 4408, 4414, 32356, 32394]), ...]; (user_id, [list of 10 recommened friends])

user_ids_recs = friend_recommendations.filter(lambda recs: recs[0] in [924, 8941, 8942, 9019, 9020, 9021, 9022, 9990, 9992, 9993]).sortByKey()
user_ids_recs.take(5)


user_ids_recs.saveAsTextFile(outputDir)
sc.stop()


