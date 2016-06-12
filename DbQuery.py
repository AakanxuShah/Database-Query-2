#!/usr/bin/python2.7
#
# CSE 512
# Author : Aakanxu Shah
#

import psycopg2
import os
import sys
import stat

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):

    cur = openconnection.cursor()
    demolist = []
    
    #  Number of range partition
    
    cur.execute("SELECT COUNT(*) FROM RangeRatingsMetadata")
    rangecount = int(cur.fetchone()[0])

    #Query for range
    for i in range(rangecount):
        demolist.append("SELECT 'rangeratingspart" + str(i) +"' AS tablename, userid, movieid, rating FROM rangeratingspart" + str(i) + " WHERE rating >= " + str(ratingMinValue) + " AND rating <= " + str(ratingMaxValue))

    # Number of round robin partition
    
    cur.execute("SELECT PartitionNum FROM RoundRobinRatingsMetadata")
    roundpartitions = int(cur.fetchone()[0])
    
    # query  round
    for i in range(roundpartitions):
        demolist.append("SELECT 'roundrobinratingspart" + str(i) +"' AS tablename, userid, movieid, rating FROM roundrobinratingspart" + str(i) + " WHERE rating >= " + str(ratingMinValue) + " AND rating <= " + str(ratingMaxValue))

    demoquery = 'SELECT * FROM ({0}) AS T'.format(' UNION ALL '.join(demolist))
    demofile = open('RangeQueryOut.txt', 'w')

    write_file = "COPY (" + demoquery + ") TO '" + os.path.abspath(demofile.name) + "' (FORMAT text, DELIMITER ',')"

    cur.execute(write_file)

    cur.close()
    demofile.close()


def PointQuery(ratingsTableName, ratingValue, openconnection):

    cur = openconnection.cursor()
    demolist = []

    # Get range partition
    cur.execute("SELECT COUNT(*) FROM RangeRatingsMetadata")
    rangecount = int(cur.fetchone()[0])

    #query for range
    for i in range(rangecount):
        demolist.append("SELECT 'rangeratingspart" + str(i) +"' AS tablename, userid, movieid, rating FROM rangeratingspart" + str(i) + " WHERE rating = " + str(ratingValue))

    # Get round robin 
    cur.execute("SELECT PartitionNum FROM RoundRobinRatingsMetadata")
    roundnpartitions = int(cur.fetchone()[0])

    # query for round
    for i in range(roundnpartitions):
        demolist.append("SELECT 'roundrobinratingspart" + str(i) +"' AS tablename, userid, movieid, rating FROM roundrobinratingspart" + str(i) + " WHERE rating = " + str(ratingValue))

    demoquery = 'SELECT * FROM ({0}) AS T'.format(' UNION ALL '.join(demolist))

    demofile = open('PointQueryOut.txt', 'w')

    write_file = "COPY (" + demoquery + ") TO '" + os.path.abspath(demofile.name) + "' (FORMAT text, DELIMITER ',')"

    cur.execute(write_file)

    cur.close()
    demofile.close()

# Disclaimer for students : Be Original and Do not plagiarize ! :)
