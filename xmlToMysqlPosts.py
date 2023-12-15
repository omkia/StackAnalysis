import mysql.connector

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="stackoverflow"
)

from bs4 import BeautifulSoup
import re


def cleanfile(fileName):
    f = open(fileName, 'w')
    f.close()


import re

# as per recommendation from @freylis, compile once only
CLEANR = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')


def cleanhtml(raw_html):
    if raw_html is None:
        return None
    cleantext = re.sub(CLEANR, '', raw_html)
    return cleantext.replace('\n', ' ')


def filereader():
    with open("Posts.xml") as file:
        line_counter = 0
        insert_reviewers_query = """
        INSERT ignore INTO posts(
    id , 
    posttypeid,
    acceptedanswerid,
    creationdate ,
    score,
    parentid,
    viewcount,
    owneruserid,
    lasteditoruserid,
    lasteditordisplayname, 
    lasteditdate,
    lastactivitydate,
    tags,
    answercount,
    commentcount, 
    favoritecount,
    communityowneddate,
    hascode,
    hasimage,
    haslink,
    wordcount
        )
        VALUES (%s,%s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s  )
        """
        reviewers_records = []
        counter = 0
        for line in file:
            counter += 1
            if (counter % 1000000 == 0):
                print(counter)
            # if(counter<34000000):
            #     continue
            line_counter += 1
            if line_counter < 2:
                continue
            if line_counter == 1000:
                with mydb.cursor() as cursor:
                    cursor.executemany(insert_reviewers_query, reviewers_records)
                    mydb.commit()
                line_counter = 2
                reviewers_records.clear()
            soup = BeautifulSoup(line, features="html.parser")
            for item in soup.find_all("row"):

                acceptedanswerid = item.get('acceptedanswerid')
                viewcount = item.get('viewcount')
                communityowneddate = item.get('communityowneddate')
                lasteditordisplayname = item.get('lasteditordisplayname')
                answercount = item.get('answercount')
                parentid = item.get('parentid')
                favoritecount = item.get('favoritecount')
                owneruserid = item.get('owneruserid')
                lasteditoruserid = item.get('lasteditoruserid')
                lasteditdate = item.get('lasteditdate')
                hascode = ("</code>") in item.get('body')
                hasimage = ("img src=\"") in item.get('body')
                haslink = ("<a href=\"") in item.get('body')
                wordcount = len(item.get('body'))
                tags = item.get('tags')
                if (tags != None):
                    tags = (tags.replace("<", "").replace(">", ";"))
                reviewers_records.append((item["id"], item["posttypeid"], acceptedanswerid
                                          , item["creationdate"], item["score"], parentid, viewcount
                                          , owneruserid, lasteditoruserid,
                                          None, lasteditdate,
                                          item["lastactivitydate"], tags
                                          , answercount, item["commentcount"], favoritecount,
                                          communityowneddate, hascode, hasimage, haslink, wordcount))
        with mydb.cursor() as cursor:
            cursor.executemany(insert_reviewers_query, reviewers_records)
            mydb.commit()


filereader()

mydb.close()

# record["Title"] = cleanhtml(html.unescape(item["title"])).replace('\n', ' ')
# record["ContentLicense"] = item["contentlicense"]
# record["Body"] = cleanhtml(html.unescape(item["body"])).replace('\n', ' ')
