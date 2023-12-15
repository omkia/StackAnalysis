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


def cleanhtml(raw_html):
    if raw_html is None:
        return None
    pattern = re.compile('[\W_]+')
    cleantext = re.sub(pattern, ' ', raw_html)
    return cleantext.replace('\n', ' ')


def filereader():
    with open("Badges.xml") as file:
        line_counter = 0
        insert_reviewers_query = """
        INSERT INTO badges(
            Id ,
            UserId ,
            Name ,
            dateTag ,
            Class ,
            TagBased 
        )
        VALUES (%s,%s, %s, %s,%s,%s )
        """
        reviewers_records = []
        for line in file:
            line_counter += 1
            if line_counter < 2:
                continue
            if line_counter == 100:
                with mydb.cursor() as cursor:
                    cursor.executemany(insert_reviewers_query, reviewers_records)
                    mydb.commit()
                line_counter = 2
                reviewers_records.clear()
            soup = BeautifulSoup(line, features="html.parser")
            for item in soup.find_all("row"):
                reviewers_records.append(
                    (item["id"], item["userid"], cleanhtml(item["name"]), item["date"], item["class"][0],
                     item["tagbased"]))
        with mydb.cursor() as cursor:
            cursor.executemany(insert_reviewers_query, reviewers_records)
            mydb.commit()


filereader()

mydb.close()
