import mysql.connector

# mydb = mysql.connector.connect(
#     host="192.168.1.176",
#     user="root",
#     password="4km75m6ea4b27dxsnc84",
#     database="stack",
#     port=9076
# )

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="stackoverflow",
    port=3306
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
    with open("Users.xml") as file:
        line_counter = 0
        insert_reviewers_query = """
        INSERT ignore INTO users(
         id ,
         reputation , 
         creationdate , 
         displayname ,
         lastaccessdate , 
         websiteurl ,
         location , 
         aboutme ,
         views , 
         upvotes , 
         downvotes , 
         accountid  , 
         profileimageurl
        )
        VALUES (%s,%s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s )
        """

        reviewers_records = []
        counter=0
        for line in file:
            counter += 1
            if (counter % 1000000 == 0):
                print(counter)
            # if (counter < 3000000):
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
                # print(item)
                websiteurl = item.get('websiteurl')
                location = item.get('location')
                aboutme = item.get('aboutme')
                accountid = item.get('accountid')
                profileimageurl = item.get('profileimageurl')
                reviewers_records.append(
                    (item["id"], item["reputation"], item["creationdate"], cleanhtml((item["displayname"])),
                     item["lastaccessdate"], websiteurl, cleanhtml((location)), cleanhtml((aboutme)),
                     item["views"], item["upvotes"], item["downvotes"], accountid, profileimageurl))
        with mydb.cursor() as cursor:
            cursor.executemany(insert_reviewers_query, reviewers_records)
            mydb.commit()


filereader()

mydb.close()
