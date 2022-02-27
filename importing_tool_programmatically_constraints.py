import json as json
import mysql.connector
from datetime import datetime
import time

cnx = mysql.connector.connect(user='root', password='root',database='reddit_db_w_p')
cursor = cnx.cursor()

def insert_comment(id, body, score, created, parent_id, subreddit_name, link_name, author):
    try:
        sql_statement = ("INSERT INTO reddit_db_w_p.comments "
                "(id, name, body, score, created_utc, parent_id, author,  subreddit_id, link_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
        
        dt_object = datetime.fromtimestamp(int(created))
        sql_data = (id, 't1_' + id ,body, score, dt_object, parent_id, author, subreddit_name, link_name)
        cursor.execute(sql_statement, sql_data)
        #cnx.commit()
    except mysql.connector.errors.IntegrityError:
        pass


def insert_user(author):
    try:
        sql_statement = ("INSERT INTO reddit_db_w_p.users "
                "(author) "
                "VALUES (%s)")
        sql_data = (author,)
        cursor.execute(sql_statement, sql_data)
        #cnx.commit()
    except mysql.connector.errors.IntegrityError:
        pass


def insert_subreddit(subreddit_id, subreddit):
    try:
        temp = subreddit_id.split('_')
        id = temp[1]
        sql_statement = ("INSERT INTO reddit_db_w_p.subreddits "
                "(id, subreddit, name) "
                "VALUES (%s, %s, %s)")
        sql_data = (id, subreddit, subreddit_id)
        cursor.execute(sql_statement, sql_data)
        #cnx.commit()
    except mysql.connector.errors.IntegrityError:
        pass


def insert_link(link_name):
    try:
        temp = link_name.split('_')
        id = temp[1]
        sql_statement = ("INSERT INTO reddit_db_w_p.links "
                "(id, name) "
                "VALUES (%s, %s)")
        sql_data = (id, link_name)

        cursor.execute(sql_statement, sql_data)
        #cnx.commit()
    except mysql.connector.errors.IntegrityError:
        pass
        


def import_json_file(path):
    start = time.time()
    authors = []
    link_ids = []
    subreddit_ids = []
    comments_ids = []
    file = open(path)
    limit = 10000000
    i = 1
    while(i <= limit):
        print(i)
        line = file.readline()
        json_record_dict = json.loads(line)

        # link data
        link_id = json_record_dict['link_id']
        try:
            c = link_ids.index(link_id)
        except ValueError:
            insert_link(link_id)
            link_ids.append(link_id)

        # subreddit data
        subreddit_name = json_record_dict['subreddit_id']
        try:
           c = subreddit_ids.index(subreddit_name)
        except ValueError:
            subreddit = json_record_dict['subreddit']
            insert_subreddit(subreddit_name, subreddit)
            subreddit_ids.append(subreddit_name)

        # user data
        author = json_record_dict['author']
        try:
            c = authors.index(author)
        except ValueError:
            insert_user(author)
            authors.append(author)

        # comment data
        id = json_record_dict['id']
        try:
            c = comments_ids.index(id)
        except ValueError:
            body = json_record_dict['body']
            score = int(json_record_dict['score'])
            parent_id = json_record_dict['parent_id']
            created = json_record_dict['created_utc']
            
            insert_comment(id, body, score, created, parent_id, subreddit_name, link_id, author)
            comments_ids.append(id)
        i = i+1
    
    cnx.commit()
    end = time.time()
    print(end-start)


import_json_file(r'E:\ttt\RC_2011-07.json')

