import json
import os
import time
import traceback

import psycopg2

conn = psycopg2.connect(
    host="34.124.182.95", database="article_data", user="ai_user", password="{{PASSWORD}}"
)
cur = conn.cursor()
cur.execute("SELECT url from article_raw_data")
result = cur.fetchall()
inserted_list = []
for r in result:
    inserted_list.append(r[0])

path_list = [
    "beauty/celeb",
    "beauty/hair",
    "beauty/lifestyle",
    "beauty/makeup",
    "beauty/review",
    "fashion",
    "wongnai",
]

for path in path_list:
    for filename in os.listdir(path):
        f = open(os.path.join(path, filename))
        data = json.load(f)
        print(os.path.join(path, filename))
        f.close()
        try:
            if data["url"] not in inserted_list:
                cur.execute("BEGIN")
                cur.execute(
                    "INSERT INTO article_raw_data (url, category, title, content, paragraphs, raw_html, raw_json, remark) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        data["url"],
                        data["category"],
                        data["title"],
                        "",
                        json.dumps(data["paragraphs"], ensure_ascii=False),
                        data["raw_html"],
                        "{}",
                        "import_db script from Ink's v_data_processed path:" + path,
                    ),
                )
                cur.execute("COMMIT")
                inserted_list.append(data["url"])
        except psycopg2.Error as e:
            conn.rollback()
            print("Error: {}".format(e))

conn.commit()
cur.close()
conn.close()
