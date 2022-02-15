import time
import datetime
import random
from flask import Flask, render_template
from threading import Thread
from check_connection import check_connection
from sql_to_html import sql_to_html

app = Flask(__name__)

conn, cursor = check_connection()


def threaded_task(duration, unique_id):
    def insert():
        timestamp = datetime.datetime.now()
        cursor.execute(f"""INSERT INTO tasks("id", "name", "datetime", "description", "status") 
        VALUES (%s, %s, %s, %s, %s);""", (unique_id, 'task_name', timestamp, 'task description', 'IN PROGRESS'))
        conn.commit()

    insert()

    time.sleep(duration)
    pg_update = """Update tasks set status = %s where id = %s"""
    cursor.execute(pg_update, ("FINISHED", unique_id))
    conn.commit()


@app.route("/")
def hello_world():
    tasks = []

    for _ in range(5):
        tasks.append(Thread(target=threaded_task, args=(10, random.randint(10000, 1000000))))

    for task in tasks:
        task.daemon = True
        task.start()

    return f"<h1> {datetime.datetime.now()} </h1>"


@app.route('/taskmaster')
def taskmaster():
    html, prog, fin = sql_to_html("tasks")

    context = {
        "TABLE": html,
        "PROG": prog,
        "FIN": fin
    }

    return render_template("test.html", c=context)


def main():
    app.run(debug=True)


if __name__ == "__main__":
    main()

# create function to create
