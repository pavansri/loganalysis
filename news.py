#! /usr/bin/env python

import psycopg2

DBNAME = "news"


def run(query):
    """Connects to the database, runs the query passed to it,
    and returns the results"""
    db = psycopg2.connect('dbname=' + DBNAME)
    c = db.cursor()
    c.execute(query)
    row = c.fetchall()
    db.close()
    return row

query_1 = ("SELECT title, count(*) as views FROM articles \n"
           "  JOIN log\n"
           "    ON articles.slug = substring(log.path, 10)\n"
           "    GROUP BY title ORDER BY views DESC LIMIT 3;")

query_2 = ("SELECT authors.name, count(*) as views\n"
           "    FROM articles \n"
           "    JOIN authors\n"
           "      ON articles.author = authors.id \n"
           "      JOIN log \n"
           "      ON articles.slug = substring(log.path, 10)\n"
           "      WHERE log.status LIKE '200 OK'\n"
           "      GROUP BY authors.name ORDER BY views DESC LIMIT 3;")


query_3 = """
        SELECT total.day,
          ROUND(((errors.error_requests*1.0) / total.requests), 3) AS percent
        FROM (
          SELECT date_trunc('day', time) "day", count(*) AS error_requests
          FROM log
          WHERE status LIKE '404%'
          GROUP BY day
        ) AS errors
        JOIN (
          SELECT date_trunc('day', time) "day", count(*) AS requests
          FROM log
          GROUP BY day
          ) AS total
        ON total.day = errors.day
        WHERE (ROUND(((errors.error_requests*1.0) / total.requests), 3) > 0.01)
        ORDER BY percent DESC;
    """


def top_articles():
    """Returns top 3 most read articles"""

    results = run(query_1)

    # Print Results
    print('\nTOP THREE ARTICLES:')
    count = 1
    for i in results:
        title = i[0]
        views = '" with ' + str(i[1]) + " views"
        print(title + views)
        count += 1


def top_authors():
    """returns top 3 most popular authors"""

    results = run(query_2)

    # Print Results
    print('\nTOP THREE AUTHORS:')
    for i in results:
        print(i[0] + ' with ' + str(i[1]) + " views")


def days_with_errors():
    """returns days with more than 1% errors"""

    results = run(query_3)

    # Print Results
    print('\nDAYS WITH MORE THAN 1% ERRORS:')
    for i in results:
        date = i[0].strftime('%B %d, %Y')
        errors = str(round(i[1]*100, 1)) + "%" + " errors"
        print(date + "  " + errors)

print('Calculating\n')
top_articles()
top_authors()
days_with_errors()
