import psycopg2
import aiosql
import datetime
import datefinder
import csv
# db-related configuration
conn = psycopg2.connect("")
stmts = aiosql.from_path("dql.sql", "psycopg2")

# sanity date range checking
# why is this necessary?
# datefinder.find_dates() has a strict argument that defualts to False
# if strict=True, it finds no dates
# if strict=False, it finds real dates and lots of false positives
# most of the false positivies will false outside the range of the collection
start_date_str = '1997-01-01'
end_date_str = '2016-12-31'
start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date()
end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date()


datelist = []
for index, id_rec in enumerate(stmts.get_ids(conn=conn)):
    id = id_rec[0]
    head_body = stmts.get_head_body(conn=conn, doc_id=id)[0][0]
    # datefind fails if DATE: precedes a date!
    head_body_list = [w for w in head_body.split() if w != 'DATE:']
    head = ' '
    head = head.join(head_body_list)
    matches = datefinder.find_dates(head)
    # take the first date that's in the sanity date range
    for match in matches:
        doc_date = match.date()
        if start_date <= doc_date <= end_date:
            row = (id, doc_date)
            print(index, row)
            datelist.append(row)
            break

# write the list
outfile = "datefind.csv"
fields = ['id', 'date']
with open(outfile, 'w') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(fields)
    csvwriter.writerows(datelist)
