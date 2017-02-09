import pyhs2
with pyhs2.connect(host='localhost',
                   port=10000,
                   authMechanism="PLAIN",
                   user='hive',
                   password='admin',
                   database='default') as conn:
    with conn.cursor() as cur:
        #Show databases
        print cur.getDatabases()

        #Execute query
        cur.execute("create table if not exists twitter_grippe(ID varchar(255), tweet string, date_month string)")

        #Return column info from query
        print cur.getSchema()

        #Fetch table results
        for i in cur.fetch():
            print i

