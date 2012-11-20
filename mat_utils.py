import psycopg2

def fetch_streamid(uuid):
    conn = psycopg2.connect(database="archiver", host="localhost", 
                            user="archiver", password="password")
    cur = conn.cursor()
    cur.execute("SELECT * FROM stream where uuid = '" + uuid + "';")
    return cur.fetchone()[0]

