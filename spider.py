from BeautifulSoup import *
import sqlite3
import urllib
from urlparse import urljoin
from urlparse import urlparse

######  Create a Sqlite Database

conn = sqlite3.connect('spider.sqlite')
cur = conn.cursor()


######  Create tables: Pages,Links,Web 

cur.execute('''CREATE TABLE IF NOT EXISTS Pages 
    (id INTEGER PRIMARY KEY, url TEXT UNIQUE, html TEXT, 
     error INTEGER, old_rank REAL, new_rank REAL)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Links 
    (from_id INTEGER, to_id INTEGER, UNIQUE(from_id, to_id))''')

cur.execute('''CREATE TABLE IF NOT EXISTS Webs (url TEXT UNIQUE)''')


######  Enter at least one entry  

cur.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
row = cur.fetchone()

if row != None:
    print "Restarting existing crawl."
else :
    starturl = raw_input('Enter web url: ')
    if ( len(starturl) < 1 ) : starturl = 'http://www.bloomberg.com'
    if ( starturl.endswith('/') ) : starturl = starturl[:-1]
    web = starturl
    if ( starturl.endswith('.htm') or starturl.endswith('.html') ) :
        pos = starturl.rfind('/')
        web = starturl[:pos]

    if ( len(web) > 1 ) :
        cur.execute('INSERT OR IGNORE INTO Webs (url) VALUES ( ? )', ( web, ) )
        cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', ( starturl, ) ) 
        conn.commit()


######  Get the start web url

cur.execute('''SELECT url FROM Webs''')
webs = list()

for row in cur:
    webs.append(str(row[0]))

many = 100
while many>1:
	many = many - 1

	cur.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
	try:
		row = cur.fetchone()
		print row
		fromid = row[0]
		url = row[1]
	except:
		print 'No unretrieved pages found'
		many = 0
		break

	try:
		document = urllib.urlopen(url)
		html = document.read()
		if document.getcode() != 200:
			print "Error : ", document.getcode()
			cur.execute('UPDATE Pages SET error = ? WHERE url = ?',(document.getcode(),url,))

		if document.info().gettype() != 'text/html':
			print "Ignore non-text page"
			curr.execute('UPDATE Pages SET error = -1 WHERE url=?',(url, ))
			conn.commit()
			continue

		soup = BeautifulSoup(html) 

	except KeyboardInterrupt:
		print ''
		print 'Interrupted by user...'
		break

	except:
		print "Unable to retrieve or parse page"
		cur.execute('UPDATE Pages SET error=-1 WHERE url=?', (url, ) )
		conn.commit()
		continue

	tags = soup('a')

	count = 0


######  Recursively get all the link in the page

	for tag in tags:
		href = tag.get('href', None)

		if ( href is None ) : continue

		# Resolve relative references
		up = urlparse(href)
		if ( len(up.scheme) < 1 ) :
			href = urljoin(url, href)
		ipos = href.find('#')
		if ( ipos > 1 ) : href = href[:ipos]
		if ( href.endswith('.png') or href.endswith('.jpg') or href.endswith('.gif') ) : continue
		if ( href.endswith('/') ) : href = href[:-1]

		# print href
		if ( len(href) < 1 ) : continue

		# Check if the URL is in any of the webs
		found = False
		for web in webs:
			if ( href.startswith(web) ) :
				found = True
				break
		if not found : continue

		cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', ( href, ) )
		cur.execute('UPDATE Pages SET html=? WHERE url=?', (buffer(html), url ) ) 
		count = count + 1
		# conn.commit()

		cur.execute('SELECT id FROM Pages WHERE url=? LIMIT 1', ( href, ))
		try:
			row = cur.fetchone()
			toid = row[0]
		except:
			print 'Could not retrieve id'
			continue

		#insert fromid, toid into 
		cur.execute('INSERT OR IGNORE INTO Links (from_id, to_id) VALUES ( ?, ? )', ( fromid, toid ) ) 

	conn.commit()
cur.close()