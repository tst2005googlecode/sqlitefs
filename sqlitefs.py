#!/usr/bin/python
# -*- coding: utf-8 -*-


# The structure, or entities, of a a sqlite database:
STRUCTURE = ('table', 'index', 'trigger')

# helper method for representing a database's schema as a dictionary
# TODO: implementing caching which auto-updates when structure changes
def get_structure(db):
  return dict([
    (type, db.execute("SELECT * FROM sqlite_master WHERE type=?", (type,)).fetchall())
    for type
    in STRUCTURE
  ])


import sqlite3
import system
Filesystem = system.Code("/System/Storage.lib/filesystem.py")['Filesystem']
class SqliteFilesystem(Filesystem):
  
	def __init__(self, location=None):
	    Filesystem.__init__(self)
	    self.db = sqlite3.connect(location or ":memory:", check_same_thread=False)
		
	def __getitem__(self, path, fh=None):
	    query = self.process_query(path)
	    response = ["XXX"]

	    # /
	    if path == "/":
		response = [item for item in STRUCTURE]
		
	    elif query[1] not in STRUCTURE:
		response = None

	    else:
		if len(query) == 2:
			response = [str(name) for type, name, target, id, string in get_structure(self.db)[query[1]]]

		elif len(query) == 3:
			response = [str(item[0]) for item in self.db.execute("SELECT rowid FROM %s" % (query[2]))]

		elif len(query) == 4:
			response = []
			if self.db.execute("SELECT COUNT(*) FROM %s WHERE rowid=?" % (query[2]), (query[3],)).fetchone() > 0:
			  for item in get_structure(self.db)[query[1]]:
			    if item[1] == query[2]:
			      for field in item[4][item[4].find('('):][1:-1].split(','):
				if len(str(field).strip().split(' ')) > 1:
				  response.append(str(field).strip().split(' ')[0])
			
		elif len(query) == 5:
			response = []
			for value in self.db.execute("SELECT %s FROM %s WHERE rowid=?" % (query[4], query[2]), (query[3])):
			  response.append(str(value[0]))
		
	    return response


import sys
if __name__ == "__main__":
  SqliteFilesystem(sys.argv[1]).attach(sys.argv[2])