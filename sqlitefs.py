#!/usr/bin/python
# -*- coding: utf-8 -*-
# Date: 07-28-2010
# Author: Joshua Short <joshua.short@gmail.com>
# Description: Class for File System Access to a Sqlite Database

# there are three primary structures in a sqlite database:
STRUCTURE = [
  ('table', "tables"),
  ('index', "indexes"),
  ('trigger', "triggers"),
]

# TODO: implement auto-updating cache
# helper method to represent the structure of a sqlite database as a dictionary
def get_structure(db):
  return dict([
    (display, db.execute("SELECT * FROM sqlite_master WHERE type=?", (type,)).fetchall())
    for type, display
    in STRUCTURE
  ])

# XXX: This will be a show stopper until I upload the required modules
# We can load code directly from source files, treating dictionary as an environment
import system
Filesystem = system.Code("/System/Storage.lib/filesystem.py")['Filesystem']

###############################################################################
# The SqliteFilesystem Class determines how databases are represented.
###############################################################################
import sqlite3
class SqliteFilesystem(Filesystem):
  
	# To create a File System, give the location of a sqlite database
	# If location is omitted (or None), the database will reside in memory
	def __init__(self, location=None):
	    Filesystem.__init__(self)
	    self.db = sqlite3.connect(location or ":memory:", check_same_thread=False)
	    
	# __getitem__ retrieves items from the file system which match the requested path
	def __getitem__(self, path, fh=None):
	  
	    # The Filesystem class provides a simple query processing method: splitting on '/'
	    query = self.process_query(path)
	    
	    # Instead of working with many errors, use this set as a visual error flag
	    response = ["XXX"]

	    # If the root directory was requested, 
	    if path == "/":
	      
		# The response is the display names for the elemental structures
		response = [display for type, display in STRUCTURE]
		
	    # This checks to see if the query is not one of the three allowed values ('tables', 'indexes', 'triggers')
	    elif len([display for type, display in STRUCTURE if display == query[1]]) == 0:
		response = None

	    # The query appears to be vaild, so it gets passed along for further analysis
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