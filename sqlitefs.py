#!/usr/bin/python
# -*- coding: utf-8 -*-
# Date: 07-28-2010
# Author: Joshua Short <joshua.short@gmail.com>
# Description: Class for File System Access to a Sqlite Database

# This source file is under heavy construction.
# Watch you head; what once was may no longer be.


###############################################################################
# Completed Functionality
###############################################################################
# In its current state, the following features work:
#  viewing database structure:
#    $ ls /
#    $ ls /tables
#    $ ls /tables/<table_name>
#    $ ls /tables/<table_name>/<rowid>
#    $ ls /tables/<table_name>/<rowid>/<column_name>



# The current construction backlog:
#  removing items from a table:
#    $ rm /tables/<table_name>/<rowid>
#  removing tables from a database:
#    $ rm /tables/<table_name>
#  allow tables to be viewed by rows or columns:
#    $ ls /tables/rows/<rowid>/<column_name>
#    $ ls /tables/columns/<column_name>/<rowid>


# This module has a light dependency on the system's ability to load code
from system import Code


# there are three types of primary structures found in a sqlite database:
STRUCTURES = [
  # type  , display_name
  ('table', "tables"),
  ('index', "indexes"),
  ('trigger', "triggers"),
]


# this method represents the structure of a sqlite database as a dictionary
# TODO: implement as auto-updating cache
def get_structure(db):
  return dict([
    (display, db.execute("SELECT * FROM sqlite_master WHERE type=?", (type,)).fetchall())
    for type, display
    in STRUCTURES
  ])


# paths here are /-delimited
process_query = lambda path: path and path.split('/')


###############################################################################
# The SqliteFilesystem Class determines how databases are represented.
###############################################################################
Filesystem = Code("/System/Storage.lib/filesystem.py")['Filesystem']
import sqlite3
class SqliteFilesystem(Filesystem):
  
  # To create a File System, give the location of a sqlite database
  # if the location is omitted (or None), the database will reside in memory
  def __init__(self, location=None):
    Filesystem.__init__(self)

    # Open a connection to a sqlite file -- allow access by separate threads
    self.db = sqlite3.connect(location or ":memory:", check_same_thread=False)
    self.db.text_factory = lambda x: x.encode("utf-8")


  # XXX: fuse barfs with this ... need to restructure ... slightly
  #def __call__(self, path):
  #  print "Call: %s" % path

  # __getitem__ retrieves items from the file system which match the requested path
  def __getitem__(self, path, fh=None):
    
    # The Filesystem class provides a simple query processing method: splitting on '/'
    query = process_query(path)
    
    # this will be *TEMPORARILY* used as an error flag
    response = ["XXX"]

    # the root directory was requested
    if path == "/":
      
      # respond with the display names for the primary structures
      response = [name for type, name in STRUCTURES]
	
    # we can eliminate 'impossible' queries if they're not for an primary structure
    elif len([name for type, name in STRUCTURES if name == query[1]]) == 0:
      pass#response = None

    # the query appears to be vaild, so it gets analyzed further
    else:
      
      # the query is of type /<feature>
      if len(query) == 2:
	
	# for now, switch in this clunky way
	if query[1] == "indexes":
	  response = [str(name) for type, name, target, id, string in get_structure(self.db)[query[1]]]
	elif query[1] == "tables":
	  response = [str(name) for type, name, target, id, string in get_structure(self.db)[query[1]]]
	elif query[1] == "triggers":
	  response = [str(name) for type, name, target, id, string in get_structure(self.db)[query[1]]]


      # the query is of type /<feature>/<object>
      elif len(query) == 3:
	
	# currently, the only meaningful feature is 'tables'
	# TODO: change this to include the rows/columns views
	response = [str(item[0]) for item in self.db.execute("SELECT rowid FROM %s" % (query[2]))]


      # the query is of type /<feature>/<object>/<value>
      elif len(query) == 4:
	
	# the only <object> this works for is 'tables'
	# unfortunately, indexes can't be viewed (though a query could be constructed of the appropriate table)
	# this means we're getting something like /tables/<table_name>/<rowid>
	# and returning the columns that this rowid has
	response = []
	
	# TODO: first, should check to see that the requested table exists
	# first, make sure the given rowid exists in the table
	if self.db.execute("SELECT COUNT(*) FROM %s WHERE rowid=?" % (query[2]), (query[3],)).fetchone() > 0:
	  for item in get_structure(self.db)[query[1]]:
	    
	    # make sure the requested table exists
	    if item[1] == query[2]:
	      
	      # find all the column_names
	      for field in item[4][item[4].find('('):][1:-1].split(','):
		
		# a valid definition will be of name TYPE
		if len(str(field).strip().split(' ')) > 1:
		  
		  # add the column to the list of 'files'
		  response.append(str(field).strip().split(' ')[0])
	      
      # the query is of type /<feature>/<object>/<value>/<column>
      elif len(query) == 5:
	# this is another query which only seems to apply to tables
	# /tables/<table_name>/<rowid>/<column_name>
	response = []
	
	# the response is the value of the requested column
	# TODO: fix to work with row/column paradigm
	# TODO: make the column name a file which has the value of the column field
	for value in self.db.execute("SELECT %s FROM %s WHERE rowid=?" % (query[4], query[2]), (query[3],)):
	  response.append(str(value[0]))
      
    # return whatever files were rounded up
    # NOTE: might be best to make an actual error condition (ENOENT) and return within branches
    return response


import sys
if __name__ == "__main__":
  SqliteFilesystem(sys.argv[1]).attach(sys.argv[2])
