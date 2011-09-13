#!/usr/bin/env python

import pprint
import sys, os
import json
import urllib
import httplib2

class Jmx:
	def __init__(self, host, port, username='', password='', nodetype='cassandra', context='', ssl=0):
		self.host = host
		self.port = port
		self.username = username
		self.password = password
		self.nodetype = nodetype
		self.context = context
		self.ssl = (1 if ssl else 0)

	def getURL(self):
		if self.username and self.password:
			credentials = '%s:%s@' % (self.username, self.password)
			credentials = ''
		else:
			credentials = ''

		protocol = ("https://" if self.ssl else "http://")

		return '%s%s%s:%s%s' % (protocol, credentials, self.host, self.port, self.context)

	def getJson(self, post):
		json_post = json.dumps(post)

		http = httplib2.Http()
		http.add_credentials(self.username, self.password)
		resp, content = http.request(self.getURL(), "POST", json_post)

		if int(resp['status']) == 200:
			return json.loads(content)
		else:
			return resp

	def isNodeActive(self):
		data = {u'type':u'version'}
		
		response = self.getJson(data)
		if type(response).__name__ == 'dict' and response['status'] == 200:
			return 1
		else:
			return response

	def getData(self):
		json_data = open(self.nodetype + ".json")
		data = json.load(json_data)
		for idx, mbean in enumerate(data):
			data[idx]['type'] = "read"
			data[idx]['mbean'] += "*"

		json_data.close()

		return self.getJson(data)

