#!/usr/bin/python

import argparse
import sys, os
import json
import pprint
from jmx import Jmx
import signal

class JmxQuery(Jmx):
	def getData(self, mbeans, attr):
		json_query = []
		for mbean in mbeans:
			mbean_data = {u'type':u'read', u'mbean':mbean+'*', u'attribute':attr}
			json_query.append(mbean_data)

		return self.getJson(json_query)

def sig_handler(signum, frame):
	print "timeout"
	sys.exit(1)

def main():
	mbean, attr = getMbeans(args.mbeantype, args.keyspace, args.columnfamily)
	jmx = JmxQuery(args.host, args.port, args.username, args.password, context=args.context)

	state = jmx.isNodeActive()
	if state == 1:
		signal.alarm(args.timeout)
		jmx_data = jmx.getData(mbean, attr)
		signal.alarm(0)

		for mbean_result in jmx_data:
			if mbean_result['status'] != 200:
				pprint.pprint(mbean_result)
				sys.exit(1)

			for mbean, mbean_value in mbean_result['value'].items():
				for attribute, attribute_value in mbean_value.items():
					if type(attribute_value).__name__ == 'dict':
						for path, path_value in attribute_value.items():
							print "%s_%s:%s" % (attribute, path, path_value),
					elif args.mbeantype == 'Stages':
						prefix = mbean.split('=',1)[1]
						print "%s_%s:%s" % (prefix, attribute, attribute_value),
					else:
						print "%s:%s" % (attribute, attribute_value),
	else:
		print state


def getMbeans(mbeanType='ColumnFamily', ks='', cf=''):
	
	if mbeanType == 'ColumnFamily':
		mbean = ["org.apache.cassandra.db:columnfamily="+cf+",keyspace="+ks+",type=ColumnFamilies"]
		attr = ["BloomFilterFalsePositives","BloomFilterFalseRatio","LiveDiskSpaceUsed",
				"LiveSSTableCount","MaxRowSize","MeanRowSize","MemtableColumnsCount",
				"MemtableDataSize","MemtableSwitchCount","MinRowSize","ReadCount",
				"TotalDiskSpaceUsed","TotalReadLatencyMicros","TotalWriteLatencyMicros","WriteCount"]
	elif mbeanType == 'Caches':
		mbean = ["org.apache.cassandra.db:cache="+cf+"KeyCache,keyspace="+ks+",type=Caches"]
		attr = ["Capacity","Hits","Requests","Size"]
	elif mbeanType == 'Commitlog':
		mbean = ["org.apache.cassandra.db:type=Commitlog"]
		attr = ["ActiveCount","PendingTasks"]
	elif mbeanType == 'ColumnFamilyStores':
		mbean = ["org.apache.cassandra.db:columnfamily="+cf+",keyspace="+ks+",type=ColumnFamilies"]
		attr = ["LiveDiskSpaceUsed","LiveSSTableCount","MaxRowSize","MeanRowSize","MemtableColumnsCount",
				"MemtableDataSize","MemtableSwitchCount","PendingTasks","ReadCount","RecentReadLatencyMicros",
				"RecentWriteLatencyMicros","TotalDiskSpaceUsed","TotalReadLatencyMicros",
				"TotalWriteLatencyMicros","WriteCount"]
	elif mbeanType == "DroppedMessages":
		mbean = ["org.apache.cassandra.net:type=MessagingService"]
		attr = ["DroppedMessages","RecentlyDroppedMessages"]
	elif mbeanType == "Compactions":
		mbean = ["org.apache.cassandra.db:type=CompactionManager"]
		attr = ["PendingTasks"]
	elif mbeanType == "CpuTime":
		mbean = ["java.lang:type=Threading"]
		attr = ["ThreadCount","CurrentThreadCpuTime","CurrentThreadUserTime"]
	elif mbeanType == "Memory":
		mbean = ["java.lang:type=Memory"]
		attr = ["NonHeapMemoryUsage","HeapMemoryUsage"]
	elif mbeanType == "OSMemory":
		mbean = ["java.lang:type=OperatingSystem"]
		attr = ["FreePhysicalMemorySize","CommittedVirtualMemorySize"]
	elif mbeanType == "Stages":
		mbean = ["org.apache.cassandra.internal:type=AntiEntropyStage",
				"org.apache.cassandra.internal:type=FlushSorter",
				"org.apache.cassandra.internal:type=FlushWriter",
				"org.apache.cassandra.internal:type=GossipStage",
				"org.apache.cassandra.internal:type=HintedHandoff",
				"org.apache.cassandra.internal:type=InternalResponseStage",
				"org.apache.cassandra.internal:type=MemtablePostFlusher",
				"org.apache.cassandra.internal:type=MigrationStage",
				"org.apache.cassandra.internal:type=MiscStage",
				"org.apache.cassandra.internal:type=StreamStage",
				"org.apache.cassandra.request:type=MutationStage",
				"org.apache.cassandra.request:type=ReadRepairStage",
				"org.apache.cassandra.request:type=ReadStage",
				"org.apache.cassandra.request:type=ReplicateOnWriteStage",
				"org.apache.cassandra.request:type=RequestResponseStage"]
		attr = ["ActiveCount","PendingTasks"]

	return mbean, attr


if __name__ == "__main__":
	parser = argparse.ArgumentParser("Cassandra Cacti Poller")
	parser.add_argument('-H', '--host', dest='host', required=True, help='Cassandra Host')
	parser.add_argument('-P', '--port', dest='port', type=int, default=8778, help='Jolokia Port')
	parser.add_argument('-C', '--context', dest='context', default='/', help='Jolokia Context')
	parser.add_argument('-u', '--user', dest='username', help='Username')
	parser.add_argument('-p', '--pass', dest='password', help='Password')
	parser.add_argument('-m', '--mbean', dest='mbeantype', help='Mbeans (ColumnFamiliy, Caches, Commitlog, ColumnFamilyStores, DroppedMessages, Compactions, CpuTime, Memory, OSMemory, Stages')
	parser.add_argument('-ks', '--keyspace', dest='keyspace', help='Keyspace')
	parser.add_argument('-cf', '--columnfamily', dest='columnfamily', help='ColumnFamily')
	parser.add_argument('-t', '--timeout', dest='timeout', type=int, default=5, help='Request Timeout')

	args = parser.parse_args()

	signal.signal(signal.SIGALRM, sig_handler)
	
	main()
