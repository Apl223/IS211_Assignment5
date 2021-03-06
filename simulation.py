import random
import urllib.request
import csv
import time
import argparse
from io import StringIO

class Queue:
	def __init__(self):
		self.items = []
	def is_empty(self):
		return self.items == []
	def enqueue(self, item):
		self.items.insert(0,item)
	def dequeue(self):
		return self.items.pop()
	def size(self):
		return len(self.items)

class Server:
	def __init__(self):
		self.current_task = None
		self.time_remaining = 0
		self.queue = Queue()
	def tick(self):
		if self.current_task != None:
			self.time_remaining = self.time_remaining - 1
			if self.time_remaining <= 0:
				self.current_task = None
	def busy(self):
		if self.current_task != None:
			return True
		else:
			return False
	def start_next(self, new_task):
		self.current_task = new_task
		self.time_remaining = new_task.get_length()

class Request:
	def __init__(self,time,length):
		self.timestamp = time
		self.length = int(length)
	def get_stamp(self):
		return self.timestamp
	def get_length(self):
		return self.length
	def wait_time(self,current_time):
		return current_time - self.timestamp

def simulateOneServer(filename):
	#decode file obtained from URL
	data = urllib.request.urlopen(filename).read().decode("ascii","ignore") 
	d = StringIO(data)
	#create a list based off of what was read
	listdata = list(csv.reader(d))
	t_list = []
	for row in listdata:
		#append each request in the first column to t_list while converting to int
		t_list.append(int(row[0]))
		#convert first column to int
		row[0] = int(row[0])
		#convert 3rd column to int
		row[2] = int(row[2])
	#max value of the first column
	t_max = max(t_list)	

	#objects for later use
	lab_server = Server()
	request_queue = Queue()

	waiting_times = []

	t = 0
	i = 0
	
	while (i < len(listdata)):
		#if the counter is equal to t, start the timer
		if(listdata[i][0] == t):
			current_time = time.time()
			#passing values to the functions in this class (time,length)
			task = Request(current_time,listdata[i][2])
			#inserting the matched iteration of i
			request_queue.enqueue(task)
			#continue to the next value
			i = i+1
		else:
			#if it doesn't, increase t by 1 and check again
			lab_server.tick()
			t = t + 1
		if((not lab_server.busy()) and (not request_queue.is_empty())):
			new_task = request_queue.dequeue()
			current_time = time.time()
			waiting_times.append(new_task.wait_time(current_time))
			lab_server.start_next(new_task)
	avg_wait_time = sum(waiting_times)/len(waiting_times)
	print ("Average wait time for a single server:")
	print (avg_wait_time, "seconds")
	print (avg_wait_time * 1000, "milliseconds")
	print (avg_wait_time * 1000000, "microseconds")
	print ("Number of requests:", len(waiting_times))

def simulateManyServers(filename,num_servers):
	data = urllib.request.urlopen(filename).read().decode("ascii","ignore") 
	d = StringIO(data)
	listdata = list(csv.reader(d))
	t_list = []
	for row in listdata:
		t_list.append(int(row[0]))
		row[0] = int(row[0])
		row[2] = int(row[2])
	
	t_max = max(t_list)	
	server_list = []

	for i in range(num_servers):
		server_list.append(Server())
	

	waiting_times = []

	t = 0
	i = 0
	
	while (i < len(listdata)):
		if(listdata[i][0] == t):
			current_time = time.time()
			task = Request(current_time,listdata[i][2])
			if i == 0:
				server_iter = 0
			else:
				server_iter = i % num_servers 
			
			server_list[server_iter].queue.enqueue(task)
			i = i+1
		else:
			for row in server_list:
				row.tick()
			t = t + 1
		for lab_server in server_list:
			if((not lab_server.busy()) and (not lab_server.queue.is_empty())):
				new_task = lab_server.queue.dequeue()
				current_time = time.time()
				waiting_times.append(new_task.wait_time(current_time))
				lab_server.start_next(new_task)
	avg_wait_time = sum(waiting_times)/len(waiting_times)
	print("Average wait time for", num_servers, "servers")
	print(avg_wait_time, "seconds")
	print(avg_wait_time * 1000, "milliseconds")
	print(avg_wait_time * 1000000, "microseconds")
	print("Number of requests:", len(waiting_times))

def main(url,servers):
	print(f"Running main with URL = {url}...")

	if servers == 1 or servers is None:
		simulateOneServer(url)
	else:
		print("Many Queues:")
		simulateManyServers(url,servers)

if __name__ == "__main__":
	#http://s3.amazonaws.com/cuny-is211-spring2015/requests.csv'
	parser = argparse.ArgumentParser()
	parser.add_argument("--servers",type = int, help = "Number of servers to simulate")
	parser.add_argument("--url", type = str, help = "URL of file to process")
	args = parser.parse_args()
	num_servers = args.servers
	main(args.url,args.servers)
