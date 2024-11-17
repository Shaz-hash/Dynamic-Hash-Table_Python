from cgitb import lookup
from json import dumps, loads
from operator import truediv
import socket 
import threading
import os
import time
import hashlib


class Node:
	def __init__(self, host, port):
		self.stop = False
		self.host = host
		self.port = port
		self.M = 16
		self.N = 2**self.M
		self.key = self.hasher(host+str(port))
		# You will need to kill this thread when leaving, to do so just set self.stop = True
		threading.Thread(target = self.listener).start()
		threading.Thread(target=self.ping , args=() ).start()
		self.files = []
		self.backUpFiles = []
		if not os.path.exists(host+"_"+str(port)):
			os.mkdir(host+"_"+str(port))   ## file directory will be like : host+"_"+str(port)+/+filename
		'''
		------------------------------------------------------------------------------------
		DO NOT EDIT ANYTHING ABOVE THIS LINE
		'''
		# Set value of the following variables appropriately to pass Intialization test
		self.successor =   tuple([self.host , self.port])
		self.predecessor =   tuple([self.host , self.port])
		
		# additional state variables

		self.my_details = tuple([self.host , self.port])
		self.my_files = []

	
	def hasher(self, key):
		'''
		DO NOT EDIT THIS FUNCTION.
		You can use this function as follow:
			For a node: self.hasher(node.host+str(node.port))
			For a file: self.hasher(file)
		'''
		return int(hashlib.md5(key.encode()).hexdigest(), 16) % self.N


	def lookup (self , type , sender_details) :
		
		if (type =="join"):

			success_hash = self.hasher( self.successor[0] + str(self.successor[1]))
			my_hash = self.hasher( self.my_details[0] + str(self.my_details[1]))
			sender_hash = self.hasher (sender_details[0] + str(sender_details[1]))

			#print("my hash : " , my_hash , " successor hash : " , success_hash , "  sender_hash : ", sender_hash , "sender details : ", sender_details)

			
			## Creating edge case when there's only a single node in the list :

			if (self.successor == (self.host , self.port) and self.predecessor == (self.host , self.port) ):

				return True 

			## comparing my successor's hash and the node's hash :

			## 1) if the hash is between my sucessor and me as in hash < successor and hash > me :

			if (my_hash < sender_hash and sender_hash < success_hash):

				#print("in option 1 : " , sender_details , sender_hash)
				return True ## indicating keh this i am the one to start the stuff with 

			
			## 2) if my successor < me and hash is > me and hash > me :
			elif (success_hash < my_hash and sender_hash > my_hash):
				#print("in option 2 : " , sender_details , sender_hash)
				return True

			## 3) if my successor < me and hash is < successor :

			elif (success_hash < my_hash and sender_hash < success_hash):
				#print("in option 3 : " , sender_details , sender_hash)
				return True
			
			else :

			## else i will pass the details to my successor with same join message via the socket with my successor :

				sender_details = tuple(sender_details)
				soc = socket.socket()
				soc.connect(self.successor)
				msg = dumps(["join" , sender_details])
				#print(msg)
				soc.send(msg.encode())

				return None 
		
		elif (type =="file") :

			## in this case sender details will have the address of the node who want's to store/put the file in the ring :
			## sender_details = [file_name , (sender_address , port)]

			file_name = sender_details[0]

			file_hash = self.hasher(file_name)
			success_hash = self.hasher( self.successor[0] + str(self.successor[1]))
			my_hash = self.hasher( self.my_details[0] + str(self.my_details[1]))

			print("my hash : " , my_hash , " successor hash : " , success_hash , "  file_hash : ", file_hash , "file details : ", sender_details)


			## Creating edge case when there's only a single node in the list :

			if (self.successor == (self.host , self.port) and self.predecessor == (self.host , self.port) ):

				return True 

			## comparing my successor's hash and the node's hash :

			## 1) if the hash is between my sucessor and me as in hash < successor and hash > me :

			if (my_hash < file_hash and file_hash < success_hash):

				print("in option 1 : " , sender_details , file_hash , " my hash : ", my_hash)
				return True ## indicating keh this i am the one to start the stuff with 

			
			## 2) if my successor < me and hash is > me and hash > me :
			elif (success_hash < my_hash and file_hash > my_hash):
				print("in option 2 : " , sender_details , file_hash," my hash : ", my_hash)
				return True

			## 3) if my successor < me and hash is < successor :

			elif (success_hash < my_hash and file_hash < success_hash):
				print("in option 3 : " , sender_details , file_hash, " my hash : " , my_hash)
				return True
			
			else :

			## else i will pass the details to my successor with same join message via the socket with my successor :

				sender_details = tuple(sender_details)
				soc = socket.socket()
				soc.connect(self.successor)
				msg = dumps(["file" , sender_details])
				#print(msg)
				soc.send(msg.encode())

				return None 


	def check_my_worthiness (self , file_name):

		file_hash = self.hasher(file_name)
		presuccess_hash = self.hasher( self.predecessor[0] + str(self.predecessor[1]))
		my_hash = self.hasher( self.my_details[0] + str(self.my_details[1]))


		if (my_hash > file_hash) and (file_hash < presuccess_hash):
			return True

		elif (my_hash < presuccess_hash) and (my_hash > presuccess_hash):
			return True

		elif (my_hash < presuccess_hash) and (my_hash < presuccess_hash ):
			return True

		else :
			return None
		


		

	# FUNCTION USED TO FIX NODE IN THE RING :
	def adjust_new_node(self , sender_details_tuple):


		if (self.successor == (self.host , self.port) and self.predecessor == (self.host , self.port) ):## meaning there's just one node in the ring	

			
			## checking whether the sender hash is bigger than mine :
			if (self.hasher( self.successor[0] + str(self.successor[1])) < self.hasher (sender_details_tuple[0] + str(sender_details_tuple[1])) ):

				self.successor = sender_details_tuple
				self.predecessor = sender_details_tuple

			else :

				self.predecessor = sender_details_tuple
				self.successor = sender_details_tuple

			## communicating the new details :

			soc = socket.socket()
			soc.connect(sender_details_tuple)
			msg = dumps(["2nd_entry_details" , tuple([self.host , self.port])])
			soc.send(msg.encode())

		
		else : ## for general case :

			## I will tell the new node , who is their new sucessor :
			print("sender's info : " , sender_details_tuple)
			soc = socket.socket()
			soc.connect(sender_details_tuple)
			msg = dumps(["Your_new_successor_details : " , self.successor , self.my_details])
			soc.send(msg.encode())
			
			
			
	# FUNCTION USED TO SEND PING TO THEIR NEIGHBOR :	

	def ping(self):

		while (self.stop != True):

			time.sleep(0.5) ## making every iteration with the delay of 0.5 seconds 

			## sending only messages to my successor if there's more than one nodes in the ring :

			if (self.my_details != self.successor):
				soc = socket.socket()
				soc.connect(self.successor)
				msg = dumps(["ping" , self.my_details])
				#print("yes ping ! :  " , msg)
				soc.send(msg.encode())

		
			

	def handleConnection(self, client, addr):
		'''
		 Function to handle each inbound connection, called as a thread from the listener.
		'''
		## msg_recieved = [	msg_command , addresses tuple/s....  , (sender's adress , port) ]
		
		msg_recieved = client.recv(4096).decode()
		sample_msg = msg_recieved
		#print("message recieved : " , msg_recieved)
		try :
			msg_recieved = loads(msg_recieved)
			msg_command = msg_recieved[0]
			sender_details_tuple =  msg_recieved[-1]
			sender_details_tuple = tuple(sender_details_tuple)
		except :
			print("error : ", len(sample_msg) , sample_msg)

			return
		
		## CONDITIONS & COMMANDS FOR THE JOIN FUNCTION :

		## join msg pattern = ["join" , sender's details ]

		if (msg_command == "join") :

			lookup_result = self.lookup("join" ,sender_details_tuple)

			## if result gotten is true then that means i m the one who has to take the steps :

			if (lookup_result == True):

				self.adjust_new_node(sender_details_tuple)

		elif (msg_command == "2nd_entry_details"):

			self.successor = sender_details_tuple
			self.predecessor = sender_details_tuple

		elif (msg_command == "Your_new_successor_details : "):

			## indicates that :
				## firstly I am the new node to the ring :
				## secondly I got my new successor 

			## 1) editing my details :
			#print("i am called ! ")

			self.predecessor = None
			self.successor = tuple(msg_recieved[1])
			#print("my new successor : ", self.successor)

			## 2) informing my successor that i am the one :

			soc = socket.socket()
			soc.connect(self.successor)
			msg = dumps(["Your_new_predecessor_details : " , self.my_details])
			soc.send(msg.encode())

		## 3) getting a new predecessor :

		elif (msg_command == "Your_new_predecessor_details : "):
			#print(msg_recieved[-1])
			#print("yesss i am called ! , my details : ", self.my_details , " my predecessor : " , self.predecessor)
			self.predecessor = tuple(msg_recieved[-1])
			#print("yesss i am called ! , my details : ", self.my_details , " my predecessor : " , self.predecessor)

		## 4) Ping to the original successor & it's reply by sucessor to the sender: 

		elif (msg_command == "ping" ):

			## if my predecessor is empty then I will have a new one :

			if (self.predecessor == None):

				self.predecessor = tuple(sender_details_tuple)
		
			soc = socket.socket()
			soc.connect(sender_details_tuple)
			msg = dumps(["reply_to_ping" , self.predecessor , self.my_details])
			soc.send(msg.encode())
		
		## 5) On recieving the reply of the ping :

		elif (msg_command == "reply_to_ping"):
			
			given_predecessor = tuple(msg_recieved[1])
			if (given_predecessor != self.my_details):
				self.successor = given_predecessor 


		## THE FOLLOWING COMMANDS ARE FOR THE FILING SECTION :

		elif ( msg_command == "file"):

			
			lookup_result = self.lookup("file" , sender_details_tuple)

			if (lookup_result == True): ## indicating that I am the predecessor of the node which will be storing the file in it's directory

				file_name = sender_details_tuple[0]
				file_sender_details = tuple(sender_details_tuple[1])

				## sending my successor the details for the socket address/port from which the file will be send  
				soc = socket.socket()
				soc.connect(self.successor)
				msg = dumps(["Open_socket_with_this_to_recieve_file : ", file_name , file_sender_details])
				soc.send(msg.encode())

				## soc2 : sending that node the details of my successor to the file sender node so that it can send the file to deserving node :
				soc2 = socket.socket()
				soc2.connect(file_sender_details)
				msg = dumps(["Open_socket_with_this_to_send_file : ", file_name , tuple(self.successor)])
				soc2.send(msg.encode())
		

		## getting signal and details to recieve the file from the appropiate server :

		elif (msg_command == "Open_socket_with_this_to_recieve_file : "):

			file_sender_details = tuple(sender_details_tuple)
			file_name = str(msg_recieved[1])


			## creating a socket with that node :
			soc = socket.socket()
			soc.connect(file_sender_details)

			## storing the file's new path in my directory : 
			file_actual_name = file_name.split("/")[-1]
			file_actual_path = self.host + "_" + str(self.port) +"/"+ file_actual_name
			print("file's new path to be stored : ", file_actual_path)
			self.my_files.append(file_actual_path)
			
			## recieving file over : 

			self.recieveFile(soc , file_name)

			


	
				 
		elif ("Open_socket_with_this_to_send_file : "):

			file_reciever_details = tuple(sender_details_tuple)
			file_name = str(msg_recieved[1])

			## creating a socket with that node :

			soc = socket.socket()
			soc.connect(file_reciever_details)

			## sending the file over :

			self.sendFile(soc , file_name)
			


		

		#print(addr)

	def listener(self):
		'''
		We have already created a listener for you, any connection made by other nodes will be accepted here.
		For every inbound connection we spin a new thread in the form of handleConnection function. You do not need
		to edit this function. If needed you can edit signature of handleConnection function, but nothing more.
		'''
		listener = socket.socket()
		listener.bind((self.host, self.port))
		listener.listen(10)
		while not self.stop:

			client, addr = listener.accept()
			threading.Thread(target = self.handleConnection, args = (client, addr)).start()
		
		print ("Shutting down node:", self.host, self.port)
		try:
			listener.shutdown(2)
			listener.close()
		except:
			listener.close()

	
	
	def join(self, joiningAddr):
		'''
		This function handles the logic of a node joining. This function should do a lot of things such as:
		Update successor, predecessor, getting files, back up files. SEE MANUAL FOR DETAILS.
		'''

		## creating a soc to forward the new_node's address and port 
		if (joiningAddr != ""):
			soc = socket.socket()
			soc.connect(joiningAddr)
			msg = dumps(["join" , (self.host , self.port)])
			soc.send(msg.encode())

		## socket will end after this function 


	def put(self, fileName):
		'''
		This function should first find node responsible for the file given by fileName, then send the file over the socket to that node
		Responsible node should then replicate the file on appropriate node. SEE MANUAL FOR DETAILS. Responsible node should save the files
		in directory given by host_port e.g. "localhost_20007/file.py".
		'''
		
		sender_details = [fileName , self.my_details]

		
		my_results = self.check_my_worthiness(fileName)

		print( "my results are : " ,fileName , my_results)
		
		
		
		# lookup_result = self.lookup("file" , sender_details) 

		# print("my details are : ", self.my_details)

		# if (lookup_result == True): ## indicating that I am the predecessor of the node which will be storing the file in it's directory

		# 	print("i am the one to be doing this ! ")
		# 	# file_name = sender_details_tuple[0]
		# 	# file_sender_details = tuple(sender_details_tuple[1])

		# 	## sending my successor the details for the socket address/port from which the file will be send  
		# 	soc = socket.socket()
		# 	soc.connect(self.successor)
		# 	msg = dumps(["Open_socket_with_this_to_recieve_file : ", fileName , self.my_details])
		# 	soc.send(msg.encode())

		# 	## Sending the file myself  :

		# 	self.sendFile(soc, fileName)
			
			
			
			
			
			
			
			# soc2 = socket.socket()
			# soc2.connect(file_sender_details)
			# msg = dumps(["Open_socket_with_this_to_send_file : ", file_name , tuple(self.successor)])
			# soc2.send(msg.encode())




	def get(self, fileName):
		'''
		This function finds node responsible for file given by fileName, gets the file from responsible node, saves it in current directory
		i.e. "./file.py" and returns the name of file. If the file is not present on the network, return None.
		'''

		
	def leave(self):
		'''
		When called leave, a node should gracefully leave the network i.e. it should update its predecessor that it is leaving
		it should send its share of file to the new responsible node, close all the threads and leave. You can close listener thread
		by setting self.stop flag to True
		'''


	def sendFile(self, soc, fileName):
		''' 
		Utility function to send a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		fileSize = os.path.getsize(fileName)
		soc.send(str(fileSize).encode('utf-8'))
		soc.recv(1024).decode('utf-8')
		with open(fileName, "rb") as file:
			contentChunk = file.read(1024)
			while contentChunk!="".encode('utf-8'):
				soc.send(contentChunk)
				contentChunk = file.read(1024)


	def recieveFile(self, soc, fileName):
		'''
		Utility function to recieve a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		print("i m being called!!")
		fileSize = int(soc.recv(1024).decode('utf-8'))
		soc.send("ok".encode('utf-8'))
		contentRecieved = 0
		file = open(fileName, "wb")
		while contentRecieved < fileSize:
			contentChunk = soc.recv(1024)
			contentRecieved += len(contentChunk)
			file.write(contentChunk)
		file.close()

	def kill(self):
		# DO NOT EDIT THIS, used for code testing
		self.stop = True

		
