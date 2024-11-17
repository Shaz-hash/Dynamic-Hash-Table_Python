from cgitb import lookup
from json import dumps, loads
from operator import truediv
import socket 
import threading
import os
import time
import hashlib





### ANSWER TO MY FILE TOLERANCE APPLICATION :

"""
	I have replicated files in my Predecessor because the way my program has been created to tackle file tolerance issue. Basically all the reasoning
	for this implementation boils down to my ping function. My ping function, every .5 sec , ping's its successor and in return my successor provides its 
	predeccessor with 

	1) names of all the files it has
	2) the name of the grandsuccessor aka successor of the successor 

	hence using the reply to the ping given to the predeccessor, it can determine whether there has been change in successor's successor or/and change in the 
	files its successor has. And if there's a change , a predeccessoor can ask specifically to the successor the exact filename and/or the info abt the new changes
	and hence in the case of the file it saves the contents as a duplication of the successor's view & contents of the file. 

	Overall this goes on in the ring and each node has duplicated files stored of other node which is its successor. In the case a node leaves , predeccosr in 
	my implementation will take the actions :

	1) make it's successor as grand successor 
	2) send over all the duplicated files to this new successor 

	and the cycle continues.....

	Generally speaking this is open choice question and it depends on the programmer's implmentation which drives them to a different conclusion to tackle this
	problem `\O_O/`



"""


















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
		self.answer_from_get_req = False
		self.file_contents_from_get_req = ""
		self.no_reply_status = 0

		## This is a dictionary storing in format : { file_name_1 : file_content_1 , file_name_2 : file_content_2 .......    } 
		self.grandsucessor = tuple(["",""])
		self.successor_files = {}


	
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

			soc = socket.socket()
			soc.connect(self.successor)
			msg = dumps(sender_details)
			soc.send(msg.encode())			

		elif (type == "Get"):

			## just pass over the request to successor :

			soc = socket.socket()
			soc.connect(self.successor)
			msg = dumps(sender_details)
			soc.send(msg.encode())


	def check_my_worthiness (self , file_name):

		file_hash = self.hasher(file_name)

		while(self.predecessor == None):
			time.sleep(0.2)	
			continue
		
		presuccess_hash = self.hasher( self.predecessor[0] + str(self.predecessor[1]))
		my_hash = self.hasher( self.my_details[0] + str(self.my_details[1]))


		#print("my hash : " , my_hash , " : file hash " , file_hash , " presucess hash : " , presuccess_hash )

		if (my_hash > file_hash) and (file_hash > presuccess_hash):
			
			#print("I am the right one ! ")
			#print("my hash : " , my_hash , " : file hash " , file_hash , " presucess hash : " , presuccess_hash , self.my_details , self.predecessor)


			return True

		elif (my_hash < presuccess_hash) and (my_hash > file_hash):

			#print("I am the right one ! ")
			#print("my hash : " , my_hash , " : file hash " , file_hash , " presucess hash : " , presuccess_hash , self.my_details , self.predecessor)
			return True

		elif (my_hash < presuccess_hash) and (presuccess_hash < file_hash ):

			#print("I am the right one ! ")
			#print("my hash : " , my_hash , " : file hash " , file_hash , " presucess hash : " , presuccess_hash , self.my_details , self.predecessor)
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
			#print("sender's info : " , sender_details_tuple)
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
				try :
					soc.connect(self.successor)
					msg = dumps(["ping" , self.my_details])
					#print("yes ping ! :  " , msg)
					soc.send(msg.encode())

				except :
					print("error in finding the node...")
					

				## checking if i am reciveing the send ping or not :

				if (self.stop == True):
					return 
				while ( self.no_reply_status != "recieved" and self.no_reply_status < 2 ):

					time.sleep(0.5)

					if (self.no_reply_status != "recieved"):
						self.no_reply_status = self.no_reply_status + 1


					else :

						break 	
					try :

						if (self.stop == True):
							return 
						soc = socket.socket()
						soc.connect(self.successor)
						msg = dumps(["ping" , self.my_details])
						#print("yes ping ! :  " , msg)
						soc.send(msg.encode())
					except :
						print("failure in connection...")

				if( self.no_reply_status == "recieved"):

					self.no_reply_status = 0

					continue

				else  :

					if (self.stop == True):
						return 
					self.no_reply_status = 0
					# self_socket =  socket.socket()
					# self_socket.bind(tuple(self.my_details))
					#print ("yahoooooooooooooo")

					soc = socket.socket()
					soc.connect(tuple(self.grandsucessor))
					msg = dumps(["Leaving : your new predeccessor is : " , tuple(self.my_details)])
					soc.send(msg.encode())	
					soc.close()	
					time.sleep(0.1)			


					print("old successor was : ", self.successor)
					self.successor = tuple(self.grandsucessor)
					print("new succesor is : " , self.successor)
					

					all_successor_file_name = list(self.successor_files.keys())
					
					for file_name in all_successor_file_name :

						file_contents = self.successor_files[file_name]
						sender_file_details = [ "file" , file_name , self.my_details , file_contents] 

						soc = socket.socket()
						soc.connect(self.successor)
						msg = dumps(sender_file_details)
						soc.send(msg.encode())	
						soc.close()
					



			# time.sleep(0.5) ## making every iteration with the delay of 0.5 seconds 
		

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
			#print("error : ", len(sample_msg) , sample_msg)

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
			
			self.predecessor = tuple(msg_recieved[-1])

			sample_file_name_list = []

			for file_name in self.files :
				sample_file_name_list.append(file_name) 

			## at this point i know that i have my new node as predecessor and my predecessor knows I am the sucessor :
			self.files.clear()

			for file_name in sample_file_name_list :

				folder_name = self.host + "_" + str(self.port)
				complete_path = os.path.join(folder_name , file_name)

				with open(complete_path,"r") as file :
					file_contents = file.read()

				os.remove(complete_path)

				## now sending the new info as file command  :

				sender_file_details = [ "file" , file_name , self.my_details , file_contents]

				soc = socket.socket()
				soc.connect(self.predecessor)
				msg = dumps(sender_file_details)
				soc.send(msg.encode())

		## 4) Ping to the original successor & it's reply by sucessor to the sender: 

		elif (msg_command == "ping" ):

			## if my predecessor is empty then I will have a new one :

			if (self.predecessor == None):

				self.predecessor = tuple(sender_details_tuple)
		
			# while(True):
			try:
				soc = socket.socket()
				soc.connect(tuple(sender_details_tuple))
				msg = dumps(["reply_to_ping" , self.predecessor , self.my_details , self.files , self.successor])
				soc.send(msg.encode())
				pass
			except :
				print("errrrrorrr" , self.predecessor , self.my_details , sender_details_tuple)
				self.predecessor = None
				
				
			# 	print(self.my_details , " m facing error in replying to : " , self.predecessor , " sender is : " , sender_details_tuple)

		## 5) On recieving the reply of the ping :

		elif (msg_command == "reply_to_ping"):
			
			self.no_reply_status = "recieved"
			given_predecessor = tuple(msg_recieved[1])
			if (given_predecessor != self.my_details):
				self.successor = given_predecessor 
			

			## CHECKING WHETHER THE DETAILS ARE CHANGED OR NOT :


			else :
				self.grandsucessor = tuple(msg_recieved[-1])
				successor_files = msg_recieved[3]

				for successor_file in successor_files :


					# print("YESSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSs" , successor_file)

					if (successor_file in self.successor_files.keys()):
						
						#print("file present is : ", successor_file)
						continue 

					else :

						## indicating that a new file_name is in my successor but not in me :

						#print("I AM DEMANDING THE FILE : ", successor_file)
						soc = socket.socket()
						soc.connect( tuple(msg_recieved[2]))
						msg = dumps(["I need this specific file : " , successor_file , self.my_details])
						soc.send(msg.encode())
						

				## removing the files which are not longer in my successor's storage :
				try :
					for my_successor_file_name , contents in self.successor_files.items():

						if my_successor_file_name not in successor_files :

							del self.successor_files[my_successor_file_name]
				except :
					print("director changing due to rehash ")

		## THE FOLLOWING COMMANDS ARE FOR THE FILING SECTION :
	
		elif (msg_command =="Save this file : "):
			
			
			
			file_name =str(msg_recieved[-1])

			with open(file_name,"r") as file :
				file_contents = file.read()

			folder_name = self.host + "_" + str(self.port) 
			complete_name = os.path.join(folder_name , file_name)

			file = open(complete_name,"w")
			file.write(file_contents)
			file.close

			self.files.append(file_name)


		elif (msg_command == "put this file : "):

			#print(" I am called " , self.my_details , " with the file name : ", str(msg_recieved[-1]) )
			
			self.put(str(msg_recieved[-1]))


		elif (msg_command == "file"):

			
		
			file_name = msg_recieved[1]
			file_contents = msg_recieved[-1]
			
			my_results = self.check_my_worthiness(msg_recieved[1])


			if (my_results == True ): 

				self.files.append(file_name)

				folder_name = self.host + "_" + str(self.port) 
				complete_name = os.path.join(folder_name , file_name)

				file = open(complete_name,"w")
				file.write(file_contents)
				file.close

			else : ## I am not the 

				lookup_result = self.lookup(msg_recieved[0] , msg_recieved) 


		## THE FOLLOWING COMMANDS ARE FOR THE FILING GET SECTION :

		elif( msg_command == "Get"):

			file_name = msg_recieved[1]

			## checking if this belongs to me or not :
			file_holding_result = self.check_my_worthiness(file_name)

			file_fetch_result = False

			if (file_holding_result == True):

				if (file_name  in self.files):

					## getting the file's contents :



					folder_name = self.host + "_" + str(self.port) 
					complete_name = os.path.join(folder_name , file_name)

					#print(self.my_details , file_name)
					
					try :
						with open(complete_name,"r") as file :
							file_contents = file.read()

						file_fetch_result = ["Get request reply : ", file_name, file_contents]
					except :

						print("could not find the file : " , file_name , self.my_details)



				else :

					file_fetch_result = ["Get request reply : " , "Not in Network" , "None"]

				file_demander_details = tuple(msg_recieved[-1])
				

				#print("I got it ! " , self.my_details)
				soc = socket.socket()
				soc.connect(file_demander_details)
				msg = dumps(file_fetch_result)
				soc.send(msg.encode())

			else :

				self.lookup(msg_recieved[0] , msg_recieved)


		elif ( msg_command == "Get request reply : "):
			
			self.answer_from_get_req = msg_recieved[1]
			self.file_contents_from_get_req = msg_recieved[-1]



		## THE FOLLOWING COMMANDS ARE FOR THE LEAVE SECTION :

		elif ( msg_command == "Leaving : your new predeccessor is : "):

			self.predecessor = tuple(msg_recieved[-1])
		
		elif ( msg_command == "Leaving : your new successor is : "):

			self.successor = tuple(msg_recieved[-1])

		
		## THE FOLLOWING COMMANDS ARE FOR THE FAILURE TOLERANCE :

		elif ( msg_command == "I need this specific file : "):

			file_name = msg_recieved[1]
			sender_address = msg_recieved[-1]

			folder_name = self.host + "_" + str(self.port) 
			complete_name = os.path.join(folder_name , file_name)

			with open(complete_name,"r") as file :
				file_contents = file.read()

			try:
				soc = socket.socket()
				soc.connect( tuple(sender_address))
				msg = dumps(["Here is your specific file : " , file_name , file_contents])
				soc.send(msg.encode())
			except :
				print("node is leaving so next time ... ")

		elif (msg_command == "Here is your specific file : "):

			file_name = msg_recieved[1]
			file_contents = msg_recieved[-1]

			self.successor_files[file_name] = file_contents

			
			












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
		
		
		with open(fileName,"r") as file :
			file_contents = file.read()
		
		sender_file_details = [ "file" , fileName , self.my_details , file_contents]
		
		my_results = self.check_my_worthiness(fileName)

		#print( "my results are : " ,fileName , my_results)

		if (my_results == True ): ## indicating that i am the correct node for the delivery :

			self.files.append(fileName)

			with open(fileName,"r") as file :
				file_contents = file.read()

			folder_name = self.host + "_" + str(self.port) 
			complete_name = os.path.join(folder_name , fileName)

			file = open(complete_name,"w")
			file.write(file_contents)
			file.close

		else : ## I am not the 


			self.lookup(sender_file_details[0], sender_file_details) 



	def get(self, fileName):
		
		'''
		This function finds node responsible for file given by fileName, gets the file from responsible node, saves it in current directory
		i.e. "./file.py" and returns the name of file. If the file is not present on the network, return None.
		'''


		## my message forwarded in the lookup will be in the form : ["Get","file_name",(asker-details) ]

		## 1) checking if I am worthy for having this file in my data structure :

		my_results = self.check_my_worthiness(fileName)

		if ( my_results == True ):

			if (fileName in self.files):

				#print(self.files , self.my_details)
				#print("yeesssss!")

				return fileName

			else :

				return None

		else :

			fetch_get_msg = ["Get" , fileName , self.my_details]

			self.lookup(fetch_get_msg[0] , fetch_get_msg)

			while(self.answer_from_get_req == False):

				time.sleep(0.3)

				if (self.answer_from_get_req != False):
					break


			if (self.answer_from_get_req == fileName):

				## creating file in the directory :

				# with open(fileName,"r") as file :
				# 	file_contents = file.read()

				# folder_name = self.host + "_" + str(self.port) 
				complete_name = os.path.join(".", fileName)


				#print("yess!" , fileName , self.file_contents_from_get_req)
				file = open(complete_name,"w")
				file.write(self.file_contents_from_get_req)
				file.close

				self.answer_from_get_req = False
				self.file_contents_from_get_req = ""

				return fileName

			else :

				#print("Noneeee")
				return None

		
	def leave(self):
		'''
		When called leave, a node should gracefully leave the network i.e. it should update its predecessor that it is leaving
		it should send its share of file to the new responsible node, close all the threads and leave. You can close listener thread
		by setting self.stop flag to True
		'''

		temp_successor_details = dumps(self.successor)
		temp_predecessor_details = dumps(self.predecessor)


		temp_successor_details = tuple(loads(temp_successor_details))
		temp_predecessor_details = tuple(loads(temp_predecessor_details))


		## 1) informing the successor that there's a new predecessor for it :

		soc = socket.socket()
		soc.connect(temp_successor_details)
		msg = dumps( ["Leaving : your new predeccessor is : " , temp_predecessor_details])
		soc.send(msg.encode())


		## 2) informing the predeccessor that there's a successor for it :

		soc_2 = socket.socket()
		soc_2.connect(temp_predecessor_details)
		msg = dumps( ["Leaving : your new successor is : " , temp_successor_details] )
		soc_2.send(msg.encode())

		## 3) sending over the files to my predeccessor : 

		sample_file_name_list = []

		for file_name in self.files :
			sample_file_name_list.append(file_name) 

		## at this point i know that i have my new node as predecessor and my predecessor knows I am the sucessor :
		# self.files.clear()

		for file_name in sample_file_name_list :

			folder_name = self.host + "_" + str(self.port)
			complete_path = os.path.join(folder_name , file_name)

			with open(complete_path,"r") as file :
				file_contents = file.read()

			os.remove(complete_path)

			## now sending the new info as file command  :

			#print("sending files to : ", temp_successor_details)
			sender_file_details = [ "file" , file_name , self.my_details , file_contents]
			#print (sender_file_details)
			soc = socket.socket()
			soc.connect(temp_successor_details)
			msg = dumps(sender_file_details)
			soc.send(msg.encode())

		
		time.sleep(0.1)
		self.stop = True




		



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

		
