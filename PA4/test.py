
import os

file_name = "dummy.txt"

with open(file_name,"r") as file:
    file_contents = file.read()

print(file_contents)

## completeName = os.path.join(save_path, name_of_file+".txt")

file_actual_path = "localhost_8500"
complete_name = os.path.join(file_actual_path , file_name)   

file = open(complete_name,"w")
file.write(file_contents)
file.close