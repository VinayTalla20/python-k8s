import os
import yaml
import sys


conf = input("provide the path of cluster config file: ")
print("Checking for path:",conf,"\n")
user = os.getenv('USER')
print("current user:",user, "\n")

if (os.access(conf, os.R_OK)):
       print("The path",conf,"has access to the current user",user,"\n")

else:

    print("user",user,"does not has access instead go with user root or use sudo previlage \n")
    sys.exit()


if(os.path.exists(conf)):
    with open(conf) as f:
         results = yaml.load(f, Loader=yaml.FullLoader)
         try:
              print ("API-SERVER-URL=",results['clusters'][0]['cluster']['server'],"\n")
         except:
             print: "Does'nt exists"
