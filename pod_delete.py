import json

import yaml
from kubernetes import config
import kubernetes

config.load_config()


namespace = "default"
pod_name = []
print("Enter the Namespace")
#namespace= input(namespace)
print(namespace)
pod_api = kubernetes.client.CoreV1Api()

pod_list = pod_api.list_namespaced_pod(namespace=namespace)

#print(pod_list.items[0].metadata.name)

for i in pod_list.items:
    pod = i.metadata.name
    pod_name.append(pod)

#print(pod_name)

for s in pod_name:
    pod_status_list = pod_api.read_namespaced_pod_status(namespace=namespace, name=s)

    pod_status = pod_status_list.status.phase
    #print("pod_name= " + s +" Status= "+ pod_status)
    if pod_status == "Running":
        print("The Pod "+ s + " is "+ pod_status + ", So Everything is ok")
    else:
        print("The Pod "+ s + " is "+ pod_status+ ", it Will be Terminated")
        pod_delete_list = pod_api.delete_namespaced_pod(namespace=namespace, name=s)
        print("The Pod "+ s + " is Terminated Successfully" )
