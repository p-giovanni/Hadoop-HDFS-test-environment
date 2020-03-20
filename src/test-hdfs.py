
import os
import sys
from snakebite.client import AutoConfigClient


def main():
    hadoop_conf_dir = "/media/d2/code-sky/dockers/hadoop/etc/hadoop"
    os.environ['HADOOP_CONF_DIR'] = hadoop_conf_dir

    file_dict = {}

    cli = AutoConfigClient()
    target_hdfs_path = "/"
    
    for element in cli.ls([target_hdfs_path]):
        print("Result: "+str(element))

if __name__ == '__main__':

   main()

   sys.exit(0)
