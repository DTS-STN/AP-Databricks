from azutils.AKVClient import AKVClient
import re

class MountClient:

  """
  This class allows user/developer mount Azure Datalake containers to Databricks Workspace
  and also to read from and write to ADLS at a preferred location in a desired file format.
  """

  storage_account_name = AKVClient.adlsName()
  conf_key = f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net"
  configs = {conf_key:AKVClient.adlsAccessKey()}
  
  def __init__(self, container_name):
    self.container_name = container_name
    self.source = f"wasbs://{self.container_name}@{self.storage_account_name}.blob.core.windows.net"
    self.mount_point = f"/mnt/{self.storage_account_name}/{self.container_name}"

  def mount(self):
    """Mounts the ADLS container."""
    #mounts container if it is not already mounted
    if not self._is_mounted(): 
      #mounts if container exists in azure storage
      try:
        AKVClient.dbutils.fs.mount(
          source = self.source,
          mount_point = f"/mnt/{self.storage_account_name}/{self.container_name}",
          extra_configs = self.configs
        )
        print(f"The container {self.container_name} successfully mounted")
      except: print(f"The container {self.container_name} does not exist in Azure storage account")
    else:
      print(f"The container {self.container_name} is already mounted")
  
  def unmount(self):
    """Unmounts the ADLS container."""
    if self._is_mounted():
      AKVClient.dbutils.fs.unmount(self.mount_point)
    else:
      print(f"The container {self.container_name} not found in DBFS. Nothing to unmount.")
    
  def _is_mounted(self): 
    """Checks to see if the container is mounted."""
    return any(mount.mountPoint == self.mount_point for mount in AKVClient.dbutils.fs.mounts())

  @classmethod
  def read(cls, source_file_path: str, source_file_type: str, reader_options:dict = None):
    """Reads data from Azure Data Lake Storage"""
    #mounts container if container is not already mounted 
    pattern = "/mnt/stsaebdevca01/([A-Za-z0-9-_]*)/.*$"
    container = re.search(pattern, source_file_path).group(1) 

    MountClient(container).mount()

    if MountClient(container)._is_mounted():

      source_file = source_file_path.rsplit('/',1)[1]
      directory = source_file_path.rsplit('/',1)[0]
      
      try:
        AKVClient.dbutils.fs.ls(directory)
        #check if file exists
        if source_file in [file.name for file in AKVClient.dbutils.fs.ls(directory)]:
          if reader_options == None:
            if source_file_type == "csv":
              #reads csv file with header if file type is csv and reader options are not given
              reader_options = {"header": True}
              return AKVClient.spark.read \
                .format(source_file_type) \
                .options(**reader_options) \
                .load(source_file_path)
            else: 
              #basic file read if file type is not a csv and reader options are not given
              return AKVClient.spark.read \
                .format(source_file_type) \
                .load(source_file_path)
          else:
            #reads file with given reader options
            return AKVClient.spark.read \
              .format(source_file_type) \
              .options(**reader_options) \
              .load(source_file_path)
        else: print(f"File does not exist: {source_file_path}")
      except: print(f"Path does not exist: {source_file_path}")

  @classmethod
  def write(cls, dataframe, destination_file_path: str, destination_file_type: str, write_mode:str = "overwrite", writer_options:dict = None):
    """Writes data to Azure Data Lake Storage. Default write mode is 'overwrite'."""
    #mounts container if container is not already mounted 
    pattern = "/mnt/stsaebdevca01/([A-Za-z0-9-_]*)/.*$"
    container = re.search(pattern, destination_file_path).group(1) 

    MountClient(container).mount()
    
    #write to container only if it is mounted
    if MountClient(container)._is_mounted():
      
      if writer_options==None:
        if destination_file_type=="csv":
          #writes csv file with header if file type is csv and writer options are not given
          writer_options={"header": True}
          dataframe.write \
            .format(destination_file_type) \
            .options(**writer_options) \
            .mode(write_mode) \
            .save(destination_file_path)
          print(f"Data written to {destination_file_path}")
        else: 
          #basic dataframe write if file type is not a csv and writer options are not given
          dataframe.write \
            .format(destination_file_type) \
            .mode(write_mode) \
            .save(destination_file_path)
          print(f"Data written to {destination_file_path}")
      else:
        #writes file with given writer options
        dataframe.write \
          .format(destination_file_type) \
          .options(**writer_options) \
          .mode(write_mode) \
          .save(destination_file_path)
        print(f"Data written to {destination_file_path}")
