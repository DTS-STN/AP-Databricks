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
  
  def __init__(self, container):
    self.container = container
    self.source = f"wasbs://{self.container}@{self.storage_account_name}.blob.core.windows.net"
    self.mount_point = f"/mnt/{self.storage_account_name}/{self.container}"

  def mount(self):
    """Mounts the ADLS container."""
    #mounts container if it is not already mounted
    if not self._is_mounted(): 
      #mounts if container exists in azure storage
      try:
        AKVClient.dbutils.fs.mount(
          source = self.source,
          mount_point = f"/mnt/{self.storage_account_name}/{self.container}",
          extra_configs = self.configs
        )
        print(f"The container {self.container} successfully mounted")
      except: print(f"The container {self.container} does not exist in Azure storage account")
    else:
      print(f"The container {self.container} is already mounted")
  
  def unmount(self):
    """Unmounts the ADLS container."""
    if self._is_mounted():
      AKVClient.dbutils.fs.unmount(self.mount_point)
    else:
      print(f"The container {self.container} not found in DBFS. Nothing to unmount.")
    
  def _is_mounted(self): 
    """Checks to see if the container is mounted."""
    return any(mount.mountPoint == self.mount_point for mount in AKVClient.dbutils.fs.mounts())

  @classmethod
  def read(cls, source_path: str, source_format: str, schema = None, options:dict = None):
    """Reads data from Azure Data Lake Storage"""
    #mounts container if container is not already mounted 
    pattern = "/mnt/stsaebdevca01/([A-Za-z0-9-_]*)/.*$"
    container = re.search(pattern, source_path).group(1) 

    MountClient(container).mount()

    if MountClient(container)._is_mounted():

      source_file = source_path.rsplit('/',1)[1]
      directory = source_path.rsplit('/',1)[0]
      
      try:
        AKVClient.dbutils.fs.ls(directory)
        #check if file exists
        if source_file in [file.name for file in AKVClient.dbutils.fs.ls(directory)]:
          if schema == None:
            if options == None:
              if source_format == "csv":
                #reads csv file with header if file type is csv and reader options are not given
                options = {"header": True}
                return AKVClient.spark.read \
                  .format(source_format) \
                  .options(**options) \
                  .load(source_path)
              else: 
                #basic file read if file type is not a csv and reader options are not given
                return AKVClient.spark.read \
                  .format(source_format) \
                  .load(source_path)
            else:
              #reads file with given reader options
              return AKVClient.spark.read \
                .format(source_format) \
                .options(**options) \
                .load(source_path)
          else:
            if options == None:
              if source_format == "csv":
                #reads csv file with header if file type is csv and reader options are not given
                options = {"header": True}
                return AKVClient.spark.read \
                  .format(source_format) \
                  .options(**options) \
                  .schema(schema) \
                  .load(source_path)
              else: 
                #basic file read if file type is not a csv and reader options are not given
                return AKVClient.spark.read \
                  .format(source_format) \
                  .schema(schema) \
                  .load(source_path)
            else:
              #reads file with given reader options
              return AKVClient.spark.read \
                .format(source_format) \
                .options(**options) \
                .schema(schema) \
                .load(source_path)
        else: print(f"File does not exist: {source_path}")
      except: print(f"Path does not exist: {source_path}")

  @classmethod
  def write(cls, df, output_path: str, output_format: str, mode:str = "overwrite", options:dict = None):
    """Writes data to Azure Data Lake Storage. Default write mode is 'overwrite'."""
    #mounts container if container is not already mounted 
    pattern = "/mnt/stsaebdevca01/([A-Za-z0-9-_]*)/.*$"
    container = re.search(pattern, output_path).group(1) 

    MountClient(container).mount()
    
    #write to container only if it is mounted
    if MountClient(container)._is_mounted():
      
      if options==None:
        if output_format=="csv":
          #writes csv file with header if file type is csv and writer options are not given
          options={"header": True}
          df.write \
            .format(output_format) \
            .options(**options) \
            .mode(mode) \
            .save(output_path)
          print(f"Data written to {output_path}")
        else: 
          #basic df write if file type is not a csv and writer options are not given
          df.write \
            .format(output_format) \
            .mode(mode) \
            .save(output_path)
          print(f"Data written to {output_path}")
      else:
        #writes file with given writer options
        df.write \
          .format(output_format) \
          .options(**options) \
          .mode(mode) \
          .save(output_path)
        print(f"Data written to {output_path}")
