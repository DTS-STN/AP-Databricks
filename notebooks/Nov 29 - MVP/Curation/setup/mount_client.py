# Databricks notebook source
class MountClient:
  storage_account_name = "stsaebdevca01"
  conf_key = f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net"
  scope_name = "akv-saeb-dbc-scrt-scp"
  key_name = "saebadlsstorageaccesskey"
  configs = {conf_key:dbutils.secrets.get(scope=scope_name, key=key_name)}
  
  def __init__(self, container_name = None):
    self.container_name = container_name
    self.source = f"wasbs://{self.container_name}@{self.storage_account_name}.blob.core.windows.net"
    self.mount_point = f"/mnt/{self.storage_account_name}/{self.container_name}"
    
  def mount(self):
    if not self._is_mounted():
      dbutils.fs.mount(
        source = self.source,
        mount_point = f"/mnt/{self.storage_account_name}/{self.container_name}",
        extra_configs = self.configs
      )
      print(f"The container {self.container_name} successfully mounted")
    else:
      print(f"The container {self.container_name} is already mounted")
  
  def unmount(self):
    if self._is_mounted():
      dbutils.fs.unmount(self.mount_point)
    else:
      print(f"The container {self.container_name} not found in DBFS. Nothing to unmount.")
    
  def _is_mounted(self): 
      return any(mount.mountPoint == self.mount_point for mount in dbutils.fs.mounts())