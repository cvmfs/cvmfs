#
# This file is part of the CernVM File System.
#

from datetime import datetime, timezone
from dxf import DXF, hash_file, hash_bytes
from dxf.exceptions import DXFUnauthorizedError
import json
import subprocess
import tarfile
import tempfile
from requests.exceptions import HTTPError
import os
import zlib

def exec_bash(cmd):
  process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  output, error = process.communicate()
  return (output, error)

class FatManifest:
  """
  Class which represents a "fat" OCI image configuration manifest
  """
  def __init__(self, manif):
    self.manif = json.loads(manif)

  def init_cvmfs_layer(self, tar_digest, gz_digest):
    """
    Method which initializes the cvmfs injection capability by adding an empty /cvmfs layer
    to the image's fat manifest
    """
    if self.manif["rootfs"]["type"] != "layers":
      raise ValueError("Cannot inject in rootfs of type " + self.manif["rootfs"]["type"])
    self.manif["rootfs"]["diff_ids"].append(tar_digest)
    
    # Write history
    local_time = datetime.now(timezone.utc).astimezone()
    self.manif["history"].append({
      "created":local_time.isoformat(),
      "created_by":"/bin/sh -c #(nop) ADD file:"+tar_digest+" in / ",
      "author":"cvmfs_shrinkwrap",
      "comment": "This change was executed through the CVMFS Shrinkwrap Docker Injector"
    })

    # Setup labels
    if "Labels" not in self.manif["config"]:
      self.manif["config"]["Labels"] = {}
    self.manif["config"]["Labels"]["cvmfs_injection_tar"] = tar_digest
    self.manif["config"]["Labels"]["cvmfs_injection_gz"] = gz_digest

    if "container_config" in self.manif:
      if "Labels" not in self.manif["config"]:
        self.manif["container_config"]["Labels"] = {}
      self.manif["container_config"]["Labels"]["cvmfs_injection_tar"] = tar_digest
      self.manif["container_config"]["Labels"]["cvmfs_injection_gz"] = gz_digest

  def inject(self, tar_digest, gz_digest):
    """
    Injects a new version of the layer by replacing the corresponding digests
    """
    if not self.is_cvmfs_prepared():
      raise ValueError("Cannot inject in unprepated image")
    old_tar_digest = self.manif["config"]["Labels"]["cvmfs_injection_tar"]

    self.manif["container_config"]["Labels"]["cvmfs_injection_tar"] = tar_digest
    self.manif["container_config"]["Labels"]["cvmfs_injection_gz"] = gz_digest
    self.manif["config"]["Labels"]["cvmfs_injection_tar"] = tar_digest
    self.manif["config"]["Labels"]["cvmfs_injection_gz"] = gz_digest

    found = False
    for i in range(len(self.manif["rootfs"]["diff_ids"])):
      if self.manif["rootfs"]["diff_ids"][i] == old_tar_digest:
        self.manif["rootfs"]["diff_ids"][i] = tar_digest
        found = True
        break
    if not found:
      raise ValueError("Image did not contain old cvmfs injection!")
    
    local_time = datetime.now(timezone.utc).astimezone()
    self.manif["history"].append({
      "created":local_time.isoformat(),
      "created_by":"/bin/sh -c #(nop) UPDATE file: from "+old_tar_digest+" to "+tar_digest+" in / ",
      "author":"cvmfs_shrinkwrap",
      "comment": "This change was executed through the CVMFS Shrinkwrap Docker Injector",
      "empty_layer":True
    })
    
  def is_cvmfs_prepared(self):
    """
    Checks whether image is prepared for cvmfs injection
    """
    return "cvmfs_injection_gz" in self.manif["config"]["Labels"]\
      and "cvmfs_injection_tar" in self.manif["config"]["Labels"]

  def get_gz_digest(self):
    """
    Retrieves the GZ digest necessary for layer downloading
    """
    return self.manif["config"]["Labels"]["cvmfs_injection_gz"]

  def as_JSON(self):
    """
    Retrieve JSON version of OCI manifest (for upload)
    """
    res = json.dumps(self.manif)
    return res

class ImageManifest:
  """
  Class which represents the "slim" image manifest used by the OCI distribution spec
  """
  def __init__(self, manif):
    self.manif = json.loads(manif)
  def get_fat_manif_digest(self):
    """
    Method for retrieving the digest (content address) of the manifest.
    """
    return self.manif['config']['digest']

  def init_cvmfs_layer(self, layer_digest, layer_size, manifest_digest, manifest_size):
    """
    Method which initializes the cvmfs injection capability by adding an empty /cvmfs layer
    to the image's slim manifest
    """
    self.manif["layers"].append({
      'mediaType':'application/vnd.docker.image.rootfs.diff.tar.gzip',
      'size':layer_size,
      'digest':layer_digest
    })
    self.manif["config"]["size"] = manifest_size
    self.manif["config"]["digest"] = manifest_digest
  def inject(self ,old, new, layer_size, manifest_digest, manifest_size):
    """
    Injects a new version of the layer by replacing the corresponding digest
    """
    for i in range(len(self.manif["layers"])):
      if self.manif["layers"][i]["digest"] == old:
        self.manif["layers"][i]["digest"] = new
        self.manif["layers"][i]["size"] = layer_size
    self.manif["config"]["size"] = manifest_size
    self.manif["config"]["digest"] = manifest_digest
  def as_JSON(self):
    res = json.dumps(self.manif)
    return res

class DockerInjector:
  """
  The main class of the Docker injector which injects new versions of a layer into 
  OCI images retrieved from an OCI compliant distribution API
  """
  def __init__(self, host, repo, alias, user, pw):
    """
    Initializes the injector by downloading both the slim and the fat image manifest
    """
    def auth(dxf, response):
      dxf.authenticate(user, pw, response=response)
    self.dxfObject = DXF(host, repo, tlsverify=True, auth=auth)
    self.image_manifest = self._get_manifest(alias)  
    self.fat_manifest = self._get_fat_manifest(self.image_manifest)

  def setup(self, push_alias):
    """
    Sets an image up for layer injection
    """
    tar_digest, gz_digest = self._build_init_tar()
    layer_size = self.dxfObject.blob_size(gz_digest)
    self.fat_manifest.init_cvmfs_layer(tar_digest, gz_digest)
    fat_man_json = self.fat_manifest.as_JSON()
    manifest_digest = hash_bytes(bytes(fat_man_json, 'utf-8'))
    self.dxfObject.push_blob(data=fat_man_json, digest=manifest_digest)
    manifest_size = self.dxfObject.blob_size(manifest_digest)
    self.image_manifest.init_cvmfs_layer(gz_digest, layer_size, manifest_digest, manifest_size)

    image_man_json = self.image_manifest.as_JSON()
    self.dxfObject.set_manifest(push_alias, image_man_json)
  
  def unpack(self, dest_dir):
    """
    Unpacks the current version of a layer into the dest_dir directory in order to update it
    """
    if not self.fat_manifest.is_cvmfs_prepared():
      os.makedirs(dest_dir+"/cvmfs", exist_ok=True)
      return
      
    gz_digest = self.fat_manifest.get_gz_digest()
    # Write out tar file
    decompress_object = zlib.decompressobj(16+zlib.MAX_WBITS)
    try:
      chunk_it = self.dxfObject.pull_blob(gz_digest)
    except HTTPError as e:
      if e.response.status_code == 404:
        print("ERROR: The hash of the CVMFS layer must have changed.")
        print("This is a known issue. Please do not reupload images to other repositories after CVMFS injection!")
      else:
        raise e
    with tempfile.TemporaryFile() as tmp_file:
      for chunk in chunk_it:
        tmp_file.write(decompress_object.decompress(chunk))
      tmp_file.write(decompress_object.flush())
      tmp_file.seek(0)
      tar = tarfile.TarFile(fileobj=tmp_file)
      tar.extractall(dest_dir)
      tar.close()

  def update(self, src_dir, push_alias):
    """
    Packs and uploads the contents of src_dir as a layer and injects the layer into the image.
    The new layer version is stored under the tag push_alias
    """
    if not self.fat_manifest.is_cvmfs_prepared():
      print("Preparing image for CVMFS injection...")
      self.setup(push_alias)
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
      print("Bundling file into tar...")
      _, error = exec_bash("tar --xattrs -C "+src_dir+" -cvf "+tmp_file.name+" .")
      if error:
        raise RuntimeError("Failed to tar with error " + str(error))
      tar_digest = hash_file(tmp_file.name)
      print("Bundling tar into gz...")
      gz_dest = tmp_file.name+".gz"
      _, error = exec_bash("gzip "+tmp_file.name)
      if error:
        raise RuntimeError("Failed to tar with error " + str(error))
      print("Uploading...")
      gz_digest = self.dxfObject.push_blob(gz_dest)
      os.unlink(gz_dest)
    print("Refreshing manifests...")
    old_gz_digest = self.fat_manifest.get_gz_digest()
    layer_size = self.dxfObject.blob_size(gz_digest)
    self.fat_manifest.inject(tar_digest, gz_digest)
    fat_man_json = self.fat_manifest.as_JSON()
    manifest_digest = hash_bytes(bytes(fat_man_json, 'utf-8'))
    self.dxfObject.push_blob(data=fat_man_json, digest=manifest_digest)
    manifest_size = self.dxfObject.blob_size(manifest_digest)

    self.image_manifest.inject(old_gz_digest, gz_digest, layer_size, manifest_digest, manifest_size)
    
    image_man_json = self.image_manifest.as_JSON()
    self.dxfObject.set_manifest(push_alias, image_man_json)


  def _get_manifest(self, alias):
    return ImageManifest(self.dxfObject.get_manifest(alias))

  def _get_fat_manifest(self, image_manifest):
    fat_manifest = ""
    (readIter, _) = self.dxfObject.pull_blob(self.image_manifest.get_fat_manif_digest(), size=True, chunk_size=4096)
    for chunk in readIter:
      fat_manifest += str(chunk)[2:-1]
    fat_manifest = fat_manifest.replace("\\\\","\\")
    return FatManifest(fat_manifest)

  def _build_init_tar(self):
    """
    Builds an empty /cvmfs tar and uploads it to the registry

    :rtype: tuple
    :returns: Tuple containing the tar digest and gz digest
    """
    ident = self.image_manifest.get_fat_manif_digest()[5:15]
    tmp_name = "/tmp/injector-"+ident
    os.makedirs(tmp_name+"/cvmfs", exist_ok=True)
    tar_dest = "/tmp/"+ident+".tar"
    _, error = exec_bash("tar --xattrs -C "+tmp_name+" -cvf "+tar_dest+" .")
    if error:
      print("Failed to tar with error " + str(error))
      return
    tar_digest = hash_file(tar_dest)
    _, error = exec_bash("gzip -n "+tar_dest)
    if error:
      print("Failed to tar with error " + str(error))
      return
    gz_dest = tar_dest+".gz"
    gzip_digest = self.dxfObject.push_blob(tar_dest+".gz")

    # Cleanup
    os.rmdir(tmp_name+"/cvmfs")
    os.rmdir(tmp_name)
    os.unlink(gz_dest)
    return (tar_digest, gzip_digest)
