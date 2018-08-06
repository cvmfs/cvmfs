from datetime import datetime, timezone
from dxf import DXF, hash_file, hash_bytes
import json
import subprocess
import tarfile
import tempfile
import os
import zlib

def exec_bash(cmd):
  process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  output, error = process.communicate()
  return (output, error)
# TODO(steuber): Error handling with throws
class FatManifest:
  def __init__(self, manif):
    self.manif = json.loads(manif)
  def init_cvmfs_layer(self, tar_digest, gz_digest):
    if self.manif["rootfs"]["type"] != "layers":
      print("Cannot inject in rootfs of type " + self.manif["rootfs"]["type"])
      print("Currently only supporting layer rootfs")
      return
    self.manif["rootfs"]["diff_ids"].append(tar_digest)
                                   
    local_time = datetime.now(timezone.utc).astimezone()
    self.manif["history"].append({
      "created":local_time.isoformat(),
      "created_by":"/bin/sh -c #(nop) ADD file:"+tar_digest+" in / ",
      "author":"cvmfs_shrinkwrap",
      "comment": "This change was executed through the CVMFS Shrinkwrap Docker Injector"
    })
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
    if not self.is_cvmfs_prepared():
      print("Cannot inject in unprepated image")
      return
    old_tar_digest = self.manif["config"]["Labels"]["cvmfs_injection_tar"]

    self.manif["container_config"]["Labels"]["cvmfs_injection_tar"] = tar_digest
    self.manif["container_config"]["Labels"]["cvmfs_injection_gz"] = gz_digest
    self.manif["config"]["Labels"]["cvmfs_injection_tar"] = tar_digest
    self.manif["config"]["Labels"]["cvmfs_injection_gz"] = gz_digest

    for i in range(len(self.manif["rootfs"]["diff_ids"])):
      if self.manif["rootfs"]["diff_ids"][i] == old_tar_digest:
        self.manif["rootfs"]["diff_ids"][i] = tar_digest
    
    local_time = datetime.now(timezone.utc).astimezone()
    self.manif["history"].append({
      "created":local_time.isoformat(),
      "created_by":"/bin/sh -c #(nop) UPDATE file: from "+old_tar_digest+" to "+tar_digest+" in / ",
      "author":"cvmfs_shrinkwrap",
      "comment": "This change was executed through the CVMFS Shrinkwrap Docker Injector",
      "empty_layer":True
    })
    
  def is_cvmfs_prepared(self):
    return "cvmfs_injection_gz" in self.manif["config"]["Labels"]\
      and "cvmfs_injection_tar" in self.manif["config"]["Labels"]
  def get_gz_digest(self):
    return self.manif["config"]["Labels"]["cvmfs_injection_gz"]
  def as_JSON(self):
    return json.dumps(self.manif)

class ImageManifest:
  def __init__(self, manif):
    self.manif = json.loads(manif)
  def get_fat_manif_digest(self):
    return self.manif['config']['digest']
  def init_cvmfs_layer(self, layer_digest, layer_size, manifest_digest, manifest_size):
    self.manif["layers"].append({
      'mediaType':'application/vnd.docker.image.rootfs.diff.tar.gzip',
      'size':layer_size,
      'digest':layer_digest
    })
    self.manif["config"]["size"] = manifest_size
    self.manif["config"]["digest"] = manifest_digest
  def inject(self ,old, new, layer_size, manifest_digest, manifest_size):
    for i in range(len(self.manif["layers"])):
      if self.manif["layers"][i]["digest"] == old:
        self.manif["layers"][i]["digest"] = new
        self.manif["layers"][i]["size"] = layer_size
    self.manif["config"]["size"] = manifest_size
    self.manif["config"]["digest"] = manifest_digest
  def as_JSON(self):
    return json.dumps(self.manif)

class DockerInjector:
  def __init__(self, host, repo, alias):
    # TODO(steuber): remove tlsverify
    self.dxfObject = DXF(host, repo, tlsverify=False)
    self.image_manifest = self._get_manifest(alias)
    self.fat_manifest = self._get_fat_manifest(self.image_manifest)

  def setup(self, push_alias):
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
    if not self.fat_manifest.is_cvmfs_prepared():
      print("This image is not correctly prepared for cvmfs injection (lacking Docker labels)")
    gz_digest = self.fat_manifest.get_gz_digest()
    # Write out tar file
    decompress_object = zlib.decompressobj(16+zlib.MAX_WBITS)
    chunk_it = self.dxfObject.pull_blob(gz_digest)
    with tempfile.TemporaryFile() as tmp_file:
      for chunk in chunk_it:
        tmp_file.write(decompress_object.decompress(chunk))
      tmp_file.write(decompress_object.flush())
      tmp_file.seek(0)
      tar = tarfile.TarFile(fileobj=tmp_file)
      # TODO(steuber): TAR checking beforehand (in paticular for ../ paths)
      tar.extractall(dest_dir)
      tar.close()

  def update(self, src_dir, push_alias):
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
      print(tmp_file.name)
      _, error = exec_bash("tar -C "+src_dir+" -cvf "+tmp_file.name+" .")
      if error:
        print("Failed to tar with error " + str(error))
        return
      tar_digest = hash_file(tmp_file.name)
      gz_dest = tmp_file.name+".gz"
      _, error = exec_bash("gzip "+tmp_file.name)
      if error:
        print("Failed to tar with error " + str(error))
        return
      gz_digest = self.dxfObject.push_blob(gz_dest)
      os.unlink(gz_dest)
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
    (readIter, size) = self.dxfObject.pull_blob(self.image_manifest.get_fat_manif_digest(), size=True, chunk_size=4096)
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
    os.mkdir(tmp_name)
    os.mkdir(tmp_name+"/cvmfs")
    tar_dest = "/tmp/"+ident+".tar"
    _, error = exec_bash("tar -C "+tmp_name+" -cvf "+tar_dest+" .")
    if error:
      print("Failed to tar with error " + error)
      return
    tar_digest = hash_file(tar_dest)
    _, error = exec_bash("gzip "+tar_dest)
    if error:
      print("Failed to tar with error " + error)
      return
    gz_dest = tar_dest+".gz"
    gzip_digest = self.dxfObject.push_blob(tar_dest+".gz")
    
    # Cleanup
    os.rmdir(tmp_name+"/cvmfs")
    os.rmdir(tmp_name)
    os.unlink(gz_dest)
    return (tar_digest, gzip_digest)