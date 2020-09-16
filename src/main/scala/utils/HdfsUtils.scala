package utils

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

class HdfsUtils(val fs: FileSystem) {

  def moveFilesInDir(oldPlace: String, newPlace: String): Unit = {
    val filesInOldDir = fs.listFiles(new Path(oldPlace), true)
    while (filesInOldDir.hasNext) {
      var curFile = filesInOldDir.next().getPath
      fs.rename(curFile, new Path(newPlace))
    }
  }

  def deleteFiles(path: String): Boolean = {
    fs.delete(new Path(path), true)
  }

  def mkdir(path: String): Boolean = {
    fs.mkdirs(new Path(path))
  }

  def exists(path: String): Boolean = {
    fs.exists(new Path(path))
  }
}