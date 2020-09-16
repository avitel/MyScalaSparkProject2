package utils

import java.util.Properties
import java.io.{BufferedReader, FileInputStream, IOException, InputStreamReader, Reader}

class FsUtils {
	def getPropertyFile(filename: String): Properties = {
		val wrapMatching: Properties = new Properties()
		var file: Reader = null
		try {
			file = new BufferedReader(new InputStreamReader(new FileInputStream(filename)))
			wrapMatching.load(file)
		}catch {
			case e:IOException => throw e
		}
		finally {
			if (file != null) file.close()
		}
		wrapMatching
	}
}