package utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.alias.CredentialProviderFactory

class Jceks {
  def getValue(jceksPath: String, alias: String): String = {
    val configuration = new Configuration()
    configuration.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, jceksPath)
    val value = configuration.getPassword(alias)
    new String(value)
  }
}