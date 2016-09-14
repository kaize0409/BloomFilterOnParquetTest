/**
  * Created by kaiser on 16/9/1.
  */
object TransferUtil {

  def bigIntTransfer(numString: String): BigInt = {
    if ("".equals(numString)) {
      0
    } else {
      BigInt.apply(numString)
    }
  }

  def intTransfer(numString: String): Int = {
    if ("".equals(numString)) {
      0
    } else {
      numString.toInt
    }
  }

  def patitionKeyTransfer(numString: String): String = {
    if ("".equals(numString)) {
      "NULL"
    } else {
      numString.substring(numString.length - 1)
    }

  }
}
