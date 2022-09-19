

import java.io.File
import java.math.BigInteger
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicLong


class TransactionsGenerator {

  // State Variables
  private val totalUsers = 10_000_000 // 10Mil
  private val veryActiveUsers = 10_000 // 10K
  private val isins = listOf("INF178L01012", "INF178L01013", "INF178L01014", "INF178L01015", "INF178L01016", "INF178L01017", "INF178L01018", "INF178L01019", "INF178L01020",
  "INF178L01021", "INF178L01022", "INF178L01023", "INF178L01024", "INF178L01025", "INF178L01026", "INF178L01027", "INF178L01028")
  private val random = Random()

  private val transactionsGenerated = AtomicLong(0)
  private val numberOfThreads = 20

  fun generateTransactions(numberOfTransactions: Long, fileName: String) {

    File(fileName).delete()

    val bufferedWriter = File(fileName).bufferedWriter()

    val executorService: ExecutorService = ThreadPoolExecutor(
      10, numberOfThreads, 0L, TimeUnit.MILLISECONDS,
      LinkedBlockingQueue()
    )

    val writingTask = Runnable {
      while (transactionsGenerated.incrementAndGet() <= numberOfTransactions) {
        val transaction = getBiasedUserTransaction()
        bufferedWriter.write(transaction.toCsvRow())
      }
    }

    val futures: MutableList<Future<*>> = ArrayList<Future<*>>()

    for (i in 2..numberOfThreads) {
      val f = executorService.submit(writingTask)
      futures.add(f)
    }

    Thread {
      while (true) {
        var allDone = true
        for (future in futures) {
          allDone = allDone and future.isDone // check if future is done
        }
        if (allDone) {
          println("finished with $numberOfTransactions rows")
          bufferedWriter.close()
          executorService.shutdownNow()
          break
        } else {
          Thread.sleep(3000)
          println("Written ${transactionsGenerated.get()} rows - ${transactionsGenerated.get()*100/numberOfTransactions}%")
        }
      }
    }.start()
  }



  private fun getBiasedUserTransaction(): UserTransaction {
    val userIndex = getBiasedUserIndex()
    val transaction = UserTransaction()
    transaction.inf = isins[(userIndex + random.nextInt(5)) % isins.size]
    transaction.userId = getMd5(userIndex.toString())
    transaction.price = (100 + random.nextInt(100000)).toDouble()
    transaction.folio =  "${transaction.userId}${random.nextInt(3)}"
    transaction.timeStamp = Date(Date().time - random.nextInt(365*2)*24*60*60*1000L)
    transaction.unitsAllotted = 5+random.nextDouble(100.0)
    transaction.amount = transaction.price!! * transaction.unitsAllotted!!
    transaction.status = random.nextInt(10)
    return transaction
  }

  private fun getBiasedUserIndex(): Int {
    val isBiased = random.nextInt(10) >= 3
    if (isBiased) {
      return random.nextInt(veryActiveUsers)
    }
    return random.nextInt(totalUsers)
  }

  private fun getMd5(input: String): String {
    return try {

      // Static getInstance method is called with hashing MD5
      val md = MessageDigest.getInstance("MD5")

      // digest() method is called to calculate message digest
      // of an input digest() return array of byte
      val messageDigest = md.digest(input.toByteArray())

      // Convert byte array into signum representation
      val no = BigInteger(1, messageDigest)

      // Convert message digest into hex value
      var hashtext: String = no.toString(16)
      while (hashtext.length < 32) {
        hashtext = "0$hashtext"
      }
      hashtext
    } // For specifying wrong message digest algorithms
    catch (e: NoSuchAlgorithmException) {
      throw RuntimeException(e)
    }
  }
}