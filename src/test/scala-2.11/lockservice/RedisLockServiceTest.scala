package lockservice

import java.util.concurrent.atomic.AtomicReference
import com.bwsw.tstreams.lockservice.impl._
import com.bwsw.tstreams.lockservice.traits.ILockService
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import scala.collection.mutable.ListBuffer


class RedisLockServiceTest extends FlatSpec with Matchers with BeforeAndAfterAll{

  val flag = new AtomicReference[Int](0)
  val checkVal = new AtomicReference[Boolean](true)

  "Locker.lock(), Locker.unlock()" should "lock concrete reddis path and unlock concrete reddis path" in {
    val path = "/testpath"
    val config = new org.redisson.Config()
    config.useSingleServer().setAddress("localhost:6379")

    val factory1 = new RedisLockServiceFactory("/unittest",config)
    val factory2 = new RedisLockServiceFactory("/unittest",config)
    val factory3 = new RedisLockServiceFactory("/unittest",config)
    val factory4 = new RedisLockServiceFactory("/unittest",config)
    val factory5 = new RedisLockServiceFactory("/unittest",config)

    var lockers = new ListBuffer[ILockService]()
    lockers+=factory1.getLocker(path)
    lockers+=factory2.getLocker(path)
    lockers+=factory3.getLocker(path)
    lockers+=factory4.getLocker(path)
    lockers+=factory5.getLocker(path)

    val threads = ListBuffer[Thread]()
    for (i <- 0 until 5)
      threads += new Thread(getRunnable(lockers(i)))

    threads.foreach(t=>t.start())
    threads.foreach(t=>t.join())

    factory1.closeFactory()
    factory2.closeFactory()
    factory3.closeFactory()
    factory4.closeFactory()
    factory5.closeFactory()

    checkVal.get() shouldEqual true
  }

  def getRunnable(locker: ILockService) = {
    new Runnable {
      override def run(): Unit = {
        var i : Int = 0

        locker.lock()

        flag.set(flag.get()+1)
        val assertVal = flag.get()
        while (i < 5){
          checkVal.set(flag.get() == assertVal && checkVal.get())
          i += 1
          Thread.sleep(1000)
        }

        locker.unlock()
      }
    }
  }
}
