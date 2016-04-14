package lockservice

import java.net.InetSocketAddress

import com.bwsw.tstreams.lockservice.impl.{ZkLockServiceFactory, ZkLockService}
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable.ListBuffer

class ZkLockServiceTest extends FlatSpec with Matchers with BeforeAndAfterAll{

  val flag = new AtomicReference[Int](0)
  val checkVal = new AtomicReference[Boolean](true)

  "Locker.lock(), Locker.unlock()" should "lock concrete zk path and unlock concrete zk path" in {
    val path = "/testpath"
    val factory1 = new ZkLockServiceFactory(List(new InetSocketAddress("localhost",2181)),"/unittest",10)
    val factory2 = new ZkLockServiceFactory(List(new InetSocketAddress("localhost",2181)),"/unittest",10)
    val factory3 = new ZkLockServiceFactory(List(new InetSocketAddress("localhost",2181)),"/unittest",10)
    val factory4 = new ZkLockServiceFactory(List(new InetSocketAddress("localhost",2181)),"/unittest",10)
    val factory5 = new ZkLockServiceFactory(List(new InetSocketAddress("localhost",2181)),"/unittest",10)
    factory1.createLocker(path)
    factory2.createLocker(path)
    factory3.createLocker(path)
    factory4.createLocker(path)
    factory5.createLocker(path)

    var lockers = new ListBuffer[ZkLockService]()
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

  def getRunnable(locker: ZkLockService) = {
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
