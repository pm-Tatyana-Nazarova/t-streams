package lockservice

import java.net.InetSocketAddress
import com.bwsw.tstreams.lockservice.impl.{ZkLockServiceFactory, ZkLockService}
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}


class ZkLockServiceFactoryTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  "LockerFactory.createLocker() and LockerFactory.getLocker()" should
    "create locker and retrieve it from LockerFactory instances storage" in {

    val zkServers = List(new InetSocketAddress("localhost", 2181))
    val factory: ZkLockServiceFactory = new ZkLockServiceFactory(zkServers, "/unittest_path", zkSessionTimeout = 10)
    factory.createLocker("/stream1")
    factory.createLocker("/stream2")
    factory.createLocker("/stream3")

    val lockerForStream1 = factory.getLocker("/stream1")
    val lockerForStream2 = factory.getLocker("/stream2")
    val lockerForStream3 = factory.getLocker("/stream3")

    val checkVal = lockerForStream1.isInstanceOf[ZkLockService] && lockerForStream2.isInstanceOf[ZkLockService] && lockerForStream3.isInstanceOf[ZkLockService]

    factory.closeFactory()

    checkVal shouldEqual true
  }
}
