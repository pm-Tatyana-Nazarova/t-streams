package lockservice

import com.bwsw.tstreams.lockservice.impl.{ZkServer, ZkLockerFactory, ZkLocker}
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}


class ZkLockerFactoryTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  "LockerFactory.createLocker() and LockerFactory.getLocker()" should
    "create locker and retrieve it from LockerFactory instances storage" in {

    val zkServers = List(ZkServer("localhost", 2181))
    val factory: ZkLockerFactory = new ZkLockerFactory(zkServers, "/unittest_path", zkSessionTimeout = 10)
    factory.createLocker("/stream1")
    factory.createLocker("/stream2")
    factory.createLocker("/stream3")

    val lockerForStream1 = factory.getLocker("/stream1")
    val lockerForStream2 = factory.getLocker("/stream2")
    val lockerForStream3 = factory.getLocker("/stream3")

    val checkVal = lockerForStream1.isInstanceOf[ZkLocker] && lockerForStream2.isInstanceOf[ZkLocker] && lockerForStream3.isInstanceOf[ZkLocker]
    checkVal shouldEqual true
  }
}
