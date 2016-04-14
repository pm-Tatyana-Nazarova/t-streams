package lockservice

import com.bwsw.tstreams.lockservice.impl.{RedisLockServiceFactory,RedisLockService}
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}


class RedisLockServiceFactoryTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  "LockerFactory.createLocker() and LockerFactory.getLocker()" should
    "create locker and retrieve it from LockerFactory instances storage" in {

    val config = new org.redisson.Config()
    config.useSingleServer().setAddress("localhost:6379")

    val factory: RedisLockServiceFactory = new RedisLockServiceFactory(path = "/unit_test", config)
    factory.createLocker("/stream1")
    factory.createLocker("/stream2")
    factory.createLocker("/stream3")

    val lockerForStream1 = factory.getLocker("/stream1")
    val lockerForStream2 = factory.getLocker("/stream2")
    val lockerForStream3 = factory.getLocker("/stream3")

    val checkVal = lockerForStream1.isInstanceOf[RedisLockService] &&
      lockerForStream2.isInstanceOf[RedisLockService] && lockerForStream3.isInstanceOf[RedisLockService]

    factory.closeFactory()

    checkVal shouldEqual true
  }
}
