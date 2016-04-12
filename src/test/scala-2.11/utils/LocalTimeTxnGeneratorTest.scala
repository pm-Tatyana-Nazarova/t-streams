package utils

import java.util.UUID
import com.bwsw.tstreams.txngenerator.LocalTimeTxnGenerator
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class LocalTimeTxnGeneratorTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  "Generators.getTimeUUID()" should "return unique UUID" in {
    val gen = new LocalTimeTxnGenerator
    var uniqElements = Set[UUID]()
    for (i <- 0 until 100) {
      val prevSize = uniqElements.size

      val uuid: UUID = gen.getTimeUUID()
      uniqElements += uuid

      prevSize shouldEqual uniqElements.size-1
    }
  }
}
