package txngenerator

import java.util.UUID
import com.bwsw.tstreams.generator.LocalTimeUuidGenerator
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class LocalTimeUuidGeneratorTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  "LocalTimeUuidGenerator.getTimeUUID()" should "return unique UUID" in {
    val gen = new LocalTimeUuidGenerator
    var uniqElements = Set[UUID]()
    for (i <- 0 until 100) {
      val prevSize = uniqElements.size

      val uuid: UUID = gen.getTimeUUID()
      uniqElements += uuid

      prevSize shouldEqual uniqElements.size-1
    }
  }
}
