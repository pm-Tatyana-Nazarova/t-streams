package utils

import java.util.UUID
import com.bwsw.tstreams.utils.GeneratorsEntity
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class GeneratorsEntityTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  "Generators.getTimeUUID()" should "return unique UUID" in {
    val gen = new GeneratorsEntity
    var uniqElements = Set[UUID]()
    for (i <- 0 until 100) {
      val prevSize = uniqElements.size

      val uuid: UUID = gen.getTimeUUID()
      uniqElements += uuid

      prevSize shouldEqual uniqElements.size-1
    }
  }
}
