package com.bwsw.tstreams.common.serializer

import java.lang.reflect.{ParameterizedType, Type}

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

class JsonSerializer extends Serializer {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def serialize(value: Any): String = {
    import java.io.StringWriter
    val writer = new StringWriter()
    mapper.writeValue(writer, value)
    writer.toString
  }

  def deserialize[T: Manifest](value: String): T =
    mapper.readValue(value, typeReference[T])

  //WorkAround for Observer
  //Works with class hierarchies where we pass manifest of subclass(_) and retrieve an instance of superclass (T) (but the actual type is the subclass type)
  def deserializeWithManifest[T](value: String, manifest : Manifest[_]): T =
    mapper.readValue(value, new TypeReference[T] {
      override def getType = typeFromManifest(manifest)
    })

  private def typeReference[T: Manifest] = new TypeReference[T] {
    override def getType = typeFromManifest(manifest[T])
  }

  private def typeFromManifest(m: Manifest[_]): Type = {
    if (m.typeArguments.isEmpty) {
      m.runtimeClass
    }
    else new ParameterizedType {
      def getRawType = m.runtimeClass

      def getActualTypeArguments = m.typeArguments.map(typeFromManifest).toArray

      def getOwnerType = null
    }
  }

  override def setIgnoreUnknown(ignore: Boolean): Unit = {
    if (ignore) {
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    } else {
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
    }
  }

  override def getIgnoreUnknown(): Boolean = {
    !((mapper.getDeserializationConfig.getDeserializationFeatures & DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES.getMask) == DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES.getMask)
  }
}