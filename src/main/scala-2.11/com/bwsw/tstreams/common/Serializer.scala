package com.bwsw.tstreams.common


trait Serializer {
  def serialize(value: Any): String
  def deserialize[T: Manifest](value: String) : T
  def deserializeWithManifest[T](value: String, manifest : Manifest[_]): T
  def setIgnoreUnknown(ignore: Boolean)
  def getIgnoreUnknown() : Boolean
}
