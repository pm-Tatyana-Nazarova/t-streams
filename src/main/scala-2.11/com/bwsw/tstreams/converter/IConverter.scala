package com.bwsw.tstreams.converter

/**
 * Converter to convert IN type into OUT type
 * @tparam IN
 * @tparam OUT
 */
trait IConverter[IN,OUT] {
  def convert(obj : IN) : OUT
}
