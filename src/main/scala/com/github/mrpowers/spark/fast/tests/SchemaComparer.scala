package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.types.{StructField, StructType}

object SchemaComparer {

  def equals(s1: StructType, s2: StructType, ignoreNullable: Boolean = false, ignoreColumnNames: Boolean = false, orderedColumnComparison: Boolean = true): Boolean = {
    if (s1.length != s2.length) {
      false
    } else if (orderedColumnComparison) {
      orderedEquals(s1, s2, ignoreNullable, ignoreColumnNames)
    } else {
      unorderedEquals(s1, s2, ignoreNullable, ignoreColumnNames)
    }
  }

  /**
   * Performs equality check between schemas that are known to be equal length.
   */
  private def orderedEquals(s1: StructType, s2: StructType, ignoreNullable: Boolean, ignoreColumnNames: Boolean): Boolean = {
    val structFields: Seq[(StructField, StructField)] = s1.zip(s2)
    structFields.forall { t =>
      ((t._1.nullable == t._2.nullable) || ignoreNullable) &&
        ((t._1.name == t._2.name) || ignoreColumnNames) &&
        (t._1.dataType == t._2.dataType)
    }
  }

  /**
   * Performs equality check between schemas that are known to be equal length, ignoring column order.
   * Cannot ignore column names.
   */
  private def unorderedEquals(s1: StructType, s2: StructType, ignoreNullable: Boolean, ignoreColumnNames: Boolean): Boolean = {
    if (ignoreColumnNames) {
      throw DatasetSchemaMismatch(
        """Cannot ignore column names when comparing columns out of order.
      Set `ignoreColumnNames = false` or `orderedColumnComparison = true`.""")
    }
    throw DatasetSchemaMismatch("Unimplemented") // TODO
  }
}
