package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.types.{IntegerType, StringType}
import SparkSessionExt._

import org.scalatest.FreeSpec

class DataFrameComparerTest extends FreeSpec with DataFrameComparer with SparkSessionTestWrapper {

  "prints a descriptive error message if it bugs out" in {
    val sourceDF = spark.createDF(
      List(
        ("bob", 1, "uk"),
        ("camila", 5, "peru")
      ),
      List(
        ("name", StringType, true),
        ("age", IntegerType, true),
        ("country", StringType, true)
      )
    )

    val expectedDF = spark.createDF(
      List(
        ("bob", 1, "france"),
        ("camila", 5, "peru")
      ),
      List(
        ("name", StringType, true),
        ("age", IntegerType, true),
        ("country", StringType, true)
      )
    )

    val e = intercept[DatasetContentMismatch] {
      assertSmallDataFrameEquality(sourceDF, expectedDF)
    }
    assert(e.getMessage.indexOf("bob") >= 0)
    assert(e.getMessage.indexOf("camila") >= 0)
  }

  "works well for wide DataFrames" in {
    val sourceDF = spark.createDF(
      List(
        ("bobisanicepersonandwelikehimOK", 1, "uk"),
        ("camila", 5, "peru")
      ),
      List(
        ("name", StringType, true),
        ("age", IntegerType, true),
        ("country", StringType, true)
      )
    )

    val expectedDF = spark.createDF(
      List(
        ("bobisanicepersonandwelikehimNOT", 1, "france"),
        ("camila", 5, "peru")
      ),
      List(
        ("name", StringType, true),
        ("age", IntegerType, true),
        ("country", StringType, true)
      )
    )

    val e = intercept[DatasetContentMismatch] {
      assertSmallDataFrameEquality(sourceDF, expectedDF)
    }
  }

  "also print a descriptive error message if the right side is missing" in {
    val sourceDF = spark.createDF(
      List(
        ("bob", 1, "uk"),
        ("camila", 5, "peru"),
        ("jean-jacques", 4, "france")
      ),
      List(
        ("name", StringType, true),
        ("age", IntegerType, true),
        ("country", StringType, true)
      )
    )

    val expectedDF = spark.createDF(
      List(
        ("bob", 1, "france"),
        ("camila", 5, "peru")
      ),
      List(
        ("name", StringType, true),
        ("age", IntegerType, true),
        ("country", StringType, true)
      )
    )

    val e = intercept[DatasetContentMismatch] {
      assertSmallDataFrameEquality(sourceDF, expectedDF)
    }

    assert(e.getMessage.indexOf("jean-jacques") >= 0)
  }

  "also print a descriptive error message if the left side is missing" in {
    val sourceDF = spark.createDF(
      List(
        ("bob", 1, "uk"),
        ("camila", 5, "peru")
      ),
      List(
        ("name", StringType, true),
        ("age", IntegerType, true),
        ("country", StringType, true)
      )
    )

    val expectedDF = spark.createDF(
      List(
        ("bob", 1, "france"),
        ("camila", 5, "peru"),
        ("jean-claude", 4, "france")
      ),
      List(
        ("name", StringType, true),
        ("age", IntegerType, true),
        ("country", StringType, true)
      )
    )

    val e = intercept[DatasetContentMismatch] {
      assertSmallDataFrameEquality(sourceDF, expectedDF)
    }

    assert(e.getMessage.indexOf("jean-claude") >= 0)
  }

  "checkingDataFrameEquality" - {

    "does nothing if the DataFrames have the same schemas and content" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )

      val expectedDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )

      assertSmallDataFrameEquality(sourceDF, expectedDF)
      assertLargeDataFrameEquality(sourceDF, expectedDF)
    }

    "throws an error if the DataFrames have different schemas" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )

      val expectedDF = spark.createDF(
        List(
          (1, "word"),
          (5, "word")
        ),
        List(
          ("number", IntegerType, true),
          ("word", StringType, true)
        )
      )

      val e = intercept[DatasetSchemaMismatch] {
        assertLargeDataFrameEquality(sourceDF, expectedDF)
      }
      val e2 = intercept[DatasetSchemaMismatch] {
        assertSmallDataFrameEquality(sourceDF, expectedDF)
      }
    }

    "throws an error for incompatible column comparison arguments" in {
      val sourceDF = spark.createDF(
        List(
          ("word", 1),
          ("word", 5)
        ),
        List(
          ("word", StringType, true),
          ("number", IntegerType, true)
        )
      )

      val expectedDF = spark.createDF(
        List(
          (1, "word"),
          (5, "word")
        ),
        List(
          ("number", IntegerType, true),
          ("word", StringType, true)
        )
      )

      val e = intercept[DatasetSchemaMismatch] {
        assertLargeDataFrameEquality(sourceDF, expectedDF, ignoreColumnNames = true, orderedColumnComparison = false)
      }
      val e2 = intercept[DatasetSchemaMismatch] {
        assertSmallDataFrameEquality(sourceDF, expectedDF, ignoreColumnNames = true, orderedColumnComparison = false)
      }
    }

    "throws an error if the DataFrames' columns do not match due to nullability when order is ignored" in {
      val sourceDF = spark.createDF(
        List(
          ("word", 1),
          ("word", 5)
        ),
        List(
          ("word", StringType, true),
          ("number", IntegerType, true)
        )
      )

      val expectedDF = spark.createDF(
        List(
          (1, "word"),
          (5, "word")
        ),
        List(
          ("number", IntegerType, false),
          ("word", StringType, true)
        )
      )

      val e = intercept[DatasetSchemaMismatch] {
        assertLargeDataFrameEquality(sourceDF, expectedDF, orderedColumnComparison = false)
      }
      val e2 = intercept[DatasetSchemaMismatch] {
        assertSmallDataFrameEquality(sourceDF, expectedDF, orderedColumnComparison = false)
      }
    }

    "throws an error if the DataFrames' columns do not match due to data type when order is ignored" in {
      val sourceDF = spark.createDF(
        List(
          ("word", 1),
          ("word", 5)
        ),
        List(
          ("word", StringType, true),
          ("number", IntegerType, true)
        )
      )

      val expectedDF = spark.createDF(
        List(
          ("1", "word"),
          ("5", "word")
        ),
        List(
          ("number", StringType, true),
          ("word", StringType, true)
        )
      )

      val e = intercept[DatasetSchemaMismatch] {
        assertLargeDataFrameEquality(sourceDF, expectedDF, orderedColumnComparison = false)
      }
      val e2 = intercept[DatasetSchemaMismatch] {
        assertSmallDataFrameEquality(sourceDF, expectedDF, orderedColumnComparison = false)
      }
    }

    "performs comparisons of DataFrames with unordered columns" in {
      val sourceDF = spark.createDF(
        List(
          ("word", 1),
          ("word", 5)
        ),
        List(
          ("word", StringType, true),
          ("number", IntegerType, true)
        )
      )

      val expectedDF = spark.createDF(
        List(
          (1, "word"),
          (5, "word")
        ),
        List(
          ("number", IntegerType, true),
          ("word", StringType, true)
        )
      )

      assertLargeDataFrameEquality(sourceDF, expectedDF, orderedColumnComparison = false)
      assertSmallDataFrameEquality(sourceDF, expectedDF, orderedColumnComparison = false)
    }

    "performs comparisons of DataFrames with unordered columns and rows" in {
      val sourceDF = spark.createDF(
        List(
          ("word", 1),
          ("word", 5)
        ),
        List(
          ("word", StringType, true),
          ("number", IntegerType, true)
        )
      )

      val expectedDF = spark.createDF(
        List(
          (5, "word"),
          (1, "word")
        ),
        List(
          ("number", IntegerType, true),
          ("word", StringType, true)
        )
      )

      assertLargeDataFrameEquality(sourceDF, expectedDF, orderedColumnComparison = false)
      assertSmallDataFrameEquality(sourceDF, expectedDF, orderedColumnComparison = false)
    }

    "performs comparisons of DataFrames with unordered columns when nullable differences are ignored" in {
      val sourceDF = spark.createDF(
        List(
          ("word", 1),
          ("word", 5)
        ),
        List(
          ("word", StringType, false),
          ("number", IntegerType, true)
        )
      )

      val expectedDF = spark.createDF(
        List(
          (1, "word"),
          (5, "word")
        ),
        List(
          ("number", IntegerType, true),
          ("word", StringType, true)
        )
      )

      assertLargeDataFrameEquality(sourceDF, expectedDF, ignoreNullable = true, orderedColumnComparison = false)
      assertSmallDataFrameEquality(sourceDF, expectedDF, ignoreNullable = true, orderedColumnComparison = false)
    }

    "throws an error if the DataFrames content is different" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )

      val expectedDF = spark.createDF(
        List(
          (10),
          (5)
        ),
        List(("number", IntegerType, true))
      )

      val e = intercept[DatasetContentMismatch] {
        assertLargeDataFrameEquality(sourceDF, expectedDF)
      }
      val e2 = intercept[DatasetContentMismatch] {
        assertSmallDataFrameEquality(sourceDF, expectedDF)
      }
    }

  }

  "assertSmallDataFrameEquality" - {

    "can perform unordered DataFrame comparisons" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )
      val expectedDF = spark.createDF(
        List(
          (5),
          (1)
        ),
        List(("number", IntegerType, true))
      )
      assertSmallDataFrameEquality(sourceDF, expectedDF, orderedComparison = false)
    }

    "throws an error for unordered DataFrame comparisons that don't match" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )
      val expectedDF = spark.createDF(
        List(
          (5),
          (1),
          (10)
        ),
        List(("number", IntegerType, true))
      )
      val e = intercept[DatasetContentMismatch] {
        assertSmallDataFrameEquality(sourceDF, expectedDF, orderedComparison = false)
      }
    }

  }

}
