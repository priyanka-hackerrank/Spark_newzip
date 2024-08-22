package com.hackerrank

class MainTest extends TestSparkSession {
  import Main._

  test("Create DataFrame") {
    implicit def spark = _spark
    val df = createDataFrame("data/data.csv")

    assert(df !== null)
    assert(df.count === 10)
    assert(df.columns === Array("id", "order_no"))
  }

  test("Distinct Id 1") {
    val data = Array(
      ("id-1", "order-1"),
      ("id-2", "order-2"),
      ("id-3", "order-3"),
      ("id-3", "order-3")
    )
    val inputDf = createTestDataFrame(data)
    val idDf = distinctID(inputDf)

    assert(idDf !== null)
    assert(idDf.count === 3)
    assert(
      idDf.collect().map(r => r.getAs[String]("id")).sorted === Array(
        "id-1",
        "id-2",
        "id-3"
      )
    )
  }

  test("Distinct Id 2") {
    val data = Array(
      ("id-1", "order-1"),
      ("id-2", "order-a"),
      ("id-3", "order-b"),
      ("id-4", "order-2"),
      ("id-5", "order-1"),
      ("id-5", "order-a")
    )
    val inputDf = createTestDataFrame(data)
    val idDf = distinctID(inputDf)

    assert(idDf !== null)
    assert(idDf.count === 5)
    assert(
      idDf.collect().map(r => r.getAs[String]("id")).sorted === Array(
        "id-1",
        "id-2",
        "id-3",
        "id-4",
        "id-5"
      )
    )
  }

  test("Distinct OrderNo 1") {
    val data = Array(
      ("id-1", "order-1"),
      ("id-2", "order-2"),
      ("id-3", "order-3"),
      ("id-3", "order-3")
    )
    val inputDf = createTestDataFrame(data)
    val orderDf = distinctOrderNo(inputDf)

    assert(orderDf !== null)
    assert(orderDf.count === 3)
    assert(
      orderDf.collect().map(r => r.getAs[String]("order_no")).sorted === Array(
        "order-1",
        "order-2",
        "order-3"
      )
    )
  }

  test("Distinct OrderNo 2") {
    val data = Array(
      ("id-1", "order-1"),
      ("id-2", "order-a"),
      ("id-3", "order-b"),
      ("id-4", "order-2"),
      ("id-5", "order-1"),
      ("id-5", "order-a")
    )
    val inputDf = createTestDataFrame(data)
    val orderDf = distinctOrderNo(inputDf)

    assert(orderDf !== null)
    assert(orderDf.count === 4)
    assert(
      orderDf.collect().map(r => r.getAs[String]("order_no")).sorted === Array(
        "order-1",
        "order-2",
        "order-a",
        "order-b"
      )
    )
  }
}
