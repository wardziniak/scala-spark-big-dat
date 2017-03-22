package timeusage

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {

  def initializeTimeUsage(): Boolean =
    try {
      TimeUsage
      true
    } catch {
      case ex: Throwable =>
        println(ex.getMessage)
        ex.printStackTrace()
        false
    }

  override def afterAll(): Unit = {
    assert(initializeTimeUsage(), " -- did you fill in all the values in TimeUsage (conf, sc, wikiRdd)?")
    import TimeUsage._
    spark.stop()
  }

//  test("'dfSchema' should create List of Column from names") {
//    assert(initializeTimeUsage(), " -- did you fill in all the values in TimeUsage (conf, sc, wikiRdd)?")
//    import TimeUsage.dfSchema
//    val columnNames = List("col1", "col2", "col3", "col4")
//    val someColumns = dfSchema(columnNames).toList
//    assert(someColumns.head.name == "col1")
//    assert(someColumns(1).name == "col2")
//    assert(someColumns(2).name == "col3")
//    assert(someColumns(3).name == "col4")
//  }
//
//  test("'row' An RDD Row compatible with the schema produced by `dfSchema`") {
//    assert(initializeTimeUsage(), " -- did you fill in all the values in TimeUsage (conf, sc, wikiRdd)?")
//    import TimeUsage.row
//    val strRow = List("20030100014427","1","-1","40","2","1","21","33250")
//    val sampleRow = row(strRow)
//    assert(sampleRow.get(0).asInstanceOf[String] == "20030100014427")
//    assert(sampleRow.get(1).asInstanceOf[Double] == 1)
//    assert(sampleRow.get(2).asInstanceOf[Double] == -1)
//    assert(sampleRow.get(3).asInstanceOf[Double] == 40)
//    assert(sampleRow.get(4).asInstanceOf[Double] == 2)
//    assert(sampleRow.get(5).asInstanceOf[Double] == 1)
//    assert(sampleRow.get(6).asInstanceOf[Double] == 21)
//    assert(sampleRow.get(7).asInstanceOf[Double] == 33250)
//  }
//
//  test("'read' The read DataFrame along with its column names.") {
//    assert(initializeTimeUsage(), " -- did you fill in all the values in TimeUsage (conf, sc, wikiRdd)?")
//    import TimeUsage._
//
//    val (columns, initDf) = read("/timeusage/atussum_2rows.csv")
//    val rows = initDf.collect()
//    assert(rows.head.get(0).asInstanceOf[String] == "\"20030100013280\"")
//    assert(rows.head.get(1).asInstanceOf[Double] == 1)
//    assert(rows.head.get(2).asInstanceOf[Double] == -1)
//    assert(rows.head.get(3).asInstanceOf[Double] == 44)
//    assert(rows.head.get(4).asInstanceOf[Double] == 2)
//    assert(rows.head.get(5).asInstanceOf[Double] == 2)
//    assert(rows.head.get(6).asInstanceOf[Double] == 60)
//    assert(rows.head.get(7).asInstanceOf[Double] == 2)
//    assert(rows.head.get(8).asInstanceOf[Double] == 2)
//    assert(rows.head.get(9).asInstanceOf[Double] == -1)
//  }
//
//  test("'classifiedColumns' The initial data frame columns partitioned in three groups.") {
//    assert(initializeTimeUsage(), " -- did you fill in all the values in TimeUsage (conf, sc, wikiRdd)?")
//    import TimeUsage._
//    val columnsNames = List("t01", "t03", "t05", "t1805", "t11", "t02", "t04", "t06", "t07", "t08", "t09", "t10", "t12", "t13", "t14", "t15", "t16", "t18", "t1801", "t1803")
//
//    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columnsNames)
//
//    assert(primaryNeedsColumns == List(new Column("t01"), new Column("t03"), new Column("t11"), new Column("t1801"), new Column("t1803")))
//    assert(workColumns == List(new Column("t05"), new Column("t1805")))
//
//  }
//
//  test("'timeUsageSummary'") {
//    assert(initializeTimeUsage(), " -- did you fill in all the values in TimeUsage (conf, sc, wikiRdd)?")
//    import TimeUsage._
//
//    val (columns, initDf) = read("/timeusage/atussum_100rows.csv")
//    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
//    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
//    summaryDf.show()
//  }
//
  test("'timeUsageGrouped'") {
    assert(initializeTimeUsage(), " -- did you fill in all the values in TimeUsage (conf, sc, wikiRdd)?")
    import TimeUsage._

    val (columns, initDf) = read("/timeusage/atussum_100rows.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    //summaryDf.show()
    val finalDf = timeUsageGrouped(summaryDf)
    finalDf.show(50)
    println("dupa")
  }
//
//  test("'timeUsageGroupedSql'") {
//    assert(initializeTimeUsage(), " -- did you fill in all the values in TimeUsage (conf, sc, wikiRdd)?")
//    import TimeUsage._
//
//    val (columns, initDf) = read("/timeusage/atussum_100rows.csv")
//    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
//    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
//    val sqlSummaryDf = timeUsageGroupedSql(summaryDf)
//    sqlSummaryDf.show()
//    println("dupa")
//    println(sqlSummaryDf.getClass)
//  }
//
//  test("'timeUsageSummaryTyped'") {
//    assert(initializeTimeUsage(), " -- did you fill in all the values in TimeUsage (conf, sc, wikiRdd)?")
//    import TimeUsage._
//
//    val (columns, initDf) = read("/timeusage/atussum_100rows.csv")
//    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
//    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
//    val summaryDs = timeUsageSummaryTyped(summaryDf)
//    summaryDs.show()
//  }

  test("'timeUsageGroupedTyped'") {
    assert(initializeTimeUsage(), " -- did you fill in all the values in TimeUsage (conf, sc, wikiRdd)?")
    import TimeUsage._

    val (columns, initDf) = read("/timeusage/atussum.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    val summaryDs = timeUsageSummaryTyped(summaryDf)
    val groupedDs = timeUsageGroupedTyped(summaryDs)
    groupedDs.show()

//    import org.apache.spark.sql.expressions.scalalang.typed
//    import spark.implicits._
//    val ds1 = summaryDs.groupByKey(row => (row.working, row.sex, row.age))
//   //     .mapGroups((key, rows) => (key._1, key._2, key._3, rows))
//      .agg(round(typed.avg[TimeUsageRow](_.primaryNeeds), 1).as[Double].name("primaryNeeds"),
//        round(typed.avg[TimeUsageRow](_.work), 1).as[Double].name("work"),
//        round(typed.avg[TimeUsageRow](_.other), 1).as[Double].name("other"))
//      .map(row => TimeUsageRow(row._1._1, row._1._2, row._1._3, row._2, row._3, row._4))
//      //.map((key, primaryNeeds, work, other) => (key._1, key._2, key._3, primaryNeeds, work, other))
//    ds1.show()
  }

}
