package spark35
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object sparkobj {
	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					val spark =  SparkSession.builder().getOrCreate()
					import spark.implicits._


					println("=================TASK 1====================")
					val readxmldata=spark.read.format("com.databricks.spark.xml").option("rowTag","POSLog").load("file:///C:/data/complexdata/transactions.xml")
					readxmldata.show()
					//readxmldata.printSchema()
					val flattenxml = readxmldata
					.withColumn("Transaction",explode(col("Transaction")))
					.withColumn("LineItem",explode(col("Transaction.RetailTransaction.LineItem")))
					.withColumn("Total", explode(col("Transaction.RetailTransaction.Total")))
					.select("Transaction.BusinessDayDate", 
							"Transaction.ControlTransaction.OperatorSignOff.*",
							"Transaction.ControlTransaction.ReasonCode",
							"Transaction.ControlTransaction._Version",
							"Transaction.CurrencyCode",
							"Transaction.EndDateTime",
							"Transaction.OperatorID._OperatorName",
							"Transaction.OperatorID._VALUE",
							"Transaction.RetailStoreID",
							"Transaction.RetailTransaction.ItemCount",
							"Transaction.RetailTransaction.ReceiptDateTime",
							"Transaction.RetailTransaction.TransactionCount",
							"Transaction.RetailTransaction._Version",
							"Transaction.RetailTransaction.ItemCount",
							"Transaction.RetailTransaction.PerformanceMetrics.*",
							"Transaction.SequenceNumber",
							"Transaction.WorkstationID",
							"LineItem.Sale.*",
							"LineItem.Sale.Itemizers._FoodStampable",
							"LineItem.Sale.Itemizers._Itemizer6",
							"LineItem.Sale.Itemizers._Itemizer8",
							"LineItem.Sale.Itemizers._Tax1",
							"LineItem.Sale.Itemizers._VALUE",
							"LineItem.Sale.MerchandiseHierarchy._DepartmentDescription",
							"LineItem.Sale.MerchandiseHierarchy._Level",
							"LineItem.Sale.MerchandiseHierarchy._VALUE",
							"LineItem.Sale.POSIdentity.*",
							"LineItem.SequenceNumber",
							"LineItem.Tax.Amount",
							"LineItem.Tax.Percent",
							"LineItem.Tax.Reason",
							"LineItem.Tax.TaxableAmount",
							"LineItem.Tax._TaxDescription",
							"LineItem.Tax._TaxID",
							"LineItem.Tender.Amount",
							"LineItem.Tender.Authorization.*",	
							"LineItem.Tender.OperatorSequence",
							"LineItem.Tender.TenderID",
							"LineItem.Tender._TenderDescription",
							"LineItem.Tender._TenderType",
							"LineItem.Tender._TypeCode",
							"Total._TotalType",
							"Total._VALUE"
							).drop("LineItem").drop("Total")

					flattenxml.printSchema()
					flattenxml.show()
					
					println("=================TASK 2====================")
					val readjson1=spark.read.format("json").option("multiline","true").load("file:///c:/data/complexdata/users.json")
					readjson1.printSchema()
					val flattenjson1=readjson1.withColumn("users",explode(col("users"))).select(col("page"),col("per_page"),col("total"),col("total_pages"),col("users.user.*"))
					flattenjson1.printSchema()
					flattenjson1.show()
					val shrinkjson=flattenjson1
					.groupBy(col("page"),col("per_page"),col("total"),col("total_pages"))
					.agg(
							collect_list(
									struct(
											col("emailAddress"),
											col("firstName"),
											col("lastName"),
											col("phoneNumber"),
											col("userId"))).as("users"))
					shrinkjson.printSchema()
					shrinkjson.show()
	}
}