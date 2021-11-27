import sys
import pandas as pd
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructField,DateType,TimestampType,StructType,IntegerType,DoubleType
import pyspark.sql.functions as f
spark = SparkSession.builder.appName("SearchKeywordPerformanceCalculator").getOrCreate()

class RevenueComputer:
    def __init__(self, file_path):
        self.file_path = file_path

    def findSearchEngineDomain(self,df):
        df = df.withColumn("comIndex", f.instr("referrer", ".com")+3)
        df = df.withColumn("Search Engine Domain", f.expr("substring(referrer,1,comIndex)"))
        df = df.withColumn('Search Engine Domain', f.regexp_replace('Search Engine Domain', 'http://|www.', ''))
        df = df.select("product_list", "referrer", "Search Engine Domain")
        return df

    def findSearchKeyword(self,df):
        df=df.withColumn("SearchStringIndexQ",f.instr("referrer","q=")).withColumn("SearchStringIndexP",f.instr("referrer","p=")).withColumn("SearchStringIndexK",f.instr("referrer","k="))
        df=df.withColumn("SearchStringIndex",f.greatest("SearchStringIndexQ","SearchStringIndexP","SearchStringIndexK")+2)
        df=df.withColumn("SearchString1",f.expr("substring(referrer,SearchStringIndex,length(referrer))"))
        df=df.withColumn("SearchStringIndex2",f.instr("SearchString1","&")-1)
        df=df.withColumn("SearchStringIndex3",f.when(df.SearchStringIndex2==-1,f.length(df.SearchString1)).otherwise(df.SearchStringIndex2))
        df=df.withColumn("Search Keyword",f.expr("substring(SearchString1,1,SearchStringIndex3)"))
        df = df.withColumn('Search Keyword', f.regexp_replace('Search Keyword', '\\+', ' '))
        df=df.select("product_list","referrer","Search Engine Domain","Search Keyword")
        return df

    def findRevenueFromProductList(self, df):
        df=df.withColumn("Product_List_Array",f.split(df.product_list,','))
        df=df.withColumn("Product",f.explode(df.Product_List_Array))
        df=df.withColumn("Product_Array",f.split(df.Product,';'))
        df=df.withColumn("Product_Revenue",f.element_at(df.Product_Array,4))
        return df

    def computer_revenue(self):

        #read the input file into a dataframe
        prod_df = spark.read.format("csv").option("sep", "\t").option("header", True).load(self.file_path).select("product_list", "referrer")

        #filter out the records that are not needed
        prod_df = prod_df.filter(f.col("referrer").like("%search%"))

        #select only the columns that are needed
        prod_df = prod_df.select("product_list", "referrer")

        ##Finding Search Engine Domain from Referral
        print("Finding Search Engine Domain from referrer")
        prod_df=self.findSearchEngineDomain(prod_df)
        prod_df.show(truncate=False)

        ##Finding Search Keyword from Referral
        print("Finding Search Keyword from referrer")
        prod_df=self.findSearchKeyword(prod_df)
        prod_df.show(truncate=False)

        ##Finding revenue from Product_List
        print("Finding revenue from Product_List")
        prod_df=self.findRevenueFromProductList(prod_df)
        prod_df.show(truncate=False)

        #group by SearchEngineDomain','SearchString'
        print ("Grouping by SearchEngineDomain and SearchString")
        df_agg=prod_df.groupBy(['Search Engine Domain','Search Keyword']).agg(f.sum('Product_Revenue').alias('Revenue'))
        df_agg=df_agg.na.fill(0,"Revenue")
        df_agg.show()

        #filter out esshopzilla.com
        print ("Filtering out esshopzilla.com")
        df_agg = df_agg.filter(f.col("Search Engine Domain")!=f.lit("esshopzilla.com"))
        df_agg.show(truncate=False)

        #sort by Revenue
        print ("Sorting by revenue desc")
        df_agg=df_agg.sort(df_agg.Revenue.desc())
        df_agg.show(truncate=False)

        #Saving data to file
        print("Saving result to output file")
        today = str(date.today())
        output_path='s3n://revenuecalculator/'+today+'_SearchKeywordPerformance.tab'
        print ("Output file path is "+ output_path)
        df_agg.toPandas().to_csv(output_path, sep='\t', index=False)



def main():
    print("Starting the application to calculate the revenue per keyword search")
    try:
       path= str(sys.argv[1])
       print ("Path of the input file is "+path)
       rc = RevenueComputer(path)
       rc.computer_revenue()
    except:
        print("An exception has occurred during the execution of the program. Please check the logs for more details")
    print("The application to calculate the revenue per keyword search completed successfully")

if __name__ == "__main__":
    main()






