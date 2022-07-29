#Create a stocks dataframe

>>> stocks_schema = """
    symbol string,
    Date date,
    Open Double,
    High Double,
    Low Double,
    Close Double,
    Adj_Close Double,
    Volume Double
    """

>>> stocks = spark.read.csv("data/stocks/*.csv",
                            schema=stocks_schema,
                            header=False)

>>> stocks.printSchema()
root
    |-- symbol: string (nullable = true)
    |-- Date: date (nullable = true)
    |-- Open: double (nullable = true)
    |-- High: double (nullable = true)
    |-- Low: double (nullable = true)
    |-- Close: double (nullable = true)
    |-- Adj_Close: double (nullable = true)
        
        
>>> stocks.count()
24197442

>>> stocks.show(5)
+------+----------+------------------+------------------+------------------+------------------+------------------+-------+
|symbol|      Date|              Open|              High|               Low|             Close|         Adj_Close| Volume|
+------+----------+------------------+------------------+------------------+------------------+------------------+-------+
|   HPQ|1962-01-02|0.1312727034091949|0.1312727034091949|0.1241768822073936|0.1241768822073936|0.0068872850388288|2480300|
|   HPQ|1962-01-03|0.1241768822073936|0.1241768822073936|0.1215159520506858|0.1228464171290397| 0.006813489831984| 507300|
|   HPQ|1962-01-04|0.1228464171290397| 0.126837819814682|0.1179680377244949| 0.120185486972332|0.0066659012809395| 845500|
|   HPQ|1962-01-05|0.1197419986128807|0.1197419986128807|0.1175245493650436|0.1175245493650436|0.0065183169208467| 338200|
|   HPQ|1962-01-08|0.1175245493650436|0.1192985102534294|0.1153071075677871|0.1192985102534294|0.0066167060285806| 873700|
+------+----------+------------------+------------------+------------------+------------------+------------------+-------+

# create symbols dataframe

>>> symbols = spark.read.csv("symbols_valid_meta.csv", inferSchema=True, header=True)

>>> symbols.count()
8049

>>> symbols.show(5)
+------+--------------------+----------------+---------------+---+--------------+----------+----------------+----------+-------------+----------+
|Symbol|       Security Name|Listing Exchange|Market Category|ETF|Round Lot Size|Test Issue|Financial Status|CQS Symbol|NASDAQ Symbol|NextShares|
+------+--------------------+----------------+---------------+---+--------------+----------+----------------+----------+-------------+----------+
|     A|Agilent Technolog...|               N|               |  N|           100|         N|            null|         A|            A|         N|
|    AA|Alcoa Corporation...|               N|               |  N|           100|         N|            null|        AA|           AA|         N|
|  AAAU|Perth Mint Physic...|               P|               |  Y|           100|         N|            null|      AAAU|         AAAU|         N|
|  AACG|ATA Creativity Gl...|               Q|              G|  N|           100|         N|               N|      null|         AACG|         N|
|  AADR|AdvisorShares Dor...|               P|               |  Y|           100|         N|            null|      AADR|         AADR|         N|
+------+--------------------+----------------+---------------+---+--------------+----------+----------------+----------+-------------+----------+
only showing top 5 rows

>>> symbols.printSchema()
root
|-- Symbol: string (nullable = true)
|-- Security Name: string (nullable = true)
|-- Listing Exchange: string (nullable = true)
|-- Market Category: string (nullable = true)
|-- ETF: string (nullable = true)
|-- Round Lot Size: string (nullable = true)
|-- Test Issue: string (nullable = true)
|-- Financial Status: string (nullable = true)
|-- CQS Symbol: string (nullable = true)
|-- NASDAQ Symbol: string (nullable = true)
|-- NextShares: string (nullable = true)

#get default bradcast join threshold
>>> spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
'10485760b'


# disable bbroadcast join
>>> spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 0)

stocks.join(
    symbols, stocks.symbol == symbols.Symbol, "inner").\
    select(stocks.symbol, "Security Name").\
    distinct().\
    show(5,truncate=False)
+------+---------------------------------------------------------------------------------------+
|symbol|Security Name                                                                          |
+------+---------------------------------------------------------------------------------------+
|AA    |Alcoa Corporation Common Stock                                                         |
|AACG  |ATA Creativity Global - American Depositary Shares, each representing two common shares|
|AAMC  |Altisource Asset Management Corp Com                                                   |
|AAPL  |Apple Inc. - Common Stock                                                              |
|AAT   |American Assets Trust, Inc. Common Stock                                               |
+------+---------------------------------------------------------------------------------------+
only showing top 5 rows

#set BroadcastJoinThreshold to 1 GB
>>> spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '1073741824b')

# reset BroadcastJoinThreshold to it's default value: 10 MB
>>> spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '10485760b')

>>> stocks.join(
    broadcast(symbols),
    stocks.symbol == symbols.Symbol, "inner").\
    select(stocks.symbol, "Security Name").\
    distinct().\
    show()

+------+---------------------------------------------------------------------------------------+
|symbol|Security Name                                                                          |
+------+---------------------------------------------------------------------------------------+
|AA    |Alcoa Corporation Common Stock                                                         |
|AACG  |ATA Creativity Global - American Depositary Shares, each representing two common shares|
|AAMC  |Altisource Asset Management Corp Com                                                   |
|AAPL  |Apple Inc. - Common Stock                                                              |
|AAT   |American Assets Trust, Inc. Common Stock                                               |
+------+---------------------------------------------------------------------------------------+


