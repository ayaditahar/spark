#Create a stocks dataframe

stocks_path = "/home/ahmed/data/stocks"
stocks_schema = """
    symbol string,
    Date date,
    Open Double,
    High Double,
    Low Double,
    Close Double, 
    Adj_Close Double,	
    Volume Double
"""
stocks = spark.read.csv(stocks_path,
                        schema=stocks_schema,
                        header=False)
stocks.printSchema()
stocks.count()

symbols_path = "/home/ahmed/data/symbols_valid_meta.csv"
symbols = spark.read.csv(symbols_path, 
                         inferSchema=True, 
                         header=True)
symbols.count()
symbols.show()

# disable bbroadcast join
stocks.join(
    symbols, stocks.symbol == symbols.Symbol, "inner").show()
