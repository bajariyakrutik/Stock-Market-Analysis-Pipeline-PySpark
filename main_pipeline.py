# Stock Market Data Processing Pipeline with PySpark - Alpha Vantage Library Version
# Complete implementation from data extraction to Tableau preparation

import os
from alpha_vantage.timeseries import TimeSeries
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import numpy as np
import time
from datetime import datetime, timedelta

# =============================================================================
# 1. PROJECT SETUP AND CONFIGURATION
# =============================================================================

class StockDataPipeline:
    def __init__(self, alpha_vantage_api_key=None):
        # Alpha Vantage API configuration
        if alpha_vantage_api_key is None:
            # You can also set this as an environment variable: export ALPHA_VANTAGE_API_KEY=your_key
            self.api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
            if not self.api_key:
                raise ValueError("Please provide Alpha Vantage API key either as parameter or set ALPHA_VANTAGE_API_KEY environment variable")
        else:
            self.api_key = alpha_vantage_api_key
        
        # Initialize Alpha Vantage TimeSeries
        self.ts = TimeSeries(key=self.api_key, output_format='pandas')
        
        # Initialize Spark Session
        self.spark = SparkSession.builder \
            .appName("StockMarketDataPipeline") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        # Stock symbols for portfolio analysis
        self.stock_symbols = [
            'AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 
            'NVDA', 'META', 'NFLX', 'AMD', 'CRM'
        ]
        
        # Company metadata
        self.company_metadata = {
            'AAPL': {'name': 'Apple Inc.', 'sector': 'Technology', 'industry': 'Consumer Electronics'},
            'GOOGL': {'name': 'Alphabet Inc.', 'sector': 'Technology', 'industry': 'Internet Services'},
            'MSFT': {'name': 'Microsoft Corp.', 'sector': 'Technology', 'industry': 'Software'},
            'AMZN': {'name': 'Amazon.com Inc.', 'sector': 'Consumer Discretionary', 'industry': 'E-commerce'},
            'TSLA': {'name': 'Tesla Inc.', 'sector': 'Consumer Discretionary', 'industry': 'Electric Vehicles'},
            'NVDA': {'name': 'NVIDIA Corp.', 'sector': 'Technology', 'industry': 'Semiconductors'},
            'META': {'name': 'Meta Platforms Inc.', 'sector': 'Technology', 'industry': 'Social Media'},
            'NFLX': {'name': 'Netflix Inc.', 'sector': 'Communication Services', 'industry': 'Streaming'},
            'AMD': {'name': 'Advanced Micro Devices', 'sector': 'Technology', 'industry': 'Semiconductors'},
            'CRM': {'name': 'Salesforce Inc.', 'sector': 'Technology', 'industry': 'Cloud Software'}
        }
        
        # Output directories
        self.raw_data_path = "data/raw_stock_data"
        self.tableau_data_path = "data/tableau_ready"
        
        # Create directories
        os.makedirs(self.raw_data_path, exist_ok=True)
        os.makedirs(self.tableau_data_path, exist_ok=True)

# =============================================================================
# 2. ALPHA VANTAGE DATA EXTRACTION MODULE - USING LIBRARY
# =============================================================================

    def extract_stock_data(self, outputsize='full'):
        """
        Extract stock data using the alpha_vantage library
        outputsize: 'compact' for last 100 days, 'full' for 20+ years of data
        """
        print(f"Starting data extraction using Alpha Vantage library...")
        print(f"API Key: {'*' * 10 + self.api_key[-4:] if len(self.api_key) > 4 else 'Set'}")
        print(f"Output size: {outputsize}")
        
        all_stock_data = []
        failed_symbols = []

        for i, symbol in enumerate(self.stock_symbols):
            print(f"Fetching data for {symbol}... ({i+1}/{len(self.stock_symbols)})")
            
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    # Get daily data using alpha_vantage library
                    data, meta_data = self.ts.get_daily(symbol=symbol, outputsize=outputsize)
                    
                    if data is not None and not data.empty:
                        # Reset index to make date a column
                        data.reset_index(inplace=True)
                        
                        # Rename columns to standard format
                        column_mapping = {
                            'date': 'Date',
                            '1. open': 'Open',
                            '2. high': 'High', 
                            '3. low': 'Low',
                            '4. close': 'Close',
                            '5. volume': 'Volume'
                        }
                        data.rename(columns=column_mapping, inplace=True)
                        
                        # Ensure we have all required columns
                        required_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
                        if all(col in data.columns for col in required_columns):
                            # Keep only required columns
                            data = data[required_columns]
                            
                            # Add metadata
                            metadata = self.company_metadata.get(symbol, {
                                'name': symbol, 
                                'sector': 'Unknown', 
                                'industry': 'Unknown'
                            })
                            
                            data['Symbol'] = symbol
                            data['Company_Name'] = metadata['name']
                            data['Sector'] = metadata['sector']
                            data['Industry'] = metadata['industry']
                            
                            # Ensure date is properly formatted
                            data['Date'] = pd.to_datetime(data['Date'])
                            
                            # Sort by date
                            data = data.sort_values('Date').reset_index(drop=True)
                            
                            all_stock_data.append(data)
                            print(f"✓ Successfully extracted {len(data)} records for {symbol}")
                            print(f"  Date range: {data['Date'].min().date()} to {data['Date'].max().date()}")
                            break
                        else:
                            missing_cols = [col for col in required_columns if col not in data.columns]
                            raise Exception(f"Missing required columns: {missing_cols}")
                            
                    else:
                        print(f"⚠ No data returned for {symbol}")
                        
                except Exception as e:
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 15
                        print(f"⚠ Attempt {attempt + 1} failed for {symbol}: {str(e)}")
                        print(f"Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        print(f"✗ All attempts failed for {symbol}: {str(e)}")
                        failed_symbols.append(symbol)
            
            # Rate limiting: Sleep 12 seconds between requests (5 requests per minute limit)
            if i < len(self.stock_symbols) - 1:
                print("Waiting 12s to respect API rate limits...")
                time.sleep(12)

        if failed_symbols:
            print(f"⚠ Failed to fetch data for: {failed_symbols}")
        
        if not all_stock_data:
            raise Exception("❌ No stock data extracted. Check your API key and network connection.")

        # Combine all data
        print("\nCombining data from all stocks...")
        combined_data = pd.concat(all_stock_data, ignore_index=True)
        
        # Ensure columns are in the right order
        column_order = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Symbol', 'Company_Name', 'Sector', 'Industry']
        combined_data = combined_data[column_order]
        
        # Save raw data
        combined_data.to_csv(f"{self.raw_data_path}/raw_stock_data.csv", index=False)
        
        print(f"\n✅ Data extraction completed successfully!")
        print(f"✓ Total records: {len(combined_data):,}")
        print(f"✓ Stocks: {len(all_stock_data)}/{len(self.stock_symbols)}")
        print(f"✓ Date range: {combined_data['Date'].min().date()} to {combined_data['Date'].max().date()}")
        print(f"✓ Saved to: {self.raw_data_path}/raw_stock_data.csv")
        print(f"✓ Column names: {list(combined_data.columns)}")
        
        return combined_data 

# =============================================================================
# 3. DATA TRANSFORMATION WITH PYSPARK - FIXED
# =============================================================================

    def load_and_clean_data(self):
        """
        Load raw data into Spark and perform initial cleaning
        """
        print("Loading and cleaning data with PySpark...")
        
        # Load data with explicit schema to avoid column naming issues
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(f"{self.raw_data_path}/raw_stock_data.csv")
        
        print("Loaded columns:", df.columns)
        
        # Data cleaning with correct column names
        df_clean = df.filter(
            (col("Close").isNotNull()) & 
            (col("Volume") > 0) & 
            (col("Close") > 0)
        ).select(
            col("Date").cast("date").alias("trade_date"),
            col("Open").cast("double").alias("open_price"),
            col("High").cast("double").alias("high_price"),
            col("Low").cast("double").alias("low_price"),
            col("Close").cast("double").alias("close_price"),
            col("Volume").cast("long").alias("volume"),
            col("Symbol").alias("symbol"),
            col("Company_Name").alias("company_name"),
            col("Sector").alias("sector"),
            col("Industry").alias("industry")
        )
        
        # Add basic derived columns
        df_enhanced = df_clean.withColumn(
            "daily_return", 
            (col("close_price") - col("open_price")) / col("open_price")
        ).withColumn(
            "price_range",
            col("high_price") - col("low_price")
        ).withColumn(
            "price_range_pct",
            (col("high_price") - col("low_price")) / col("open_price")
        )
        
        print(f"✓ Cleaned data: {df_enhanced.count()} records")
        return df_enhanced

# =============================================================================
# 4. TECHNICAL INDICATORS CALCULATION - OPTIMIZED
# =============================================================================

    def calculate_technical_indicators(self, df):
        """
        Calculate technical indicators: Moving Averages, RSI, MACD
        """
        print("Calculating technical indicators...")
        
        # Define window specifications
        window_symbol = Window.partitionBy("symbol").orderBy("trade_date")
        
        # Moving Averages (only calculate what we need)
        df_with_ma = df.withColumn(
            "ma_5", avg("close_price").over(window_symbol.rowsBetween(-4, 0))
        ).withColumn(
            "ma_20", avg("close_price").over(window_symbol.rowsBetween(-19, 0))
        ).withColumn(
            "ma_50", avg("close_price").over(window_symbol.rowsBetween(-49, 0))
        )
        
        # Price momentum indicators
        df_with_momentum = df_with_ma.withColumn(
            "price_change_1d", 
            col("close_price") - lag("close_price", 1).over(window_symbol)
        ).withColumn(
            "price_change_pct_1d",
            (col("close_price") - lag("close_price", 1).over(window_symbol)) / lag("close_price", 1).over(window_symbol)
        )
        
        # Volatility measures
        df_with_volatility = df_with_momentum.withColumn(
            "volatility_20d",
            stddev("daily_return").over(window_symbol.rowsBetween(-19, 0))
        )
        
        # Simplified RSI Calculation
        df_with_rsi = df_with_volatility.withColumn(
            "price_gain",
            when(col("price_change_1d") > 0, col("price_change_1d")).otherwise(0)
        ).withColumn(
            "price_loss",
            when(col("price_change_1d") < 0, abs(col("price_change_1d"))).otherwise(0)
        ).withColumn(
            "avg_gain_14",
            avg("price_gain").over(window_symbol.rowsBetween(-13, 0))
        ).withColumn(
            "avg_loss_14",
            avg("price_loss").over(window_symbol.rowsBetween(-13, 0))
        ).withColumn(
            "rsi",
            when(col("avg_loss_14") == 0, 100)
            .otherwise(100 - (100 / (1 + col("avg_gain_14") / col("avg_loss_14"))))
        )
        
        # Trading signals
        df_final = df_with_rsi.withColumn(
            "ma_signal",
            when(col("close_price") > col("ma_20"), "BUY")
            .when(col("close_price") < col("ma_20"), "SELL")
            .otherwise("HOLD")
        ).withColumn(
            "rsi_signal",
            when(col("rsi") < 30, "OVERSOLD")
            .when(col("rsi") > 70, "OVERBOUGHT")
            .otherwise("NEUTRAL")
        ).withColumn(
            "trend_signal",
            when((col("ma_5") > col("ma_20")) & (col("ma_20") > col("ma_50")), "UPTREND")
            .when((col("ma_5") < col("ma_20")) & (col("ma_20") < col("ma_50")), "DOWNTREND")
            .otherwise("SIDEWAYS")
        )
        
        print("✓ Technical indicators calculated")
        return df_final

# =============================================================================
# 5. PORTFOLIO ANALYSIS - SIMPLIFIED
# =============================================================================

    def calculate_portfolio_metrics(self, df):
        """
        Calculate portfolio-level metrics and performance
        """
        print("Calculating portfolio metrics...")
        
        # Daily portfolio value (equal weights)
        portfolio_daily = df.groupBy("trade_date").agg(
            avg("close_price").alias("avg_portfolio_price"),
            sum("volume").alias("total_volume"),
            avg("daily_return").alias("avg_daily_return"),
            stddev("daily_return").alias("portfolio_volatility"),
            count("symbol").alias("active_stocks"),
            avg("rsi").alias("avg_rsi")
        )
        
        # Calculate cumulative returns
        window_date = Window.orderBy("trade_date")
        portfolio_performance = portfolio_daily.withColumn(
            "cumulative_return",
            sum("avg_daily_return").over(window_date)
        ).withColumn(
            "portfolio_value_normalized",
            100 * (1 + col("cumulative_return"))
        )
        
        print("✓ Portfolio metrics calculated")
        return portfolio_performance

# =============================================================================
# 6. RISK METRICS CALCULATION - SIMPLIFIED
# =============================================================================

    def calculate_risk_metrics(self, df):
        """
        Calculate various risk metrics for each stock
        """
        print("Calculating risk metrics...")
        
        # Calculate metrics by symbol
        risk_metrics = df.groupBy("symbol", "company_name", "sector").agg(
            avg("daily_return").alias("avg_daily_return"),
            stddev("daily_return").alias("volatility"),
            min("close_price").alias("min_price_period"),
            max("close_price").alias("max_price_period"),
            avg("volume").alias("avg_daily_volume"),
            last("close_price").alias("current_price")
        ).withColumn(
            "annualized_return", col("avg_daily_return") * 252
        ).withColumn(
            "annualized_volatility", col("volatility") * sqrt(lit(252))
        ).withColumn(
            "sharpe_ratio", 
            when(col("annualized_volatility") > 0, col("annualized_return") / col("annualized_volatility"))
            .otherwise(0)
        ).withColumn(
            "price_range_pct", 
            (col("max_price_period") - col("min_price_period")) / col("min_price_period")
        )
        
        # Risk categories
        risk_categorized = risk_metrics.withColumn(
            "risk_category",
            when(col("annualized_volatility") < 0.2, "Low Risk")
            .when(col("annualized_volatility") < 0.4, "Medium Risk")
            .otherwise("High Risk")
        ).withColumn(
            "performance_category",
            when(col("annualized_return") > 0.15, "High Performer")
            .when(col("annualized_return") > 0.05, "Average Performer")
            .otherwise("Underperformer")
        )
        
        print("✓ Risk metrics calculated")
        return risk_categorized

# =============================================================================
# 7. MAIN EXECUTION PIPELINE - SIMPLIFIED
# =============================================================================

    def run_pipeline(self):
        """
        Execute the complete data pipeline
        """
        print("=== Starting Stock Market Data Pipeline ===")
        
        try:
            # Step 1: Extract data
            print("\n--- Step 1: Data Extraction ---")
            raw_data = self.extract_stock_data()
            
            # Step 2: Load and clean with Spark
            print("\n--- Step 2: Data Cleaning ---")
            clean_df = self.load_and_clean_data()
            
            # Step 3: Calculate technical indicators
            print("\n--- Step 3: Technical Analysis ---")
            technical_df = self.calculate_technical_indicators(clean_df)
            
            # Step 4: Portfolio analysis
            print("\n--- Step 4: Portfolio Analysis ---")
            portfolio_df = self.calculate_portfolio_metrics(technical_df)
            
            # Step 5: Risk metrics
            print("\n--- Step 5: Risk Analysis ---")
            risk_df = self.calculate_risk_metrics(technical_df)
            
            # Step 6: Save processed data for Tableau
            print("\n--- Step 6: Saving Results ---")
            self.save_tableau_data(technical_df, portfolio_df, risk_df)
            
            print("\n=== Pipeline Completed Successfully ===")
            return technical_df, portfolio_df, risk_df
            
        except Exception as e:
            print(f"\n❌ Pipeline failed with error: {str(e)}")
            raise e

# =============================================================================
# 8. TABLEAU DATA PREPARATION - SIMPLIFIED
# =============================================================================

    def save_tableau_data(self, technical_df, portfolio_df, risk_df):
        """
        Save processed data in formats optimized for Tableau
        """
        print("Preparing data for Tableau...")
        
        try:
            # 1. Main stock data with technical indicators
            print("Saving technical data...")
            technical_df.coalesce(1).write.mode("overwrite") \
                .option("header", "true") \
                .csv(f"{self.tableau_data_path}/stock_technical_data")
            
            # 2. Portfolio performance data
            print("Saving portfolio data...")
            portfolio_df.coalesce(1).write.mode("overwrite") \
                .option("header", "true") \
                .csv(f"{self.tableau_data_path}/portfolio_performance")
            
            # 3. Risk metrics summary
            print("Saving risk metrics...")
            risk_df.coalesce(1).write.mode("overwrite") \
                .option("header", "true") \
                .csv(f"{self.tableau_data_path}/risk_metrics")
            
            # 4. Daily summary for quick analysis
            print("Saving daily summary...")
            daily_summary = technical_df.groupBy("trade_date").agg(
                avg("close_price").alias("market_avg_price"),
                avg("volume").alias("market_avg_volume"),
                avg("rsi").alias("market_avg_rsi"),
                count("symbol").alias("stocks_traded"),
                sum(when(col("ma_signal") == "BUY", 1).otherwise(0)).alias("buy_signals"),
                sum(when(col("ma_signal") == "SELL", 1).otherwise(0)).alias("sell_signals"),
                sum(when(col("rsi_signal") == "OVERSOLD", 1).otherwise(0)).alias("oversold_count"),
                sum(when(col("rsi_signal") == "OVERBOUGHT", 1).otherwise(0)).alias("overbought_count")
            )
            
            daily_summary.coalesce(1).write.mode("overwrite") \
                .option("header", "true") \
                .csv(f"{self.tableau_data_path}/daily_market_summary")
            
            print("✓ All Tableau datasets saved successfully!")
            
        except Exception as e:
            print(f"❌ Error saving Tableau data: {str(e)}")
            raise e

# =============================================================================
# 9. EXECUTION
# =============================================================================

if __name__ == "__main__":
    # Initialize and run pipeline
    # Method 1: Pass API key directly
    # pipeline = StockDataPipeline(alpha_vantage_api_key="YOUR_API_KEY_HERE")
    
    # Method 2: Use environment variable (recommended)
    # Set environment variable: export ALPHA_VANTAGE_API_KEY=your_actual_api_key
    try:
        pipeline = StockDataPipeline()
    except ValueError as e:
        print(f"❌ Configuration Error: {e}")
        print("\n🔑 How to get Alpha Vantage API Key:")
        print("1. Go to: https://www.alphavantage.co/support/#api-key")
        print("2. Sign up for a free account")
        print("3. Get your API key")
        print("4. Install required library: pip install alpha-vantage")
        print("5. Set environment variable: export ALPHA_VANTAGE_API_KEY=your_key")
        print("   Or pass it directly: StockDataPipeline(alpha_vantage_api_key='your_key')")
        exit(1)
    
    try:
        technical_df, portfolio_df, risk_df = pipeline.run_pipeline()
        
        # Display sample results
        print("\n" + "="*50)
        print("SAMPLE RESULTS")
        print("="*50)
        
        print("\n📊 Technical Indicators (Recent 5 records):")
        technical_df.select("trade_date", "symbol", "close_price", "ma_20", "rsi", "ma_signal", "rsi_signal") \
                   .orderBy(desc("trade_date"), "symbol") \
                   .show(5, truncate=False)
        
        print("\n📈 Risk Metrics by Stock:")
        risk_df.select("symbol", "annualized_return", "annualized_volatility", "sharpe_ratio", "risk_category") \
               .orderBy(desc("annualized_return")) \
               .show(10, truncate=False)
        
        print("\n💼 Portfolio Performance (Recent 5 days):")
        portfolio_df.select("trade_date", "avg_portfolio_price", "avg_daily_return", "portfolio_volatility", "avg_rsi") \
                    .orderBy(desc("trade_date")) \
                    .show(5, truncate=False)
        
        print(f"\n✅ Pipeline completed successfully!")
        print(f"📁 Data saved to: {pipeline.tableau_data_path}")
        
        # Display summary statistics
        total_records = technical_df.count()
        date_range = technical_df.select(min("trade_date"), max("trade_date")).collect()[0]
        print(f"📊 Total records processed: {total_records:,}")
        print(f"📅 Date range: {date_range[0]} to {date_range[1]}")
        
    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        pipeline.spark.stop()
        print("\n🔄 Spark session closed.")