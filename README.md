# 📈 Stock Market Data Processing Pipeline

A comprehensive data engineering pipeline that extracts, transforms, and prepares stock market data for advanced analytics and visualization. Built with PySpark for scalable processing and optimized for Tableau dashboard creation.

![Pipeline Status](https://img.shields.io/badge/Status-Production%20Ready-brightgreen)
![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.0%2B-orange)
![Alpha Vantage](https://img.shields.io/badge/Data%20Source-Alpha%20Vantage-red)

**🎯 [Interactive Dashboards Live Demo](https://public.tableau.com/app/profile/krutik.bajariya/vizzes)**

## 🎯 Overview

This pipeline processes **25+ years of historical stock data** for 10 major technology stocks, calculating technical indicators, risk metrics, and portfolio performance analytics. The output is optimized for creating professional financial dashboards in Tableau.

### Key Features
- 🔄 **Automated Data Extraction** from Alpha Vantage API
- ⚡ **Scalable Processing** with Apache PySpark
- 📊 **Technical Analysis** (RSI, Moving Averages, Trading Signals)
- 💼 **Portfolio Analytics** (Risk metrics, Sharpe ratios, Performance tracking)
- 📈 **Tableau-Ready Output** (Clean CSV files for visualization)
- 🛡️ **Robust Error Handling** with retry logic and rate limiting

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Alpha Vantage │───▶│  Data Ingestion │───▶│ PySpark Engine  │───▶│ Tableau Ready   │
│      API        │    │   & Validation  │    │   Processing    │    │   Datasets      │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │                        │
                                ▼                        ▼                        ▼
                       ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
                       │  Raw CSV Data   │    │ Technical       │    │ 4 Dashboard     │
                       │ (Stock Prices)  │    │ Indicators      │    │ Ready csv Files │
                       └─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 📊 Sample Output

**Recent Pipeline Run Results:**
```
✅ Pipeline completed successfully!
📊 Total records processed: 55,452
📅 Date range: 1999-11-01 to 2025-05-23
📁 Data saved to: data/tableau_ready/

📈 Top Performing Stocks:
- NFLX: 23.3% annualized return
- CRM: 23.2% annualized return  
- AMZN: 13.1% annualized return
```

## 🚀 Quick Start

### Prerequisites
```bash
# Required Python packages
pip install alpha-vantage pandas pyspark numpy

# Get your free Alpha Vantage API key
# Visit: https://www.alphavantage.co/support/#api-key
```

### Installation & Setup
```bash
# 1. Clone the repository
git clone https://github.com/bajariyakrutik/Stock-Market-Analysis-Pipeline-PySpark.git
cd Stock-Market-Analysis-Pipeline-PySpark

# 2. Set your API key (choose one method)
export ALPHA_VANTAGE_API_KEY=your_actual_api_key

# OR edit the script directly
# pipeline = StockDataPipeline(alpha_vantage_api_key="your_key")

# 3. Run the pipeline
python main_pipeline.py
```

### Expected Runtime
- **Compact Mode** (last 100 days): ~3-5 minutes
- **Full Mode** (25+ years): ~15-20 minutes
- **Rate Limiting**: 12 seconds between API calls (Alpha Vantage requirement)

## 📁 Project Structure

```
stock-market-pipeline/
├── main_pipeline.py           # Main pipeline script
├── README.md                  # This file
├── requirements.txt           # Python dependencies
└── data/
    ├── raw_stock_data/       # Raw CSV from API
    ├── processed_stock_data/ # Intermediate processing
    └── tableau_ready/        # Final dashboard datasets
        ├── stock_technical_data.csv      # Main dataset (55K+ records)
        ├── portfolio_performance.csv     # Daily portfolio metrics
        ├── risk_metrics.csv              # Stock risk analysis
        └── daily_market_summary.csv      # Market-wide statistics

```

## 🔧 Configuration

### Stock Symbols (Configurable)
Currently tracking 10 major tech stocks:
```python
stocks = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 
          'NVDA', 'META', 'NFLX', 'AMD', 'CRM']
```

### Technical Indicators
- **Moving Averages**: 5, 20, 50-day periods
- **RSI**: 14-day Relative Strength Index
- **Volatility**: 20-day rolling standard deviation
- **Trading Signals**: BUY/SELL/HOLD based on MA crossovers
- **Risk Categories**: Low/Medium/High based on volatility

### Data Output Schema

#### 1. **stock_technical_data.csv** (Main Dataset)
| Column | Type | Description |
|--------|------|-------------|
| trade_date | Date | Trading date |
| symbol | String | Stock ticker symbol |
| close_price | Float | Daily closing price |
| open_price | Float | Daily opening price |
| high_price | Float | Daily high price |
| low_price | Float | Daily low price |
| volume | Integer | Trading volume |
| ma_5 | Float | 5-day moving average |
| ma_20 | Float | 20-day moving average |
| ma_50 | Float | 50-day moving average |
| rsi | Float | Relative Strength Index |
| daily_return | Float | Daily return percentage |
| ma_signal | String | BUY/SELL/HOLD signal |
| rsi_signal | String | OVERSOLD/OVERBOUGHT/NEUTRAL |
| company_name | String | Full company name |
| sector | String | Business sector |
| industry | String | Industry classification |

#### 2. **risk_metrics.csv**
| Column | Type | Description |
|--------|------|-------------|
| symbol | String | Stock ticker |
| annualized_return | Float | Annual return percentage |
| annualized_volatility | Float | Annual volatility |
| sharpe_ratio | Float | Risk-adjusted return |
| risk_category | String | Low/Medium/High Risk |
| performance_category | String | Performance classification |

#### 3. **portfolio_performance.csv**
| Column | Type | Description |
|--------|------|-------------|
| trade_date | Date | Date |
| avg_portfolio_price | Float | Average portfolio price |
| avg_daily_return | Float | Portfolio daily return |
| portfolio_volatility | Float | Portfolio volatility |
| avg_rsi | Float | Average RSI across stocks |

#### 4. **daily_market_summary.csv**
| Column | Type | Description |
|--------|------|-------------|
| trade_date | Date | Date |
| market_avg_price | Float | Market average price |
| market_avg_volume | Float | Market average volume |
| buy_signals | Integer | Number of BUY signals |
| sell_signals | Integer | Number of SELL signals |
| oversold_count | Integer | Stocks in oversold territory |
| overbought_count | Integer | Stocks in overbought territory |

### 🎯 **View Live Dashboards**
**🔗 [Interactive Tableau Dashboards - Live Demo](https://public.tableau.com/app/profile/krutik.bajariya/vizzes)**

*Click the link above to explore interactive dashboards built with this pipeline's data*

### Featured Dashboard Types
1. **📊 Stock Performance Overview** - Price trends and current positions
2. **🎯 Technical Analysis Dashboard** - RSI, moving averages, signals
3. **💼 Portfolio Risk Analysis** - Risk-return matrices, Sharpe ratios
4. **📈 Portfolio Performance Tracking** - Returns, volatility trends
5. **🔥 Market Sentiment Dashboard** - Trading signals, market indicators

### Key Visualizations Created
- **Multi-line time series** of stock prices (25+ years of data)
- **Risk-return scatter plots** with Sharpe ratio sizing
- **RSI heatmaps** with overbought/oversold zones
- **Trading signal distributions** and trend analysis
- **Portfolio value tracking** with benchmark comparisons

## ⚡ Performance & Scalability

### Current Performance Metrics
- **Data Volume**: 55,452+ records processed
- **Time Range**: 25+ years (1999-2025)
- **Processing Speed**: ~2,500 records/minute
- **Memory Usage**: Optimized with PySpark partitioning
- **API Efficiency**: 12-second rate limiting for reliability

### Scaling Options
- **More Stocks**: Add symbols to `stock_symbols` list
- **Additional Indicators**: Extend technical analysis functions  
- **Real-time Processing**: Integrate with streaming APIs
- **Cloud Deployment**: Deploy on AWS EMR, Databricks, or Google Dataproc
- **Database Output**: Replace CSV with PostgreSQL, MySQL, or data warehouse

## 🛠️ Advanced Usage

### Custom Stock Selection
```python
# Edit main_pipeline.py
pipeline.stock_symbols = ['AAPL', 'TSLA', 'YOUR_STOCK']
```

### Different Time Periods
```python
# For recent data only (faster processing)
pipeline.extract_stock_data(outputsize='compact')  # Last 100 days

# For complete history (more comprehensive)
pipeline.extract_stock_data(outputsize='full')     # 20+ years
```

### Custom Technical Indicators
```python
# Add your own indicators in calculate_technical_indicators()
df_custom = df.withColumn(
    "custom_indicator", 
    your_calculation_logic_here
)
```

## 🔍 Troubleshooting

### Common Issues & Solutions

#### API Key Issues
```bash
Error: "Please provide Alpha Vantage API key"
Solution: 
- Get free key from https://www.alphavantage.co/support/#api-key
- Set environment variable: export ALPHA_VANTAGE_API_KEY=your_key
```

#### Rate Limiting
```bash
Error: "API Rate Limit exceeded"
Solution: 
- Pipeline automatically handles this with 12-second delays
- For faster processing, consider Alpha Vantage premium plan
```

#### Memory Issues
```bash
Error: "OutOfMemoryError" 
Solution:
- Use 'compact' mode for smaller datasets
- Increase Spark memory: .config("spark.driver.memory", "4g")
```

#### Column Naming Issues
```bash
Error: "Column cannot be resolved"
Solution:
- Check CSV file structure in data/raw_stock_data/
- Verify Alpha Vantage API response format
- Run with debug output enabled
```

## 📊 Sample Outputs & Use Cases

### Financial Analysis Use Cases
- **Portfolio Optimization**: Use Sharpe ratios and risk metrics
- **Technical Trading**: Implement RSI and MA-based strategies  
- **Risk Management**: Monitor volatility and correlation patterns
- **Performance Attribution**: Track individual stock contributions
- **Market Timing**: Analyze buy/sell signal effectiveness

### Business Intelligence Applications
- **Executive Dashboards**: High-level portfolio performance
- **Risk Reporting**: Regulatory compliance and risk monitoring
- **Client Reporting**: Investment performance summaries
- **Research Analysis**: Historical trend analysis and forecasting

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🔗 Resources & Documentation

### External APIs
- [Alpha Vantage API Documentation](https://www.alphavantage.co/documentation/)
- [Alpha Vantage Python Library](https://github.com/RomelTorres/alpha_vantage)

### Technologies Used
- [Apache PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Tableau Documentation](https://help.tableau.com/)

### Financial Analysis
- [Technical Analysis Indicators](https://www.investopedia.com/technical-analysis-4689657)
- [Modern Portfolio Theory](https://www.investopedia.com/modern-portfolio-theory-4773129)
- [Risk Metrics Explained](https://www.investopedia.com/financial-ratios-4689817)

---
*Last updated: May 2025*