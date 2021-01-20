# Quantitative Finance Engine (qfengine)
An Algorithmic Trading Engine written in Python, with object-oriented & dynamic architecture for Quantitative Trading purposes such as:
  - Managing Data (price, fundamental, alternative) from different financial markets (equities, crypto, etc...) 
  - Research, Develop & Backtest Quantitative Trading Strategies
  - Live (Paper) Trading Strategies Through Live Brokerage API (pending)
  
 The architecture written is inspired by Michael Moore's QSTrader, with major added edits & personal improvements for more complex modeling & data management:
 
 - #### Data Handling
    - *DataHandler* is written as an abstract class, which provide further versatile subclasses such as *BacktestDataHandler*
    - Each derived subclasses of *DataHandler* will take in a list of *DataSource*, or simply *Database*, which can be categorized via:
        - Format: *CSV, MySQL, etc...*
        - Data Type: *Time-Series Prices, Fundamentals, Alternative, etc...*
        - Data Frequency: *Annually, Quarterly, Weekly, Daily, Hourly, Minute, etc...*
        - *ETC...*
    - Implement Vendor APIs:
        - *Alpaca*
        - *IEXCloud*
        - *ETC...*
    - Implement Fundamental Data Management (to be added soon once finalized):
        - *SEC Filings (EDGAR)*
        - *ETC...*
 
 
 - #### Portfolio Construction
    - Alpha Model returns alpha weights (expected returns), generated through:
        - *Predictive model(s)*
        - *Historical averaging(s)*
        - *ETC...*
   
    - Risk Model returns risk factors (covariance) matrix, which can be decomposed to common risk factors & assets' specific risk factors, generated through (a combination of):
        - *Sample Covariance Method(s)*
        - *Random Matrix Theory (RMT) Correlation Filtering (specifically Marchenko-Pastur)*
        - *Stochastic Volatility*
        - *ETC...*
    
    - Optimizer takes in Alpha & Risk signals and perform Portfolio Optimization, with flexibility allowed for additional features such as:
        - *Additional Constraints*
        - *Gross & Net Exposures*
        - *Risk Aversion Ratios*
        - *ETC...*
 
 

  
## Progress Log
- **January 19, 2021**:
  - Creation & First Commit


[MORE DETAILS COMING SOON]
