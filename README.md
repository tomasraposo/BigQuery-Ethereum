# BigQuery-Ethereum
Analysis of ethereum transactions and smart contracts available on BigQuery

A subset of the data available on BigQuery was written to HDFS under /data/ethereum. The blocks, contracts and transactions tables have been pulled down and stripped of unnecessary fields. A set of scams, both active and inactive, run on the Ethereum network, were also downloaded via etherscamDB and are available under the same HDFS folder.

For this I wrote a series of Map/Reduce (Hadoop) jobs that process data and generate the required output to answer the analysis questions: 

Topics
1. **Time analysis**
    - Show the number of transactions occuring every month between the start and end of the dataset.
    - Show the average value of transaction in each month between the start and end of the data.
3. **Top ten most popular services**
    - Find the top 10 smart contracts by total Ether received
4. **Top ten most active miners**
    - Find the top 10 miners by the size of the blocks mined.
6. **Popular scams**
    - What is the most lucrative form of scam? How does it change throughout time?
7. **Contract types**
    - How many different types can you identify? 
    - What is the most popular type of contract? More information can be found at https://www.sciencedirect.com/science/article/pii/S0306457320309547#bib0019. 
    - Examples of features are: number of transactions, number of unique outflow addresses, Ether balance, etc.
8. **Fork the chain**
    - Identify of the several forks of Ethereum in the past and see what effect it had on price and general usage. For exampole, price surge/plummet.
10. **Gas Guzzlers**
    - How has gas price changed over time? Have contracts become more complicated requiring more gas?

