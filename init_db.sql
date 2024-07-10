create schema if not exists finances;

-- create table for coin data
create table if not exists finances.coin_data (
    exchangeId varchar(50),
    name varchar(50),
    rank integer,
    percentTotalVolume real,
    volumeUsd real,
    tradingPairs integer,
    socket boolean,
    exchangeUrl varchar(50),
    updated timestamp
);



-- create table for stock data
create table if not exists finances.stock_data (
    timestamp timestamp,
    symbol varchar(10),
    open real, 
    high real, 
    low real, 
    close real,
    volume integer,
    primary key (symbol, timestamp)
);



-- copy data into stock_data table
copy finances.stock_data
from '/data/stock_info.csv' 
delimiter ',' 
csv header;
