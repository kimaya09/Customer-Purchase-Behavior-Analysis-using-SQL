-- Setup a Database & Data Ingestion
create database project;
use project;
-- Data Transformation
-- Normalization of the data & Creating relationships between tables.

CREATE TABLE Customers (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(255),
    Country VARCHAR(255)
);

INSERT INTO Customers (CustomerID, CustomerName, Country)
WITH cte AS (
    SELECT customername, country, purchasedate 
    FROM customer_purchase_data
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY purchasedate) + 1000 AS CustomerID, 
    customername AS CustomerName, 
    country AS Country 
FROM cte;


CREATE TABLE Products (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(255),
    ProductCategory VARCHAR(255)
);

INSERT INTO Products (ProductID, ProductName, ProductCategory)
WITH cte1 AS (
    SELECT productname, productcategory, purchasedate 
    FROM customer_purchase_data
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY purchasedate) + 2000 AS ProductID, 
    productname AS ProductName, 
    productcategory AS ProductCategory 
FROM cte1;


-- Create temporary table for customerID mapping
CREATE TEMPORARY TABLE CustomerIDMapping AS
SELECT 
    customername, 
    MIN(CustomerID) AS CustomerID
FROM Customers
GROUP BY customername;

-- Create temporary table for ProductID mapping

CREATE TEMPORARY TABLE ProductIDMapping AS
SELECT 
    productname, 
    MIN(ProductID) AS ProductID
FROM Products
GROUP BY productname;

CREATE TABLE Purchases (
    TransactionID INT PRIMARY KEY,
    CustomerID INT,
    ProductID INT,
    PurchaseQuantity INT,
    PurchasePrice DECIMAL(10, 2),
    PurchaseDate DATE,
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);

INSERT INTO Purchases (TransactionID, CustomerID, ProductID, PurchaseQuantity, PurchasePrice, PurchaseDate)
WITH cte2 AS (
    SELECT 
        transactionid,
        customername,
        productname,
        purchasequantity,
        purchaseprice,
        purchasedate
    FROM customer_purchase_data
    WHERE transactionid NOT IN (SELECT TransactionID FROM Purchases)
)
SELECT 
    cte2.transactionid AS TransactionID,
    cm.CustomerID,
    pm.ProductID,
    cte2.purchasequantity AS PurchaseQuantity,
    cte2.purchaseprice AS PurchasePrice,
    STR_TO_DATE(cte2.purchasedate, '%m/%d/%Y') AS PurchaseDate
FROM cte2
JOIN CustomerIDMapping cm ON cm.customername = cte2.customername
JOIN ProductIDMapping pm ON pm.productname = cte2.productname;

select * from customers;
select * from products;
select * from purchases;
select* from customer_purchase_data;


-- Handling missing values.
select * from customers where customername is null or customerid is null or country is null;
select * from products where productname is null or productid is null or productcategory is null;
select * from purchases where transactionid is null or customerid is null or productid is null or purchasequantity is null or purchaseprice is null or purchasedate is null;

