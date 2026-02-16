-- Drop Fact Table First (FK Dependencies)
IF OBJECT_ID ('Fact_Sales', 'U') IS NOT NULL DROP TABLE Fact_Sales;

IF OBJECT_ID ('Dim_Date', 'U') IS NOT NULL DROP TABLE Dim_Date;

CREATE TABLE Dim_Date (
    DateKey INT PRIMARY KEY,
    Date DATE,
    Year INT,
    Quarter INT,
    Month INT,
    MonthName NVARCHAR(20),
    Day INT,
    Weekday NVARCHAR(20),
    IsWeekend BIT
);

IF OBJECT_ID ('Dim_Customer', 'U') IS NOT NULL
DROP TABLE Dim_Customer;

CREATE TABLE Dim_Customer (
    CustomerID NVARCHAR(255) PRIMARY KEY,
    CustomerName NVARCHAR(255),
    Segment NVARCHAR(50)
);

IF OBJECT_ID ('Dim_Product', 'U') IS NOT NULL DROP TABLE Dim_Product;

CREATE TABLE Dim_Product (
    ProductID NVARCHAR(255) PRIMARY KEY,
    ProductName NVARCHAR(255),
    Category NVARCHAR(50),
    SubCategory NVARCHAR(50)
);

IF OBJECT_ID ('Dim_Location', 'U') IS NOT NULL
DROP TABLE Dim_Location;

CREATE TABLE Dim_Location (
    LocationID BIGINT PRIMARY KEY,
    City NVARCHAR(100),
    State NVARCHAR(100),
    Country NVARCHAR(100),
    Region NVARCHAR(50),
    Market NVARCHAR(50),
    PostalCode NVARCHAR(20)
);

IF OBJECT_ID ('Dim_ShipMode', 'U') IS NOT NULL
DROP TABLE Dim_ShipMode;

CREATE TABLE Dim_ShipMode (
    ShipModeID BIGINT PRIMARY KEY,
    ShipMode NVARCHAR(50)
);

-- Fact creation at the end
CREATE TABLE Fact_Sales (
    RowID BIGINT PRIMARY KEY,
    OrderID NVARCHAR(255),
    DateKey INT,
    CustomerID NVARCHAR(255),
    ProductID NVARCHAR(255),
    LocationID BIGINT,
    ShipModeID BIGINT,
    Sales FLOAT,
    Quantity INT,
    Discount FLOAT,
    Profit FLOAT,
    ShippingCost FLOAT,
    shipping_days INT,
    profit_margin FLOAT,
    is_profitable BIT,
    discount_category NVARCHAR(50),
    order_value_segment NVARCHAR(50),
    CONSTRAINT FK_Fact_Date FOREIGN KEY (DateKey) REFERENCES Dim_Date (DateKey),
    CONSTRAINT FK_Fact_Customer FOREIGN KEY (CustomerID) REFERENCES Dim_Customer (CustomerID),
    CONSTRAINT FK_Fact_Product FOREIGN KEY (ProductID) REFERENCES Dim_Product (ProductID),
    CONSTRAINT FK_Fact_Location FOREIGN KEY (LocationID) REFERENCES Dim_Location (LocationID),
    CONSTRAINT FK_Fact_ShipMode FOREIGN KEY (ShipModeID) REFERENCES Dim_ShipMode (ShipModeID)
);