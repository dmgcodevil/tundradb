-- TundraDB WHERE Clause Test Script
-- Tests different WHERE clause operators and data types

-- Create schemas (id column is added automatically)
CREATE SCHEMA User (name: STRING, age: INT64, salary: INT64);
CREATE SCHEMA Product (name: STRING, price: INT64, category: STRING);

-- Create users with different ages and salaries
CREATE NODE User (name="Alice", age=25, salary=50000) RETURN id;
CREATE NODE User (name="Bob", age=30, salary=75000) RETURN id;
CREATE NODE User (name="Charlie", age=35, salary=90000) RETURN id;
CREATE NODE User (name="David", age=40, salary=120000) RETURN id;
CREATE NODE User (name="Eve", age=28, salary=65000) RETURN id;

-- Create products with different prices
CREATE NODE Product (name="Laptop", price=1200, category="Electronics") RETURN id;
CREATE NODE Product (name="Phone", price=800, category="Electronics") RETURN id;
CREATE NODE Product (name="Book", price=25, category="Education") RETURN id;
CREATE NODE Product (name="Chair", price=150, category="Furniture") RETURN id;

-- Test equality with strings
MATCH (u:User) WHERE u.name = "Alice";

-- Test inequality with strings
MATCH (u:User) WHERE u.name != "Bob";

-- Test greater than with integers
MATCH (u:User) WHERE u.age > 30;

-- Test less than with integers
MATCH (u:User) WHERE u.age < 35;

-- Test greater than or equal with integers
MATCH (u:User) WHERE u.salary >= 75000;

-- Test less than or equal with integers
MATCH (u:User) WHERE u.salary <= 65000;

-- Test with products - price range
MATCH (p:Product) WHERE p.price > 100;

-- Test with products - category filter
MATCH (p:Product) WHERE p.category = "Electronics"; 