-- TundraDB WHERE Clause AND/OR Test Script
-- Tests compound WHERE clauses with AND/OR operators

-- Create schemas (id column is added automatically)
CREATE SCHEMA User (name: STRING, age: INT64, salary: INT64, city: STRING);
CREATE SCHEMA Product (name: STRING, price: INT64, category: STRING);

-- Create users with different ages, salaries, and cities
CREATE NODE User (name="Alice", age=25, salary=50000, city="NYC") RETURN id;
CREATE NODE User (name="Bob", age=30, salary=75000, city="NYC") RETURN id;
CREATE NODE User (name="Charlie", age=35, salary=90000, city="SF") RETURN id;
CREATE NODE User (name="David", age=40, salary=120000, city="LA") RETURN id;
CREATE NODE User (name="Eve", age=28, salary=65000, city="NYC") RETURN id;
CREATE NODE User (name="Frank", age=45, salary=150000, city="SF") RETURN id;

-- Create products with different prices and categories
CREATE NODE Product (name="Laptop", price=1200, category="Electronics") RETURN id;
CREATE NODE Product (name="Phone", price=800, category="Electronics") RETURN id;
CREATE NODE Product (name="Book", price=25, category="Education") RETURN id;
CREATE NODE Product (name="Chair", price=150, category="Furniture") RETURN id;
CREATE NODE Product (name="Tablet", price=600, category="Electronics") RETURN id;

-- Test AND operator: age > 30 AND salary > 80000
MATCH (u:User) WHERE u.age > 30 AND u.salary > 80000;

-- Test OR operator: city = "NYC" OR city = "SF"
MATCH (u:User) WHERE u.city = "NYC" OR u.city = "SF";

-- Test mixed AND/OR: age > 35 AND (city = "SF" OR salary > 100000)
-- Note: This tests precedence - should match users over 35 who are either in SF or have high salary
MATCH (u:User) WHERE u.age > 35 AND u.city = "SF" OR u.salary > 100000;

-- Test OR with different data types: price < 100 OR category = "Electronics"
MATCH (p:Product) WHERE p.price < 100 OR p.category = "Electronics";

-- Test complex AND: name = "Alice" AND age = 25 AND city = "NYC"
MATCH (u:User) WHERE u.name = "Alice" AND u.age = 25 AND u.city = "NYC";

-- Test complex OR: age = 25 OR age = 30 OR age = 35
MATCH (u:User) WHERE u.age = 25 OR u.age = 30 OR u.age = 35;

-- Test mixed conditions with products: price > 500 AND category = "Electronics" OR price < 50
MATCH (p:Product) WHERE p.price > 500 AND p.category = "Electronics" OR p.price < 50; 