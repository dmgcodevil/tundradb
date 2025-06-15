-- Simple TundraDB WHERE Clause AND/OR Test
-- Demonstrates basic AND/OR functionality

-- Create schema
CREATE SCHEMA User (name: STRING, age: INT64, city: STRING);

-- Create test data
CREATE NODE User (name="Alice", age=25, city="NYC") RETURN id;
CREATE NODE User (name="Bob", age=30, city="SF") RETURN id;
CREATE NODE User (name="Charlie", age=35, city="NYC") RETURN id;

-- Test simple AND
MATCH (u:User) WHERE u.age > 25 AND u.city = "NYC";

-- Test simple OR  
MATCH (u:User) WHERE u.age = 25 OR u.city = "SF";

-- Test multiple AND
MATCH (u:User) WHERE u.name = "Alice" AND u.age = 25 AND u.city = "NYC";

-- Test multiple OR
MATCH (u:User) WHERE u.age = 25 OR u.age = 30 OR u.age = 35; 