-- TundraDB SHOW EDGES Test Script
-- This script demonstrates the new edge viewing functionality

-- Create schemas
CREATE SCHEMA User (name: STRING, age: INT64);
CREATE SCHEMA Company (name: STRING, industry: STRING);

-- Create users
CREATE NODE User (name="Alice", age=25) RETURN id;
CREATE NODE User (name="Bob", age=30) RETURN id;
CREATE NODE User (name="Charlie", age=35) RETURN id;

-- Create companies
CREATE NODE Company (name="TechCorp", industry="Technology") RETURN id;
CREATE NODE Company (name="FinanceInc", industry="Finance") RETURN id;

-- Create some edges
CREATE EDGE WORKS_AT FROM User(0) TO Company(3);
CREATE EDGE WORKS_AT FROM User(1) TO Company(3);
CREATE EDGE WORKS_AT FROM User(2) TO Company(4);

CREATE EDGE FRIEND FROM User(0) TO User(1);
CREATE EDGE FRIEND FROM User(1) TO User(2);

-- Show all edge types
SHOW EDGE TYPES;

-- Show WORKS_AT edges
SHOW EDGES WORKS_AT;

-- Show FRIEND edges
SHOW EDGES FRIEND;

-- Try to show non-existent edge type
SHOW EDGES NON_EXISTENT; 