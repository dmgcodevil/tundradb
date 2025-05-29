-- TundraDB Batch Test Script
-- This script is designed for automated testing with output verification

-- Create schemas (id column is added automatically)
CREATE SCHEMA User (name: STRING, age: INT64);
CREATE SCHEMA Company (name: STRING, industry: STRING);

-- Create test users
CREATE NODE User (name="Alice", age=25) RETURN id;
CREATE NODE User (name="Bob", age=30) RETURN id;
CREATE NODE User (name="Charlie", age=35) RETURN id;

-- Show all users
MATCH (u:User);

-- Create test companies
CREATE NODE Company (name="TechCorp", industry="Technology") RETURN id;
CREATE NODE Company (name="FinanceInc", industry="Finance") RETURN id;

-- Show all companies
MATCH (c:Company);

-- Create relationships
CREATE EDGE WORKS_AT FROM User(0) TO Company(0);
CREATE EDGE WORKS_AT FROM User(1) TO Company(0);

-- Test different join types
MATCH (u:User)-[:WORKS_AT]->(c:Company);
MATCH (u:User)-[:WORKS_AT LEFT]->(c:Company);
MATCH (u:User)-[:WORKS_AT FULL]->(c:Company);

-- Test WHERE clauses
MATCH (u:User) WHERE u.age > 25;
MATCH (c:Company) WHERE c.industry = "Technology";

-- Test DELETE operations
DELETE User(2);
MATCH (u:User);

-- Create a snapshot
COMMIT; 