-- Simple TundraDB Test Script
-- This script demonstrates basic functionality

-- Create a User schema (id column is added automatically)
CREATE SCHEMA User (name: STRING, age: INT64);

-- Create some test users
CREATE NODE User (name="Alice", age=25) RETURN id;
CREATE NODE User (name="Bob", age=30) RETURN id;
CREATE NODE User (name="Charlie", age=35) RETURN id;

-- Show all users
MATCH (u:User);

-- Create a Company schema (id column is added automatically)
CREATE SCHEMA Company (name: STRING, industry: STRING);

-- Create some companies
CREATE NODE Company (name="TechCorp", industry="Technology") RETURN id;
CREATE NODE Company (name="FinanceInc", industry="Finance") RETURN id;

-- Show all companies
MATCH (c:Company);

-- Create relationships (Company IDs are 3 and 4 since node IDs are unique across all types)
CREATE EDGE WORKS_AT FROM User(0) TO Company(3);
CREATE EDGE WORKS_AT FROM User(1) TO Company(3);
CREATE EDGE WORKS_AT FROM User(2) TO Company(4);

-- Show users with their companies
MATCH (u:User)-[:WORKS_AT]->(c:Company); 