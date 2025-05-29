-- TundraDB JOIN Test Script
-- Tests different types of joins: INNER, LEFT, RIGHT, FULL

-- Create schemas (id column is added automatically)
CREATE SCHEMA User (name: STRING, age: INT64);
CREATE SCHEMA Company (name: STRING, industry: STRING);

-- Create users
CREATE NODE User (name="Alice", age=25) RETURN id;
CREATE NODE User (name="Bob", age=30) RETURN id;
CREATE NODE User (name="Charlie", age=35) RETURN id;
CREATE NODE User (name="David", age=40) RETURN id;

-- Create companies
CREATE NODE Company (name="TechCorp", industry="Technology") RETURN id;
CREATE NODE Company (name="FinanceInc", industry="Finance") RETURN id;

-- Create some relationships (not all users work at companies)
-- User IDs: 0,1,2,3 | Company IDs: 4,5
CREATE EDGE WORKS_AT FROM User(0) TO Company(4);
CREATE EDGE WORKS_AT FROM User(1) TO Company(4);

-- Test INNER JOIN (default) - only users with jobs
MATCH (u:User)-[:WORKS_AT]->(c:Company);

-- Test LEFT JOIN - all users, with company info if they have a job
MATCH (u:User)-[:WORKS_AT LEFT]->(c:Company);

-- Test RIGHT JOIN - all companies, with user info if they have employees
MATCH (u:User)-[:WORKS_AT RIGHT]->(c:Company);

-- Test FULL JOIN - all users and companies, matched where possible
MATCH (u:User)-[:WORKS_AT FULL]->(c:Company); 