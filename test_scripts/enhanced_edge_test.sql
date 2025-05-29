-- Enhanced Edge Creation Test Script
-- Tests the new property-based edge creation syntax and UNIQUE keyword

-- Create schemas (id column is added automatically)
CREATE SCHEMA User (name: STRING, age: INT64, department: STRING);
CREATE SCHEMA Company (name: STRING, industry: STRING);
CREATE SCHEMA Project (name: STRING, budget: INT64);

-- Create users
CREATE NODE User (name="Alice", age=25, department="Engineering") RETURN id;
CREATE NODE User (name="Bob", age=30, department="Engineering") RETURN id;
CREATE NODE User (name="Charlie", age=35, department="Marketing") RETURN id;
CREATE NODE User (name="David", age=40, department="Engineering") RETURN id;
CREATE NODE User (name="Eve", age=28, department="Sales") RETURN id;

-- Show all users
MATCH (u:User);

-- Create companies
CREATE NODE Company (name="TechCorp", industry="Technology") RETURN id;
CREATE NODE Company (name="FinanceInc", industry="Finance") RETURN id;
CREATE NODE Company (name="StartupXYZ", industry="Technology") RETURN id;

-- Show all companies
MATCH (c:Company);

-- Create projects
CREATE NODE Project (name="WebApp", budget=100000) RETURN id;
CREATE NODE Project (name="MobileApp", budget=150000) RETURN id;
CREATE NODE Project (name="DataPlatform", budget=200000) RETURN id;

-- Show all projects
MATCH (p:Project);

-- Test 1: Legacy syntax (should still work)
CREATE EDGE WORKS_AT FROM User(0) TO Company(5);

-- Test 2: Property-based syntax with UNIQUE (single match expected)
CREATE UNIQUE EDGE WORKS_AT FROM (User{name="Bob"}) TO (Company{name="TechCorp"});

-- Test 3: Property-based syntax with UNIQUE for another user
CREATE UNIQUE EDGE WORKS_AT FROM (User{name="Charlie"}) TO (Company{name="FinanceInc"});

-- Test 4: Batch edge creation - Connect all Engineering users to TechCorp
CREATE EDGE WORKS_AT FROM (User{department="Engineering"}) TO (Company{name="TechCorp"});

-- Test 5: Connect users to projects based on properties
CREATE UNIQUE EDGE ASSIGNED_TO FROM (User{name="Alice"}) TO (Project{name="WebApp"});
CREATE UNIQUE EDGE ASSIGNED_TO FROM (User{name="Bob"}) TO (Project{name="MobileApp"});

-- Test 6: Connect multiple users to the same project
CREATE EDGE ASSIGNED_TO FROM (User{department="Engineering"}) TO (Project{name="DataPlatform"});

-- Test 7: Try UNIQUE with multiple matches (should fail)
-- This should fail because there are multiple users in Engineering department
-- CREATE UNIQUE EDGE LEADS FROM (User{department="Engineering"}) TO (Project{name="WebApp"});

-- Test 8: Create edges between companies and projects
CREATE EDGE SPONSORS FROM (Company{industry="Technology"}) TO (Project{budget=150000});

-- Verify all relationships
MATCH (u:User)-[w:WORKS_AT]->(c:Company);
MATCH (u:User)-[a:ASSIGNED_TO]->(p:Project);
MATCH (c:Company)-[s:SPONSORS]->(p:Project);

-- Test complex traversals with the new relationships
MATCH (u:User)-[:WORKS_AT]->(c:Company)-[:SPONSORS]->(p:Project);

-- Show users working at technology companies
MATCH (u:User)-[:WORKS_AT]->(c:Company) WHERE c.industry = "Technology";

-- Show projects with engineering team members
MATCH (u:User)-[:ASSIGNED_TO]->(p:Project) WHERE u.department = "Engineering"; 