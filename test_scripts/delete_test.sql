-- TundraDB DELETE Functionality Test Script

-- 1. Create a User schema (id column is added automatically)
CREATE SCHEMA User (name: STRING, age: INT64);

-- 2. Create some test users
CREATE NODE User (name="Alice", age=25) RETURN id;
CREATE NODE User (name="Bob", age=30) RETURN id;
CREATE NODE User (name="Charlie", age=35) RETURN id;
CREATE NODE User (name="David", age=40) RETURN id;

-- 3. Create a Company schema (id column is added automatically)
CREATE SCHEMA Company (name: STRING, industry: STRING);

-- 4. Create some test companies
CREATE NODE Company (name="TechCorp", industry="Technology") RETURN id;
CREATE NODE Company (name="FinanceInc", industry="Finance") RETURN id;

-- 5. Create WORKS_AT relationships
CREATE EDGE WORKS_AT FROM User(0) TO Company(0);
CREATE EDGE WORKS_AT FROM User(1) TO Company(0);
CREATE EDGE WORKS_AT FROM User(2) TO Company(1);

-- 6. Show all users before deletion
MATCH (u:User);

-- 7. DELETE by ID - Delete specific user
DELETE User(3);

-- 8. Show users after deleting by ID
MATCH (u:User);

-- 9. DELETE by pattern with WHERE clause - Delete users older than 30
DELETE (u:User) WHERE u.age > 30;

-- 10. Show users after conditional deletion
MATCH (u:User);

-- 11. DELETE all remaining users
DELETE (u:User);

-- 12. Show users after deleting all (should be empty)
MATCH (u:User);

-- 13. Show companies (should still exist)
MATCH (c:Company); 