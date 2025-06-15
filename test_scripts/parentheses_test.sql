-- TundraDB Parentheses Support Test
-- This script demonstrates current limitations and future possibilities

-- Create schema
CREATE SCHEMA User (name: STRING, age: INT64, salary: INT64, city: STRING);

-- Create test data
CREATE NODE User (name="Alice", age=25, salary=50000, city="NYC") RETURN id;
CREATE NODE User (name="Bob", age=30, salary=75000, city="NYC") RETURN id;
CREATE NODE User (name="Charlie", age=35, salary=90000, city="SF") RETURN id;
CREATE NODE User (name="David", age=40, salary=120000, city="LA") RETURN id;
CREATE NODE User (name="Eve", age=28, salary=65000, city="NYC") RETURN id;

-- CURRENT: This works (simple left-to-right evaluation)
-- Find users who are either young OR in NYC
MATCH (u:User) WHERE u.age < 30 OR u.city = "NYC";

-- CURRENT: This works but may not follow expected precedence
-- Currently evaluates as: ((age > 30 AND salary > 80000) OR city = "SF")
-- Due to left-to-right evaluation with simple heuristics
MATCH (u:User) WHERE u.age > 30 AND u.salary > 80000 OR u.city = "SF";

-- FUTURE: With parentheses support, users could write:
-- MATCH (u:User) WHERE u.age > 30 AND (u.salary > 80000 OR u.city = "SF");
-- This would find users over 30 who either have high salary OR live in SF

-- FUTURE: More complex expressions would be possible:
-- MATCH (u:User) WHERE (u.age < 30 OR u.age > 40) AND (u.city = "NYC" OR u.city = "SF");
-- This would find users who are either young or old, and live in NYC or SF

-- Test script for parentheses support in WHERE clauses
-- This demonstrates explicit precedence control with parentheses

-- Test 1: Basic OR operation (no parentheses needed)
-- Should return: Alice (age<30), Bob (NYC), Eve (age<30 AND NYC)
MATCH (u:User) WHERE u.age < 30 OR u.city = "NYC";

-- Test 2: Mixed AND/OR with implicit precedence
-- This is parsed as: (u.age > 30 AND u.salary > 80000) OR u.city = "SF"
-- Should return: Charlie (SF) and David (age>30 AND salary>80000)
MATCH (u:User) WHERE u.age > 30 AND u.salary > 80000 OR u.city = "SF";

-- Test 3: EXPLICIT PARENTHESES - Force different precedence
-- This should be parsed as: u.age > 25 AND (u.salary > 70000 OR u.city = "NYC")
-- Should return: Bob (age>25 AND NYC), Charlie (age>25 AND salary>70000), David (age>25 AND salary>70000), Eve (age>25 AND NYC)
MATCH (u:User) WHERE u.age > 25 AND (u.salary > 70000 OR u.city = "NYC");

-- Test 4: EXPLICIT PARENTHESES - Complex expression
-- This should be parsed as: (u.age < 30 OR u.age > 35) AND u.city = "NYC"
-- Should return: Alice (age<30 AND NYC), Eve (age<30 AND NYC)
MATCH (u:User) WHERE (u.age < 30 OR u.age > 35) AND u.city = "NYC";

-- Test 5: NESTED PARENTHESES
-- This should be parsed as: (u.age > 25 AND (u.city = "NYC" OR u.city = "SF")) OR u.salary > 100000
-- Should return: Bob (age>25 AND NYC), Charlie (age>25 AND SF), David (salary>100000), Eve (age>25 AND NYC)
MATCH (u:User) WHERE (u.age > 25 AND (u.city = "NYC" OR u.city = "SF")) OR u.salary > 100000; 