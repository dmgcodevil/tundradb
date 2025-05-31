-- TundraDB DELETE EDGES Test Script
-- This script demonstrates the new edge deletion functionality

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

-- Create many edges
CREATE EDGE WORKS_AT FROM User(0) TO Company(3);
CREATE EDGE WORKS_AT FROM User(1) TO Company(3);
CREATE EDGE WORKS_AT FROM User(2) TO Company(4);

CREATE EDGE FRIEND FROM User(0) TO User(1);
CREATE EDGE FRIEND FROM User(1) TO User(2);
CREATE EDGE FRIEND FROM User(0) TO User(2);

-- Show initial state
SHOW EDGE TYPES;
SHOW EDGES WORKS_AT;
SHOW EDGES FRIEND;

-- Test 1: Delete specific edge between two nodes
DELETE EDGE FRIEND FROM User(0) TO User(1);
COMMIT;
SHOW EDGES FRIEND;

-- Test 2: Delete all outgoing edges of a type from a node
DELETE EDGE WORKS_AT FROM User(1);
COMMIT;
SHOW EDGES WORKS_AT;

-- Test 3: Delete all incoming edges of a type to a node
DELETE EDGE WORKS_AT TO Company(4);
COMMIT;
SHOW EDGES WORKS_AT;

-- Test 4: Delete all edges of a specific type
DELETE EDGE FRIEND;
COMMIT;
SHOW EDGES FRIEND;

-- Test 5: Try to delete non-existent edge type
DELETE EDGE NON_EXISTENT;

-- Show final state
SHOW EDGE TYPES; 