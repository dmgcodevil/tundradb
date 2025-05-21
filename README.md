# tundradb

Example:

```
CREATE SCHEMA User (name: STRING, age: INT64);
CREATE NODE User (name="Alex", age=25);
CREATE NODE User (name="Bob", age=31);
CREATE NODE User (name="Jeff", age=33);
CREATE NODE User (name="Sam", age=21);
CREATE NODE User (name="Matt", age=40);



CREATE SCHEMA Company (name: STRING, size: INT64);
CREATE NODE Company (name="google", size=3000);
CREATE NODE Company (name="ibm", size=1000);

CREATE EDGE WORKS_AT FROM User(0) TO Company(1);

CREATE EDGE WORKS_AT FROM User(2) TO Company(3);

MATCH (u:User)-[:WORKS_AT INNER]->(c:Company);
MATCH (u:User)-[:WORKS_AT LEFT]->(c:Company);
```