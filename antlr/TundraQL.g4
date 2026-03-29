grammar TundraQL;

// Entry point for parsing a full command
statement: createSchemaStatement | createEdgeSchemaStatement | createNodeStatement | createEdgeStatement | matchStatement | deleteStatement | updateStatement | commitStatement | showStatement EOF;

// --- Schema Definition ---
createSchemaStatement: K_CREATE K_SCHEMA IDENTIFIER LPAREN schemaFieldList RPAREN SEMI;
createEdgeSchemaStatement: K_CREATE K_EDGE K_SCHEMA IDENTIFIER LPAREN schemaFieldList RPAREN SEMI;
schemaFieldList: schemaField (COMMA schemaField)*;
schemaField: IDENTIFIER COLON dataType;
dataType: T_STRING | T_INT64 | T_FLOAT64 | T_MAP; // Add more as needed

// --- Node Creation ---
createNodeStatement: K_CREATE K_NODE IDENTIFIER LPAREN propertyList RPAREN (K_RETURN K_ID)? SEMI;
propertyList: propertyAssignment (COMMA propertyAssignment)*;
propertyAssignment: IDENTIFIER (EQ | COLON) value;
mapLiteral: LBRACE propertyList? RBRACE;
value: STRING_LITERAL | INTEGER_LITERAL | FLOAT_LITERAL | mapLiteral; // Add more value types

// --- Edge Creation ---
createEdgeStatement: K_CREATE (K_UNIQUE)? K_EDGE IDENTIFIER K_FROM nodeSelector K_TO nodeSelector (K_WITH LPAREN propertyList RPAREN)? SEMI;

// Node selector supports both legacy ID syntax and new property-based syntax
nodeSelector: 
    nodeLocator                                    // Legacy: User(123)
    | LPAREN IDENTIFIER LBRACE propertyList RBRACE RPAREN;  // New: (User{name="Alice"})

nodeLocator: IDENTIFIER LPAREN INTEGER_LITERAL RPAREN; // e.g., User(123)

// --- Match Statement ---
matchStatement: K_MATCH patternList (K_WHERE whereClause)? (K_SELECT selectClause)? SEMI;

// Updated to support multiple comma-separated patterns
patternList: pathPattern (COMMA pathPattern)*;

// --- Delete Statement ---
deleteStatement: K_DELETE deleteTarget (K_WHERE whereClause)? SEMI;

deleteTarget: 
    nodeLocator                    // DELETE User(123);
    | pathPattern                  // DELETE (u:User)-[:FRIEND]->(f:User);
    | nodePattern                  // DELETE (u:User);
    | edgeDeleteTarget;            // DELETE EDGE edge_type FROM ... TO ...

// Edge deletion patterns
edgeDeleteTarget:
    K_EDGE IDENTIFIER                                      // DELETE EDGE edge_type;
    | K_EDGE IDENTIFIER K_FROM nodeSelector               // DELETE EDGE edge_type FROM node;
    | K_EDGE IDENTIFIER K_TO nodeSelector                 // DELETE EDGE edge_type TO node;
    | K_EDGE IDENTIFIER K_FROM nodeSelector K_TO nodeSelector;  // DELETE EDGE edge_type FROM node TO node;

// --- Update Statement ---
// UPDATE User(0) SET name = "Bob", age = 31;
// UPDATE (u:User) SET u.age = 31 WHERE u.name = "Alice";
// UPDATE MATCH (u:User)-[:WORKS_AT]->(c:Company) SET u.employed = true, c.size = 1 WHERE c.name = "Acme";
updateStatement: K_UPDATE updateTarget K_SET setClause (K_WHERE whereClause)? SEMI;

updateTarget:
    nodeLocator                    // UPDATE User(0) SET ...;
    | K_MATCH patternList          // UPDATE MATCH (u:User)-[:WORKS_AT]->(c:Company) SET ...;
    | nodePattern;                 // UPDATE (u:User) SET ... WHERE ...;

setClause: setAssignment (COMMA setAssignment)*;
setAssignment: IDENTIFIER (DOT IDENTIFIER)* EQ value;

// --- Commit Statement ---
commitStatement: K_COMMIT SEMI;

// --- Show Statement ---
showStatement: K_SHOW showTarget SEMI;

showTarget:
    K_EDGES IDENTIFIER          // SHOW EDGES edge_type
    | K_EDGE K_TYPES;           // SHOW EDGE TYPES

pathPattern: nodePattern (edgePattern nodePattern)*;

nodePattern: LPAREN IDENTIFIER (COLON IDENTIFIER)? RPAREN; // (alias:NodeType) or (alias)

edgePattern:
    MINUS LBRACKET edgeRef? (joinSpecifier)? RBRACKET MINUS GT // -[:REL_TYPE JOIN]-> or -[e:REL_TYPE JOIN]->
    | LT MINUS LBRACKET edgeRef? (joinSpecifier)? RBRACKET MINUS; // <-[:REL_TYPE JOIN]- or <-[e:REL_TYPE JOIN]-

edgeRef:
    COLON IDENTIFIER
    | IDENTIFIER COLON IDENTIFIER;

joinSpecifier: K_INNER | K_LEFT | K_RIGHT | K_FULL;

// --- WHERE Clause ---
whereClause: expression; // Define your expression grammar (e.g., comparisons, AND/OR)

// Updated expression grammar with proper precedence and parentheses support
expression: orExpression;
orExpression: andExpression (K_OR andExpression)*;
andExpression: primaryExpression (K_AND primaryExpression)*;
primaryExpression: 
    term                                    // Basic comparison
    | LPAREN expression RPAREN;            // Parenthesized expression

term: factor ( (EQ | NEQ | LT | LTE | GT | GTE) factor )?; // Simplified
factor: IDENTIFIER (DOT IDENTIFIER)* | value;

// --- SELECT Clause ---
selectClause: selectField (COMMA selectField)*;
selectField: IDENTIFIER (DOT IDENTIFIER)* (K_AS IDENTIFIER)?; // e.g., u.name AS userName

// --- Keywords ---
K_CREATE: 'CREATE';
K_SCHEMA: 'SCHEMA';
K_NODE: 'NODE';
K_EDGE: 'EDGE';
K_FROM: 'FROM';
K_TO: 'TO';
K_WITH: 'WITH';
K_MATCH: 'MATCH';
K_DELETE: 'DELETE';
K_WHERE: 'WHERE';
K_SELECT: 'SELECT';
K_RETURN: 'RETURN';
K_ID: 'id';
K_AS: 'AS';
K_INNER: 'INNER';
K_LEFT: 'LEFT';
K_RIGHT: 'RIGHT';
K_FULL: 'FULL';
K_AND: 'AND';
K_OR: 'OR';
K_UPDATE: 'UPDATE';
K_SET: 'SET';
K_COMMIT: 'COMMIT';
K_UNIQUE: 'UNIQUE';
K_SHOW: 'SHOW';
K_EDGES: 'EDGES';
K_TYPES: 'TYPES';

// --- Data Types for Schema ---
T_STRING: 'STRING';
T_INT64: 'INT64';
T_FLOAT64: 'FLOAT64';
T_MAP: 'MAP';


// --- Literals & Punctuation ---
IDENTIFIER: [a-zA-Z_][a-zA-Z_0-9]*;
INTEGER_LITERAL: [0-9]+;
FLOAT_LITERAL: [0-9]+ '.' [0-9]+;
STRING_LITERAL: '"' (~["\r\n\\] | '\\' .)*? '"'; // Basic string literal

LPAREN: '(';
RPAREN: ')';
LBRACKET: '[';
RBRACKET: ']';
LBRACE: '{';
RBRACE: '}';
SEMI: ';';
COMMA: ',';
COLON: ':';
EQ: '=';
NEQ: '!='; // Or <>
GT: '>';
GTE: '>=';
LT: '<';
LTE: '<=';
DOT: '.';
MINUS: '-';


// --- Whitespace & Comments ---
WS: [ \t\r\n]+ -> skip; // Skips whitespace
COMMENT: '//' ~[\r\n]* -> skip; // Skips single-line comments
// Add block comments if needed: /* ... */
