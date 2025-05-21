grammar TundraQL;

// Entry point for parsing a full command
statement: createSchemaStatement | createNodeStatement | createEdgeStatement | matchStatement EOF;

// --- Schema Definition ---
createSchemaStatement: K_CREATE K_SCHEMA IDENTIFIER LPAREN schemaFieldList RPAREN SEMI;
schemaFieldList: schemaField (COMMA schemaField)*;
schemaField: IDENTIFIER COLON dataType;
dataType: T_STRING | T_INT64 | T_FLOAT64; // Add more as needed

// --- Node Creation ---
createNodeStatement: K_CREATE K_NODE IDENTIFIER LPAREN propertyList RPAREN (K_RETURN K_ID)? SEMI;
propertyList: propertyAssignment (COMMA propertyAssignment)*;
propertyAssignment: IDENTIFIER EQ value;
value: STRING_LITERAL | INTEGER_LITERAL | FLOAT_LITERAL; // Add more value types

// --- Edge Creation ---
createEdgeStatement: K_CREATE K_EDGE IDENTIFIER K_FROM nodeLocator K_TO nodeLocator (K_WITH LPAREN propertyList RPAREN)? SEMI;
nodeLocator: IDENTIFIER LPAREN INTEGER_LITERAL RPAREN; // e.g., User(123)

// --- Match Statement ---
matchStatement: K_MATCH pathPattern (K_WHERE whereClause)? (K_SELECT selectClause)? SEMI;

pathPattern: nodePattern (edgePattern nodePattern)*;

nodePattern: LPAREN IDENTIFIER (COLON IDENTIFIER)? RPAREN; // (alias:NodeType) or (alias)

edgePattern:
    MINUS LBRACKET (COLON IDENTIFIER)? (joinSpecifier)? RBRACKET MINUS GT // -[:REL_TYPE JOIN]->
    | LT MINUS LBRACKET (COLON IDENTIFIER)? (joinSpecifier)? RBRACKET MINUS; // <-[:REL_TYPE JOIN]-

joinSpecifier: K_INNER | K_LEFT | K_RIGHT | K_FULL;

// --- WHERE Clause ---
whereClause: expression; // Define your expression grammar (e.g., comparisons, AND/OR)
expression: term ( (K_AND | K_OR) term )*;
term: factor ( (EQ | NEQ | LT | LTE | GT | GTE) factor )?; // Simplified
factor: IDENTIFIER (DOT IDENTIFIER)? | value;


// --- SELECT Clause ---
selectClause: selectField (COMMA selectField)*;
selectField: IDENTIFIER (DOT IDENTIFIER)? (K_AS IDENTIFIER)?; // e.g., u.name AS userName

// --- Keywords ---
K_CREATE: 'CREATE';
K_SCHEMA: 'SCHEMA';
K_NODE: 'NODE';
K_EDGE: 'EDGE';
K_FROM: 'FROM';
K_TO: 'TO';
K_WITH: 'WITH';
K_MATCH: 'MATCH';
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

// --- Data Types for Schema ---
T_STRING: 'STRING';
T_INT64: 'INT64';
T_FLOAT64: 'FLOAT64';


// --- Literals & Punctuation ---
IDENTIFIER: [a-zA-Z_][a-zA-Z_0-9]*;
INTEGER_LITERAL: [0-9]+;
FLOAT_LITERAL: [0-9]+ '.' [0-9]+;
STRING_LITERAL: '"' (~["\r\n\\] | '\\' .)*? '"'; // Basic string literal

LPAREN: '(';
RPAREN: ')';
LBRACKET: '[';
RBRACKET: ']';
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
