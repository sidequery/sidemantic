lexer grammar HolisticsLexer;

MODEL: 'Model';
DATASET: 'Dataset';
RELATIONSHIP: 'Relationship';
RELATIONSHIP_CONFIG: 'RelationshipConfig';
DIMENSION: 'dimension';
MEASURE: 'measure';
METRIC: 'metric';
FUNC: 'Func';
CONSTANT: 'Constant';
MODULE: 'Module';
EXTEND: 'Extend';
EXTEND_FUNC: 'extend';
USE: 'use';
CONST: 'const';
IF: 'if';
ELSE: 'else';

TRUE: 'true';
FALSE: 'false';
NULL: 'null';

TAGGED_BLOCK: '@' IDENT_START IDENT_CONT* .*? ';;';

LBRACE: '{';
RBRACE: '}';
LBRACK: '[';
RBRACK: ']';
LPAREN: '(';
RPAREN: ')';
COLON: ':';
COMMA: ',';
DOT: '.';
ARROW: '=>';
EQEQ: '==';
NOTEQ: '!=';
GTE: '>=';
LTE: '<=';
GT: '>';
LT: '<';
EQUAL: '=';
PLUS: '+';
STAR: '*';
SLASH: '/';
PERCENT: '%';
AND: '&&';
OR: '||';
NOT: '!';
PIPE: '|';
SEMI: ';';
DASH: '-';

STRING
    : '\'' ( '\\' . | ~['\\] )* '\''
    | '"' ( '\\' . | ~["\\] )* '"'
    | '`' ( '\\' . | ~[`\\] )* '`'
    ;

NUMBER: '-'? DIGIT+ ('.' DIGIT+)?;

IDENTIFIER: IDENT_START IDENT_CONT*;

fragment IDENT_START: [A-Za-z_];
fragment IDENT_CONT: [A-Za-z0-9_];
fragment DIGIT: [0-9];

LINE_COMMENT: '//' ~[\r\n]* -> skip;
BLOCK_COMMENT: '/*' .*? '*/' -> skip;
WS: [ \t\r\n]+ -> skip;
