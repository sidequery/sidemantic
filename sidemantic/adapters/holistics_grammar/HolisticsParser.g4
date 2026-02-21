parser grammar HolisticsParser;

options { tokenVocab=HolisticsLexer; }

document: statement* EOF;

statement
    : namedBlock
    | anonymousBlock
    | property
    | constDeclaration
    | objectAssignment
    | valueAssignment
    | useStatement
    | funcDeclaration
    | expressionStatement
    ;

namedBlock
    : blockKeyword identifier block
    ;

anonymousBlock
    : blockKeyword block
    ;

blockKeyword
    : MODEL
    | DATASET
    | RELATIONSHIP
    | RELATIONSHIP_CONFIG
    | DIMENSION
    | MEASURE
    | METRIC
    | FUNC
    | CONSTANT
    | MODULE
    | EXTEND
    | IDENTIFIER
    ;

property
    : identifier COLON expression
    ;

constDeclaration
    : CONST identifier EQUAL expression SEMI?
    ;

objectAssignment
    : blockKeyword identifier EQUAL expression SEMI?
    ;

valueAssignment
    : identifier EQUAL expression SEMI?
    ;

useStatement
    : USE usePath useImportBlock? SEMI?
    ;

usePath
    : qualifiedName
    | identifier
    ;

useImportBlock
    : LBRACE useImportItem (COMMA useImportItem)* RBRACE
    ;

useImportItem
    : identifier (COLON identifier)?
    ;

funcDeclaration
    : FUNC identifier LPAREN paramList? RPAREN (ARROW typeExpr)? block
    ;

paramList
    : param (COMMA param)*
    ;

param
    : identifier (COLON typeExpr)? (EQUAL expression)?
    ;

typeExpr
    : typePrimary (PIPE typePrimary)*
    ;

typePrimary
    : identifier
    | string
    ;

expressionStatement
    : expression SEMI?
    ;

block
    : LBRACE statement* RBRACE
    ;

expression
    : logicalOr
    ;

logicalOr
    : logicalAnd (OR logicalAnd)*
    ;

logicalAnd
    : equality (AND equality)*
    ;

equality
    : comparison ((EQEQ | NOTEQ) comparison)*
    ;

comparison
    : additive ((GT | GTE | LT | LTE) additive)*
    ;

additive
    : multiplicative ((PLUS | DASH) multiplicative)*
    ;

multiplicative
    : unary ((STAR | SLASH | PERCENT) unary)*
    ;

unary
    : (NOT | DASH) unary
    | primary
    ;

primary
    : ifExpression
    | extendCall
    | functionCall
    | reference
    | taggedBlock
    | typedBlock
    | blockLiteral
    | array
    | string
    | number
    | boolean
    | nullValue
    | identifier
    | LPAREN expression RPAREN
    ;

ifExpression
    : IF LPAREN expression RPAREN block (ELSE (ifExpression | block))?
    ;

typedBlock
    : identifier block
    ;

blockLiteral
    : block
    ;

extendCall
    : extendTarget (DOT EXTEND_FUNC LPAREN (extendArg (COMMA extendArg)*)? RPAREN)+
    ;

extendTarget
    : typedBlock
    | functionCall
    | reference
    | identifier
    | LPAREN expression RPAREN
    ;

extendArg
    : expression
    ;

functionCall
    : identifier LPAREN (callArg (COMMA callArg)*)? RPAREN
    ;

callArg
    : callNamedArg
    | expression
    ;

callNamedArg
    : identifier COLON expression
    ;

array
    : LBRACK (expression (COMMA expression)*)? RBRACK
    ;

reference
    : qualifiedName
    ;

qualifiedName
    : identifier (DOT identifier)+
    ;

taggedBlock
    : TAGGED_BLOCK
    ;

string
    : STRING
    ;

number
    : NUMBER
    ;

boolean
    : TRUE
    | FALSE
    ;

nullValue
    : NULL
    ;

identifier
    : IDENTIFIER
    | MODEL
    | DATASET
    | RELATIONSHIP
    | RELATIONSHIP_CONFIG
    | DIMENSION
    | MEASURE
    | METRIC
    | FUNC
    | CONSTANT
    | MODULE
    | EXTEND
    | USE
    | CONST
    | IF
    | ELSE
    | TRUE
    | FALSE
    | NULL
    ;
