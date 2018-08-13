/**
 * Define a grammar called AAIDsl
 */
grammar AAIDsl;


aaiquery: dslStatement;

dslStatement: (queryStep) (traverseStep | unionTraverseStep)* limitStep*;

queryStep : (singleNodeStep |singleQueryStep | multiQueryStep);

unionQueryStep: LBRACKET dslStatement ( COMMA (dslStatement))* RBRACKET;

traverseStep: (TRAVERSE (  queryStep | unionQueryStep));

unionTraverseStep: TRAVERSE unionQueryStep;

singleNodeStep: NODE STORE? ;
singleQueryStep: NODE STORE? (filterStep | filterTraverseStep);
multiQueryStep:  NODE STORE? (filterStep | filterTraverseStep) (filterStep)+;

filterStep: NOT? (LPAREN KEY COMMA KEY (COMMA KEY)*RPAREN);
filterTraverseStep: (LPAREN traverseStep* RPAREN);

limitStep: LIMIT NODE;

LIMIT: 'LIMIT';
NODE: ID;

KEY: ['] ID ['] ;

AND: [&];

STORE: [*];

OR: [|];

TRAVERSE: [>] ;

LPAREN: [(];
	
RPAREN: [)];

COMMA: [,] ;

EQUAL: [=];

LBRACKET: [[];
	
RBRACKET: [\]];

NOT: [!];

VALUE: DIGIT;

fragment LOWERCASE  : [a-z] ;
fragment UPPERCASE  : [A-Z] ;
fragment DIGIT      : [0-9] ;
ID
   : ( LOWERCASE | UPPERCASE | DIGIT) ( LOWERCASE | UPPERCASE | DIGIT | '-' |'.' |'_')*
   ;

WS : [ \t\r\n]+ -> skip ; // skip spaces, tabs, newlines


