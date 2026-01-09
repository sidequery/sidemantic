# Generated from sidemantic/adapters/holistics_grammar/HolisticsParser.g4 by ANTLR 4.13.2
from antlr4 import *
if "." in __name__:
    from .HolisticsParser import HolisticsParser
else:
    from HolisticsParser import HolisticsParser

# This class defines a complete listener for a parse tree produced by HolisticsParser.
class HolisticsParserListener(ParseTreeListener):

    # Enter a parse tree produced by HolisticsParser#document.
    def enterDocument(self, ctx:HolisticsParser.DocumentContext):
        pass

    # Exit a parse tree produced by HolisticsParser#document.
    def exitDocument(self, ctx:HolisticsParser.DocumentContext):
        pass


    # Enter a parse tree produced by HolisticsParser#statement.
    def enterStatement(self, ctx:HolisticsParser.StatementContext):
        pass

    # Exit a parse tree produced by HolisticsParser#statement.
    def exitStatement(self, ctx:HolisticsParser.StatementContext):
        pass


    # Enter a parse tree produced by HolisticsParser#namedBlock.
    def enterNamedBlock(self, ctx:HolisticsParser.NamedBlockContext):
        pass

    # Exit a parse tree produced by HolisticsParser#namedBlock.
    def exitNamedBlock(self, ctx:HolisticsParser.NamedBlockContext):
        pass


    # Enter a parse tree produced by HolisticsParser#anonymousBlock.
    def enterAnonymousBlock(self, ctx:HolisticsParser.AnonymousBlockContext):
        pass

    # Exit a parse tree produced by HolisticsParser#anonymousBlock.
    def exitAnonymousBlock(self, ctx:HolisticsParser.AnonymousBlockContext):
        pass


    # Enter a parse tree produced by HolisticsParser#blockKeyword.
    def enterBlockKeyword(self, ctx:HolisticsParser.BlockKeywordContext):
        pass

    # Exit a parse tree produced by HolisticsParser#blockKeyword.
    def exitBlockKeyword(self, ctx:HolisticsParser.BlockKeywordContext):
        pass


    # Enter a parse tree produced by HolisticsParser#property.
    def enterProperty(self, ctx:HolisticsParser.PropertyContext):
        pass

    # Exit a parse tree produced by HolisticsParser#property.
    def exitProperty(self, ctx:HolisticsParser.PropertyContext):
        pass


    # Enter a parse tree produced by HolisticsParser#constDeclaration.
    def enterConstDeclaration(self, ctx:HolisticsParser.ConstDeclarationContext):
        pass

    # Exit a parse tree produced by HolisticsParser#constDeclaration.
    def exitConstDeclaration(self, ctx:HolisticsParser.ConstDeclarationContext):
        pass


    # Enter a parse tree produced by HolisticsParser#objectAssignment.
    def enterObjectAssignment(self, ctx:HolisticsParser.ObjectAssignmentContext):
        pass

    # Exit a parse tree produced by HolisticsParser#objectAssignment.
    def exitObjectAssignment(self, ctx:HolisticsParser.ObjectAssignmentContext):
        pass


    # Enter a parse tree produced by HolisticsParser#valueAssignment.
    def enterValueAssignment(self, ctx:HolisticsParser.ValueAssignmentContext):
        pass

    # Exit a parse tree produced by HolisticsParser#valueAssignment.
    def exitValueAssignment(self, ctx:HolisticsParser.ValueAssignmentContext):
        pass


    # Enter a parse tree produced by HolisticsParser#useStatement.
    def enterUseStatement(self, ctx:HolisticsParser.UseStatementContext):
        pass

    # Exit a parse tree produced by HolisticsParser#useStatement.
    def exitUseStatement(self, ctx:HolisticsParser.UseStatementContext):
        pass


    # Enter a parse tree produced by HolisticsParser#usePath.
    def enterUsePath(self, ctx:HolisticsParser.UsePathContext):
        pass

    # Exit a parse tree produced by HolisticsParser#usePath.
    def exitUsePath(self, ctx:HolisticsParser.UsePathContext):
        pass


    # Enter a parse tree produced by HolisticsParser#useImportBlock.
    def enterUseImportBlock(self, ctx:HolisticsParser.UseImportBlockContext):
        pass

    # Exit a parse tree produced by HolisticsParser#useImportBlock.
    def exitUseImportBlock(self, ctx:HolisticsParser.UseImportBlockContext):
        pass


    # Enter a parse tree produced by HolisticsParser#useImportItem.
    def enterUseImportItem(self, ctx:HolisticsParser.UseImportItemContext):
        pass

    # Exit a parse tree produced by HolisticsParser#useImportItem.
    def exitUseImportItem(self, ctx:HolisticsParser.UseImportItemContext):
        pass


    # Enter a parse tree produced by HolisticsParser#funcDeclaration.
    def enterFuncDeclaration(self, ctx:HolisticsParser.FuncDeclarationContext):
        pass

    # Exit a parse tree produced by HolisticsParser#funcDeclaration.
    def exitFuncDeclaration(self, ctx:HolisticsParser.FuncDeclarationContext):
        pass


    # Enter a parse tree produced by HolisticsParser#paramList.
    def enterParamList(self, ctx:HolisticsParser.ParamListContext):
        pass

    # Exit a parse tree produced by HolisticsParser#paramList.
    def exitParamList(self, ctx:HolisticsParser.ParamListContext):
        pass


    # Enter a parse tree produced by HolisticsParser#param.
    def enterParam(self, ctx:HolisticsParser.ParamContext):
        pass

    # Exit a parse tree produced by HolisticsParser#param.
    def exitParam(self, ctx:HolisticsParser.ParamContext):
        pass


    # Enter a parse tree produced by HolisticsParser#typeExpr.
    def enterTypeExpr(self, ctx:HolisticsParser.TypeExprContext):
        pass

    # Exit a parse tree produced by HolisticsParser#typeExpr.
    def exitTypeExpr(self, ctx:HolisticsParser.TypeExprContext):
        pass


    # Enter a parse tree produced by HolisticsParser#typePrimary.
    def enterTypePrimary(self, ctx:HolisticsParser.TypePrimaryContext):
        pass

    # Exit a parse tree produced by HolisticsParser#typePrimary.
    def exitTypePrimary(self, ctx:HolisticsParser.TypePrimaryContext):
        pass


    # Enter a parse tree produced by HolisticsParser#expressionStatement.
    def enterExpressionStatement(self, ctx:HolisticsParser.ExpressionStatementContext):
        pass

    # Exit a parse tree produced by HolisticsParser#expressionStatement.
    def exitExpressionStatement(self, ctx:HolisticsParser.ExpressionStatementContext):
        pass


    # Enter a parse tree produced by HolisticsParser#block.
    def enterBlock(self, ctx:HolisticsParser.BlockContext):
        pass

    # Exit a parse tree produced by HolisticsParser#block.
    def exitBlock(self, ctx:HolisticsParser.BlockContext):
        pass


    # Enter a parse tree produced by HolisticsParser#expression.
    def enterExpression(self, ctx:HolisticsParser.ExpressionContext):
        pass

    # Exit a parse tree produced by HolisticsParser#expression.
    def exitExpression(self, ctx:HolisticsParser.ExpressionContext):
        pass


    # Enter a parse tree produced by HolisticsParser#logicalOr.
    def enterLogicalOr(self, ctx:HolisticsParser.LogicalOrContext):
        pass

    # Exit a parse tree produced by HolisticsParser#logicalOr.
    def exitLogicalOr(self, ctx:HolisticsParser.LogicalOrContext):
        pass


    # Enter a parse tree produced by HolisticsParser#logicalAnd.
    def enterLogicalAnd(self, ctx:HolisticsParser.LogicalAndContext):
        pass

    # Exit a parse tree produced by HolisticsParser#logicalAnd.
    def exitLogicalAnd(self, ctx:HolisticsParser.LogicalAndContext):
        pass


    # Enter a parse tree produced by HolisticsParser#equality.
    def enterEquality(self, ctx:HolisticsParser.EqualityContext):
        pass

    # Exit a parse tree produced by HolisticsParser#equality.
    def exitEquality(self, ctx:HolisticsParser.EqualityContext):
        pass


    # Enter a parse tree produced by HolisticsParser#comparison.
    def enterComparison(self, ctx:HolisticsParser.ComparisonContext):
        pass

    # Exit a parse tree produced by HolisticsParser#comparison.
    def exitComparison(self, ctx:HolisticsParser.ComparisonContext):
        pass


    # Enter a parse tree produced by HolisticsParser#additive.
    def enterAdditive(self, ctx:HolisticsParser.AdditiveContext):
        pass

    # Exit a parse tree produced by HolisticsParser#additive.
    def exitAdditive(self, ctx:HolisticsParser.AdditiveContext):
        pass


    # Enter a parse tree produced by HolisticsParser#multiplicative.
    def enterMultiplicative(self, ctx:HolisticsParser.MultiplicativeContext):
        pass

    # Exit a parse tree produced by HolisticsParser#multiplicative.
    def exitMultiplicative(self, ctx:HolisticsParser.MultiplicativeContext):
        pass


    # Enter a parse tree produced by HolisticsParser#unary.
    def enterUnary(self, ctx:HolisticsParser.UnaryContext):
        pass

    # Exit a parse tree produced by HolisticsParser#unary.
    def exitUnary(self, ctx:HolisticsParser.UnaryContext):
        pass


    # Enter a parse tree produced by HolisticsParser#primary.
    def enterPrimary(self, ctx:HolisticsParser.PrimaryContext):
        pass

    # Exit a parse tree produced by HolisticsParser#primary.
    def exitPrimary(self, ctx:HolisticsParser.PrimaryContext):
        pass


    # Enter a parse tree produced by HolisticsParser#ifExpression.
    def enterIfExpression(self, ctx:HolisticsParser.IfExpressionContext):
        pass

    # Exit a parse tree produced by HolisticsParser#ifExpression.
    def exitIfExpression(self, ctx:HolisticsParser.IfExpressionContext):
        pass


    # Enter a parse tree produced by HolisticsParser#typedBlock.
    def enterTypedBlock(self, ctx:HolisticsParser.TypedBlockContext):
        pass

    # Exit a parse tree produced by HolisticsParser#typedBlock.
    def exitTypedBlock(self, ctx:HolisticsParser.TypedBlockContext):
        pass


    # Enter a parse tree produced by HolisticsParser#blockLiteral.
    def enterBlockLiteral(self, ctx:HolisticsParser.BlockLiteralContext):
        pass

    # Exit a parse tree produced by HolisticsParser#blockLiteral.
    def exitBlockLiteral(self, ctx:HolisticsParser.BlockLiteralContext):
        pass


    # Enter a parse tree produced by HolisticsParser#extendCall.
    def enterExtendCall(self, ctx:HolisticsParser.ExtendCallContext):
        pass

    # Exit a parse tree produced by HolisticsParser#extendCall.
    def exitExtendCall(self, ctx:HolisticsParser.ExtendCallContext):
        pass


    # Enter a parse tree produced by HolisticsParser#extendTarget.
    def enterExtendTarget(self, ctx:HolisticsParser.ExtendTargetContext):
        pass

    # Exit a parse tree produced by HolisticsParser#extendTarget.
    def exitExtendTarget(self, ctx:HolisticsParser.ExtendTargetContext):
        pass


    # Enter a parse tree produced by HolisticsParser#extendArg.
    def enterExtendArg(self, ctx:HolisticsParser.ExtendArgContext):
        pass

    # Exit a parse tree produced by HolisticsParser#extendArg.
    def exitExtendArg(self, ctx:HolisticsParser.ExtendArgContext):
        pass


    # Enter a parse tree produced by HolisticsParser#functionCall.
    def enterFunctionCall(self, ctx:HolisticsParser.FunctionCallContext):
        pass

    # Exit a parse tree produced by HolisticsParser#functionCall.
    def exitFunctionCall(self, ctx:HolisticsParser.FunctionCallContext):
        pass


    # Enter a parse tree produced by HolisticsParser#callArg.
    def enterCallArg(self, ctx:HolisticsParser.CallArgContext):
        pass

    # Exit a parse tree produced by HolisticsParser#callArg.
    def exitCallArg(self, ctx:HolisticsParser.CallArgContext):
        pass


    # Enter a parse tree produced by HolisticsParser#callNamedArg.
    def enterCallNamedArg(self, ctx:HolisticsParser.CallNamedArgContext):
        pass

    # Exit a parse tree produced by HolisticsParser#callNamedArg.
    def exitCallNamedArg(self, ctx:HolisticsParser.CallNamedArgContext):
        pass


    # Enter a parse tree produced by HolisticsParser#array.
    def enterArray(self, ctx:HolisticsParser.ArrayContext):
        pass

    # Exit a parse tree produced by HolisticsParser#array.
    def exitArray(self, ctx:HolisticsParser.ArrayContext):
        pass


    # Enter a parse tree produced by HolisticsParser#reference.
    def enterReference(self, ctx:HolisticsParser.ReferenceContext):
        pass

    # Exit a parse tree produced by HolisticsParser#reference.
    def exitReference(self, ctx:HolisticsParser.ReferenceContext):
        pass


    # Enter a parse tree produced by HolisticsParser#qualifiedName.
    def enterQualifiedName(self, ctx:HolisticsParser.QualifiedNameContext):
        pass

    # Exit a parse tree produced by HolisticsParser#qualifiedName.
    def exitQualifiedName(self, ctx:HolisticsParser.QualifiedNameContext):
        pass


    # Enter a parse tree produced by HolisticsParser#taggedBlock.
    def enterTaggedBlock(self, ctx:HolisticsParser.TaggedBlockContext):
        pass

    # Exit a parse tree produced by HolisticsParser#taggedBlock.
    def exitTaggedBlock(self, ctx:HolisticsParser.TaggedBlockContext):
        pass


    # Enter a parse tree produced by HolisticsParser#string.
    def enterString(self, ctx:HolisticsParser.StringContext):
        pass

    # Exit a parse tree produced by HolisticsParser#string.
    def exitString(self, ctx:HolisticsParser.StringContext):
        pass


    # Enter a parse tree produced by HolisticsParser#number.
    def enterNumber(self, ctx:HolisticsParser.NumberContext):
        pass

    # Exit a parse tree produced by HolisticsParser#number.
    def exitNumber(self, ctx:HolisticsParser.NumberContext):
        pass


    # Enter a parse tree produced by HolisticsParser#boolean.
    def enterBoolean(self, ctx:HolisticsParser.BooleanContext):
        pass

    # Exit a parse tree produced by HolisticsParser#boolean.
    def exitBoolean(self, ctx:HolisticsParser.BooleanContext):
        pass


    # Enter a parse tree produced by HolisticsParser#nullValue.
    def enterNullValue(self, ctx:HolisticsParser.NullValueContext):
        pass

    # Exit a parse tree produced by HolisticsParser#nullValue.
    def exitNullValue(self, ctx:HolisticsParser.NullValueContext):
        pass


    # Enter a parse tree produced by HolisticsParser#identifier.
    def enterIdentifier(self, ctx:HolisticsParser.IdentifierContext):
        pass

    # Exit a parse tree produced by HolisticsParser#identifier.
    def exitIdentifier(self, ctx:HolisticsParser.IdentifierContext):
        pass



del HolisticsParser