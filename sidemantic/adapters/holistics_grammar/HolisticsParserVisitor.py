# Generated from sidemantic/adapters/holistics_grammar/HolisticsParser.g4 by ANTLR 4.13.2
from antlr4 import *
if "." in __name__:
    from .HolisticsParser import HolisticsParser
else:
    from HolisticsParser import HolisticsParser

# This class defines a complete generic visitor for a parse tree produced by HolisticsParser.

class HolisticsParserVisitor(ParseTreeVisitor):

    # Visit a parse tree produced by HolisticsParser#document.
    def visitDocument(self, ctx:HolisticsParser.DocumentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#statement.
    def visitStatement(self, ctx:HolisticsParser.StatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#namedBlock.
    def visitNamedBlock(self, ctx:HolisticsParser.NamedBlockContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#anonymousBlock.
    def visitAnonymousBlock(self, ctx:HolisticsParser.AnonymousBlockContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#blockKeyword.
    def visitBlockKeyword(self, ctx:HolisticsParser.BlockKeywordContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#property.
    def visitProperty(self, ctx:HolisticsParser.PropertyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#constDeclaration.
    def visitConstDeclaration(self, ctx:HolisticsParser.ConstDeclarationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#objectAssignment.
    def visitObjectAssignment(self, ctx:HolisticsParser.ObjectAssignmentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#valueAssignment.
    def visitValueAssignment(self, ctx:HolisticsParser.ValueAssignmentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#useStatement.
    def visitUseStatement(self, ctx:HolisticsParser.UseStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#usePath.
    def visitUsePath(self, ctx:HolisticsParser.UsePathContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#useImportBlock.
    def visitUseImportBlock(self, ctx:HolisticsParser.UseImportBlockContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#useImportItem.
    def visitUseImportItem(self, ctx:HolisticsParser.UseImportItemContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#funcDeclaration.
    def visitFuncDeclaration(self, ctx:HolisticsParser.FuncDeclarationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#paramList.
    def visitParamList(self, ctx:HolisticsParser.ParamListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#param.
    def visitParam(self, ctx:HolisticsParser.ParamContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#typeExpr.
    def visitTypeExpr(self, ctx:HolisticsParser.TypeExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#typePrimary.
    def visitTypePrimary(self, ctx:HolisticsParser.TypePrimaryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#expressionStatement.
    def visitExpressionStatement(self, ctx:HolisticsParser.ExpressionStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#block.
    def visitBlock(self, ctx:HolisticsParser.BlockContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#expression.
    def visitExpression(self, ctx:HolisticsParser.ExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#logicalOr.
    def visitLogicalOr(self, ctx:HolisticsParser.LogicalOrContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#logicalAnd.
    def visitLogicalAnd(self, ctx:HolisticsParser.LogicalAndContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#equality.
    def visitEquality(self, ctx:HolisticsParser.EqualityContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#comparison.
    def visitComparison(self, ctx:HolisticsParser.ComparisonContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#additive.
    def visitAdditive(self, ctx:HolisticsParser.AdditiveContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#multiplicative.
    def visitMultiplicative(self, ctx:HolisticsParser.MultiplicativeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#unary.
    def visitUnary(self, ctx:HolisticsParser.UnaryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#primary.
    def visitPrimary(self, ctx:HolisticsParser.PrimaryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#ifExpression.
    def visitIfExpression(self, ctx:HolisticsParser.IfExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#typedBlock.
    def visitTypedBlock(self, ctx:HolisticsParser.TypedBlockContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#blockLiteral.
    def visitBlockLiteral(self, ctx:HolisticsParser.BlockLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#extendCall.
    def visitExtendCall(self, ctx:HolisticsParser.ExtendCallContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#extendTarget.
    def visitExtendTarget(self, ctx:HolisticsParser.ExtendTargetContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#extendArg.
    def visitExtendArg(self, ctx:HolisticsParser.ExtendArgContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#functionCall.
    def visitFunctionCall(self, ctx:HolisticsParser.FunctionCallContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#callArg.
    def visitCallArg(self, ctx:HolisticsParser.CallArgContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#callNamedArg.
    def visitCallNamedArg(self, ctx:HolisticsParser.CallNamedArgContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#array.
    def visitArray(self, ctx:HolisticsParser.ArrayContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#reference.
    def visitReference(self, ctx:HolisticsParser.ReferenceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#qualifiedName.
    def visitQualifiedName(self, ctx:HolisticsParser.QualifiedNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#taggedBlock.
    def visitTaggedBlock(self, ctx:HolisticsParser.TaggedBlockContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#string.
    def visitString(self, ctx:HolisticsParser.StringContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#number.
    def visitNumber(self, ctx:HolisticsParser.NumberContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#boolean.
    def visitBoolean(self, ctx:HolisticsParser.BooleanContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#nullValue.
    def visitNullValue(self, ctx:HolisticsParser.NullValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by HolisticsParser#identifier.
    def visitIdentifier(self, ctx:HolisticsParser.IdentifierContext):
        return self.visitChildren(ctx)



del HolisticsParser