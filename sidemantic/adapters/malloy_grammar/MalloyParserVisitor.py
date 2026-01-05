# Generated from MalloyParser.g4 by ANTLR 4.13.2
from antlr4 import *
if "." in __name__:
    from .MalloyParser import MalloyParser
else:
    from MalloyParser import MalloyParser

# This class defines a complete generic visitor for a parse tree produced by MalloyParser.

class MalloyParserVisitor(ParseTreeVisitor):

    # Visit a parse tree produced by MalloyParser#malloyDocument.
    def visitMalloyDocument(self, ctx:MalloyParser.MalloyDocumentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#malloyStatement.
    def visitMalloyStatement(self, ctx:MalloyParser.MalloyStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#defineSourceStatement.
    def visitDefineSourceStatement(self, ctx:MalloyParser.DefineSourceStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#use_top_level_query_defs.
    def visitUse_top_level_query_defs(self, ctx:MalloyParser.Use_top_level_query_defsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#topLevelAnonQueryDef.
    def visitTopLevelAnonQueryDef(self, ctx:MalloyParser.TopLevelAnonQueryDefContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#tags.
    def visitTags(self, ctx:MalloyParser.TagsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#isDefine.
    def visitIsDefine(self, ctx:MalloyParser.IsDefineContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#runStatement.
    def visitRunStatement(self, ctx:MalloyParser.RunStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#sqlString.
    def visitSqlString(self, ctx:MalloyParser.SqlStringContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#sqlInterpolation.
    def visitSqlInterpolation(self, ctx:MalloyParser.SqlInterpolationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#importStatement.
    def visitImportStatement(self, ctx:MalloyParser.ImportStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#importSelect.
    def visitImportSelect(self, ctx:MalloyParser.ImportSelectContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#importItem.
    def visitImportItem(self, ctx:MalloyParser.ImportItemContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#importURL.
    def visitImportURL(self, ctx:MalloyParser.ImportURLContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#docAnnotations.
    def visitDocAnnotations(self, ctx:MalloyParser.DocAnnotationsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#ignoredObjectAnnotations.
    def visitIgnoredObjectAnnotations(self, ctx:MalloyParser.IgnoredObjectAnnotationsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#ignoredModelAnnotations.
    def visitIgnoredModelAnnotations(self, ctx:MalloyParser.IgnoredModelAnnotationsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#topLevelQueryDefs.
    def visitTopLevelQueryDefs(self, ctx:MalloyParser.TopLevelQueryDefsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#topLevelQueryDef.
    def visitTopLevelQueryDef(self, ctx:MalloyParser.TopLevelQueryDefContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#refineOperator.
    def visitRefineOperator(self, ctx:MalloyParser.RefineOperatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#turtleName.
    def visitTurtleName(self, ctx:MalloyParser.TurtleNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#sqlSource.
    def visitSqlSource(self, ctx:MalloyParser.SqlSourceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exploreTable.
    def visitExploreTable(self, ctx:MalloyParser.ExploreTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#connectionId.
    def visitConnectionId(self, ctx:MalloyParser.ConnectionIdContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#queryProperties.
    def visitQueryProperties(self, ctx:MalloyParser.QueryPropertiesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#queryName.
    def visitQueryName(self, ctx:MalloyParser.QueryNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#sourcePropertyList.
    def visitSourcePropertyList(self, ctx:MalloyParser.SourcePropertyListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#sourceDefinition.
    def visitSourceDefinition(self, ctx:MalloyParser.SourceDefinitionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#sqExplore.
    def visitSqExplore(self, ctx:MalloyParser.SqExploreContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#sourceParameters.
    def visitSourceParameters(self, ctx:MalloyParser.SourceParametersContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#legalParamType.
    def visitLegalParamType(self, ctx:MalloyParser.LegalParamTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#sourceParameter.
    def visitSourceParameter(self, ctx:MalloyParser.SourceParameterContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#parameterNameDef.
    def visitParameterNameDef(self, ctx:MalloyParser.ParameterNameDefContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#sourceNameDef.
    def visitSourceNameDef(self, ctx:MalloyParser.SourceNameDefContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exploreProperties.
    def visitExploreProperties(self, ctx:MalloyParser.ExplorePropertiesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#defExploreDimension_stub.
    def visitDefExploreDimension_stub(self, ctx:MalloyParser.DefExploreDimension_stubContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#defExploreMeasure_stub.
    def visitDefExploreMeasure_stub(self, ctx:MalloyParser.DefExploreMeasure_stubContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#defJoin_stub.
    def visitDefJoin_stub(self, ctx:MalloyParser.DefJoin_stubContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#defExploreWhere_stub.
    def visitDefExploreWhere_stub(self, ctx:MalloyParser.DefExploreWhere_stubContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#defExplorePrimaryKey.
    def visitDefExplorePrimaryKey(self, ctx:MalloyParser.DefExplorePrimaryKeyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#defExploreRename.
    def visitDefExploreRename(self, ctx:MalloyParser.DefExploreRenameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#defExploreEditField.
    def visitDefExploreEditField(self, ctx:MalloyParser.DefExploreEditFieldContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#defExploreQuery.
    def visitDefExploreQuery(self, ctx:MalloyParser.DefExploreQueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#defExploreTimezone.
    def visitDefExploreTimezone(self, ctx:MalloyParser.DefExploreTimezoneContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#defExploreAnnotation.
    def visitDefExploreAnnotation(self, ctx:MalloyParser.DefExploreAnnotationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#defIgnoreModel_stub.
    def visitDefIgnoreModel_stub(self, ctx:MalloyParser.DefIgnoreModel_stubContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#accessLabel.
    def visitAccessLabel(self, ctx:MalloyParser.AccessLabelContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#accessModifierList.
    def visitAccessModifierList(self, ctx:MalloyParser.AccessModifierListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#defMeasures.
    def visitDefMeasures(self, ctx:MalloyParser.DefMeasuresContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#defDimensions.
    def visitDefDimensions(self, ctx:MalloyParser.DefDimensionsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#renameList.
    def visitRenameList(self, ctx:MalloyParser.RenameListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#renameEntry.
    def visitRenameEntry(self, ctx:MalloyParser.RenameEntryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#defList.
    def visitDefList(self, ctx:MalloyParser.DefListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#fieldDef.
    def visitFieldDef(self, ctx:MalloyParser.FieldDefContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#fieldNameDef.
    def visitFieldNameDef(self, ctx:MalloyParser.FieldNameDefContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#joinNameDef.
    def visitJoinNameDef(self, ctx:MalloyParser.JoinNameDefContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#declareStatement.
    def visitDeclareStatement(self, ctx:MalloyParser.DeclareStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#defJoinOne.
    def visitDefJoinOne(self, ctx:MalloyParser.DefJoinOneContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#defJoinMany.
    def visitDefJoinMany(self, ctx:MalloyParser.DefJoinManyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#defJoinCross.
    def visitDefJoinCross(self, ctx:MalloyParser.DefJoinCrossContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#queryExtend.
    def visitQueryExtend(self, ctx:MalloyParser.QueryExtendContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#modEither.
    def visitModEither(self, ctx:MalloyParser.ModEitherContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#sourceArguments.
    def visitSourceArguments(self, ctx:MalloyParser.SourceArgumentsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#argumentId.
    def visitArgumentId(self, ctx:MalloyParser.ArgumentIdContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#sourceArgument.
    def visitSourceArgument(self, ctx:MalloyParser.SourceArgumentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#SQRefinedQuery.
    def visitSQRefinedQuery(self, ctx:MalloyParser.SQRefinedQueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#SQSQL.
    def visitSQSQL(self, ctx:MalloyParser.SQSQLContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#SQExtendedSource.
    def visitSQExtendedSource(self, ctx:MalloyParser.SQExtendedSourceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#SQCompose.
    def visitSQCompose(self, ctx:MalloyParser.SQComposeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#SQTable.
    def visitSQTable(self, ctx:MalloyParser.SQTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#SQInclude.
    def visitSQInclude(self, ctx:MalloyParser.SQIncludeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#SQArrow.
    def visitSQArrow(self, ctx:MalloyParser.SQArrowContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#SQParens.
    def visitSQParens(self, ctx:MalloyParser.SQParensContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#SQID.
    def visitSQID(self, ctx:MalloyParser.SQIDContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#includeBlock.
    def visitIncludeBlock(self, ctx:MalloyParser.IncludeBlockContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#includeItem.
    def visitIncludeItem(self, ctx:MalloyParser.IncludeItemContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#orphanedAnnotation.
    def visitOrphanedAnnotation(self, ctx:MalloyParser.OrphanedAnnotationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#accessLabelProp.
    def visitAccessLabelProp(self, ctx:MalloyParser.AccessLabelPropContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#includeExceptList.
    def visitIncludeExceptList(self, ctx:MalloyParser.IncludeExceptListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#includeExceptListItem.
    def visitIncludeExceptListItem(self, ctx:MalloyParser.IncludeExceptListItemContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#includeList.
    def visitIncludeList(self, ctx:MalloyParser.IncludeListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#includeField.
    def visitIncludeField(self, ctx:MalloyParser.IncludeFieldContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#SegField.
    def visitSegField(self, ctx:MalloyParser.SegFieldContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#SegRefine.
    def visitSegRefine(self, ctx:MalloyParser.SegRefineContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#SegParen.
    def visitSegParen(self, ctx:MalloyParser.SegParenContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#SegOps.
    def visitSegOps(self, ctx:MalloyParser.SegOpsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#VSeg.
    def visitVSeg(self, ctx:MalloyParser.VSegContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#VArrow.
    def visitVArrow(self, ctx:MalloyParser.VArrowContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#queryExtendStatement.
    def visitQueryExtendStatement(self, ctx:MalloyParser.QueryExtendStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#queryExtendStatementList.
    def visitQueryExtendStatementList(self, ctx:MalloyParser.QueryExtendStatementListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#joinList.
    def visitJoinList(self, ctx:MalloyParser.JoinListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#isExplore.
    def visitIsExplore(self, ctx:MalloyParser.IsExploreContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#matrixOperation.
    def visitMatrixOperation(self, ctx:MalloyParser.MatrixOperationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#joinFrom.
    def visitJoinFrom(self, ctx:MalloyParser.JoinFromContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#joinWith.
    def visitJoinWith(self, ctx:MalloyParser.JoinWithContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#joinOn.
    def visitJoinOn(self, ctx:MalloyParser.JoinOnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#joinExpression.
    def visitJoinExpression(self, ctx:MalloyParser.JoinExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#filterStatement.
    def visitFilterStatement(self, ctx:MalloyParser.FilterStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#fieldProperties.
    def visitFieldProperties(self, ctx:MalloyParser.FieldPropertiesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#aggregateOrdering.
    def visitAggregateOrdering(self, ctx:MalloyParser.AggregateOrderingContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#aggregateOrderBySpec.
    def visitAggregateOrderBySpec(self, ctx:MalloyParser.AggregateOrderBySpecContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#aggregateOrderByStatement.
    def visitAggregateOrderByStatement(self, ctx:MalloyParser.AggregateOrderByStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#fieldPropertyLimitStatement.
    def visitFieldPropertyLimitStatement(self, ctx:MalloyParser.FieldPropertyLimitStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#fieldPropertyStatement.
    def visitFieldPropertyStatement(self, ctx:MalloyParser.FieldPropertyStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#filterClauseList.
    def visitFilterClauseList(self, ctx:MalloyParser.FilterClauseListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#whereStatement.
    def visitWhereStatement(self, ctx:MalloyParser.WhereStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#havingStatement.
    def visitHavingStatement(self, ctx:MalloyParser.HavingStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#subQueryDefList.
    def visitSubQueryDefList(self, ctx:MalloyParser.SubQueryDefListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exploreQueryNameDef.
    def visitExploreQueryNameDef(self, ctx:MalloyParser.ExploreQueryNameDefContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exploreQueryDef.
    def visitExploreQueryDef(self, ctx:MalloyParser.ExploreQueryDefContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#drillStatement.
    def visitDrillStatement(self, ctx:MalloyParser.DrillStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#drillClauseList.
    def visitDrillClauseList(self, ctx:MalloyParser.DrillClauseListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#queryStatement.
    def visitQueryStatement(self, ctx:MalloyParser.QueryStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#queryJoinStatement.
    def visitQueryJoinStatement(self, ctx:MalloyParser.QueryJoinStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#groupByStatement.
    def visitGroupByStatement(self, ctx:MalloyParser.GroupByStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#queryFieldList.
    def visitQueryFieldList(self, ctx:MalloyParser.QueryFieldListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#queryFieldEntry.
    def visitQueryFieldEntry(self, ctx:MalloyParser.QueryFieldEntryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#nestStatement.
    def visitNestStatement(self, ctx:MalloyParser.NestStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#nestedQueryList.
    def visitNestedQueryList(self, ctx:MalloyParser.NestedQueryListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#nestDef.
    def visitNestDef(self, ctx:MalloyParser.NestDefContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#aggregateStatement.
    def visitAggregateStatement(self, ctx:MalloyParser.AggregateStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#calculateStatement.
    def visitCalculateStatement(self, ctx:MalloyParser.CalculateStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#projectStatement.
    def visitProjectStatement(self, ctx:MalloyParser.ProjectStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#partitionByStatement.
    def visitPartitionByStatement(self, ctx:MalloyParser.PartitionByStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#groupedByStatement.
    def visitGroupedByStatement(self, ctx:MalloyParser.GroupedByStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#orderByStatement.
    def visitOrderByStatement(self, ctx:MalloyParser.OrderByStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#ordering.
    def visitOrdering(self, ctx:MalloyParser.OrderingContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#orderBySpec.
    def visitOrderBySpec(self, ctx:MalloyParser.OrderBySpecContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#limitStatement.
    def visitLimitStatement(self, ctx:MalloyParser.LimitStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#bySpec.
    def visitBySpec(self, ctx:MalloyParser.BySpecContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#topStatement.
    def visitTopStatement(self, ctx:MalloyParser.TopStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#indexElement.
    def visitIndexElement(self, ctx:MalloyParser.IndexElementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#indexFields.
    def visitIndexFields(self, ctx:MalloyParser.IndexFieldsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#indexStatement.
    def visitIndexStatement(self, ctx:MalloyParser.IndexStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#sampleStatement.
    def visitSampleStatement(self, ctx:MalloyParser.SampleStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#timezoneStatement.
    def visitTimezoneStatement(self, ctx:MalloyParser.TimezoneStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#queryAnnotation.
    def visitQueryAnnotation(self, ctx:MalloyParser.QueryAnnotationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#sampleSpec.
    def visitSampleSpec(self, ctx:MalloyParser.SampleSpecContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#aggregate.
    def visitAggregate(self, ctx:MalloyParser.AggregateContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#malloyType.
    def visitMalloyType(self, ctx:MalloyParser.MalloyTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#compareOp.
    def visitCompareOp(self, ctx:MalloyParser.CompareOpContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#string.
    def visitString(self, ctx:MalloyParser.StringContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#shortString.
    def visitShortString(self, ctx:MalloyParser.ShortStringContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#rawString.
    def visitRawString(self, ctx:MalloyParser.RawStringContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#numericLiteral.
    def visitNumericLiteral(self, ctx:MalloyParser.NumericLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprString.
    def visitExprString(self, ctx:MalloyParser.ExprStringContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#stub_rawString.
    def visitStub_rawString(self, ctx:MalloyParser.Stub_rawStringContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprNumber.
    def visitExprNumber(self, ctx:MalloyParser.ExprNumberContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprTime.
    def visitExprTime(self, ctx:MalloyParser.ExprTimeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprNULL.
    def visitExprNULL(self, ctx:MalloyParser.ExprNULLContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprBool.
    def visitExprBool(self, ctx:MalloyParser.ExprBoolContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprRegex.
    def visitExprRegex(self, ctx:MalloyParser.ExprRegexContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#filterString_stub.
    def visitFilterString_stub(self, ctx:MalloyParser.FilterString_stubContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprNow.
    def visitExprNow(self, ctx:MalloyParser.ExprNowContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#literalTimestamp.
    def visitLiteralTimestamp(self, ctx:MalloyParser.LiteralTimestampContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#literalHour.
    def visitLiteralHour(self, ctx:MalloyParser.LiteralHourContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#literalDay.
    def visitLiteralDay(self, ctx:MalloyParser.LiteralDayContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#literalWeek.
    def visitLiteralWeek(self, ctx:MalloyParser.LiteralWeekContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#literalMonth.
    def visitLiteralMonth(self, ctx:MalloyParser.LiteralMonthContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#literalQuarter.
    def visitLiteralQuarter(self, ctx:MalloyParser.LiteralQuarterContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#literalYear.
    def visitLiteralYear(self, ctx:MalloyParser.LiteralYearContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#tablePath.
    def visitTablePath(self, ctx:MalloyParser.TablePathContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#tableURI.
    def visitTableURI(self, ctx:MalloyParser.TableURIContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#id.
    def visitId(self, ctx:MalloyParser.IdContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#timeframe.
    def visitTimeframe(self, ctx:MalloyParser.TimeframeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#ungroup.
    def visitUngroup(self, ctx:MalloyParser.UngroupContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#malloyOrSQLType.
    def visitMalloyOrSQLType(self, ctx:MalloyParser.MalloyOrSQLTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprExpr.
    def visitExprExpr(self, ctx:MalloyParser.ExprExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprCase.
    def visitExprCase(self, ctx:MalloyParser.ExprCaseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprMinus.
    def visitExprMinus(self, ctx:MalloyParser.ExprMinusContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprAddSub.
    def visitExprAddSub(self, ctx:MalloyParser.ExprAddSubContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprNullCheck.
    def visitExprNullCheck(self, ctx:MalloyParser.ExprNullCheckContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprArrayLiteral.
    def visitExprArrayLiteral(self, ctx:MalloyParser.ExprArrayLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprRange.
    def visitExprRange(self, ctx:MalloyParser.ExprRangeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprLogicalOr.
    def visitExprLogicalOr(self, ctx:MalloyParser.ExprLogicalOrContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprLiteral.
    def visitExprLiteral(self, ctx:MalloyParser.ExprLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprCompare.
    def visitExprCompare(self, ctx:MalloyParser.ExprCompareContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprForRange.
    def visitExprForRange(self, ctx:MalloyParser.ExprForRangeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprFunc.
    def visitExprFunc(self, ctx:MalloyParser.ExprFuncContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprAndTree.
    def visitExprAndTree(self, ctx:MalloyParser.ExprAndTreeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprCast.
    def visitExprCast(self, ctx:MalloyParser.ExprCastContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprAggFunc.
    def visitExprAggFunc(self, ctx:MalloyParser.ExprAggFuncContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprTimeTrunc.
    def visitExprTimeTrunc(self, ctx:MalloyParser.ExprTimeTruncContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprAggregate.
    def visitExprAggregate(self, ctx:MalloyParser.ExprAggregateContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprLogicalAnd.
    def visitExprLogicalAnd(self, ctx:MalloyParser.ExprLogicalAndContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprFieldPath.
    def visitExprFieldPath(self, ctx:MalloyParser.ExprFieldPathContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprMulDiv.
    def visitExprMulDiv(self, ctx:MalloyParser.ExprMulDivContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprSafeCast.
    def visitExprSafeCast(self, ctx:MalloyParser.ExprSafeCastContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprOrTree.
    def visitExprOrTree(self, ctx:MalloyParser.ExprOrTreeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprNot.
    def visitExprNot(self, ctx:MalloyParser.ExprNotContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprDuration.
    def visitExprDuration(self, ctx:MalloyParser.ExprDurationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprApply.
    def visitExprApply(self, ctx:MalloyParser.ExprApplyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprWarnLike.
    def visitExprWarnLike(self, ctx:MalloyParser.ExprWarnLikeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprFieldProps.
    def visitExprFieldProps(self, ctx:MalloyParser.ExprFieldPropsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprCoalesce.
    def visitExprCoalesce(self, ctx:MalloyParser.ExprCoalesceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprUngroup.
    def visitExprUngroup(self, ctx:MalloyParser.ExprUngroupContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprLiteralRecord.
    def visitExprLiteralRecord(self, ctx:MalloyParser.ExprLiteralRecordContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprWarnIn.
    def visitExprWarnIn(self, ctx:MalloyParser.ExprWarnInContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprPick.
    def visitExprPick(self, ctx:MalloyParser.ExprPickContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#exprPathlessAggregate.
    def visitExprPathlessAggregate(self, ctx:MalloyParser.ExprPathlessAggregateContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#partialCompare.
    def visitPartialCompare(self, ctx:MalloyParser.PartialCompareContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#partialTest.
    def visitPartialTest(self, ctx:MalloyParser.PartialTestContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#partialAllowedFieldExpr.
    def visitPartialAllowedFieldExpr(self, ctx:MalloyParser.PartialAllowedFieldExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#fieldExprList.
    def visitFieldExprList(self, ctx:MalloyParser.FieldExprListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#pickStatement.
    def visitPickStatement(self, ctx:MalloyParser.PickStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#pick.
    def visitPick(self, ctx:MalloyParser.PickContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#caseStatement.
    def visitCaseStatement(self, ctx:MalloyParser.CaseStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#caseWhen.
    def visitCaseWhen(self, ctx:MalloyParser.CaseWhenContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#recordKey.
    def visitRecordKey(self, ctx:MalloyParser.RecordKeyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#recordRef.
    def visitRecordRef(self, ctx:MalloyParser.RecordRefContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#recordExpr.
    def visitRecordExpr(self, ctx:MalloyParser.RecordExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#argumentList.
    def visitArgumentList(self, ctx:MalloyParser.ArgumentListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#fieldNameList.
    def visitFieldNameList(self, ctx:MalloyParser.FieldNameListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#fieldCollection.
    def visitFieldCollection(self, ctx:MalloyParser.FieldCollectionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#collectionWildCard.
    def visitCollectionWildCard(self, ctx:MalloyParser.CollectionWildCardContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#starQualified.
    def visitStarQualified(self, ctx:MalloyParser.StarQualifiedContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#taggedRef.
    def visitTaggedRef(self, ctx:MalloyParser.TaggedRefContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#refExpr.
    def visitRefExpr(self, ctx:MalloyParser.RefExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#collectionMember.
    def visitCollectionMember(self, ctx:MalloyParser.CollectionMemberContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#fieldPath.
    def visitFieldPath(self, ctx:MalloyParser.FieldPathContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#joinName.
    def visitJoinName(self, ctx:MalloyParser.JoinNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#fieldName.
    def visitFieldName(self, ctx:MalloyParser.FieldNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#sqlExploreNameRef.
    def visitSqlExploreNameRef(self, ctx:MalloyParser.SqlExploreNameRefContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#nameSQLBlock.
    def visitNameSQLBlock(self, ctx:MalloyParser.NameSQLBlockContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#connectionName.
    def visitConnectionName(self, ctx:MalloyParser.ConnectionNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#tripFilterString.
    def visitTripFilterString(self, ctx:MalloyParser.TripFilterStringContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#tickFilterString.
    def visitTickFilterString(self, ctx:MalloyParser.TickFilterStringContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#filterString.
    def visitFilterString(self, ctx:MalloyParser.FilterStringContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#debugExpr.
    def visitDebugExpr(self, ctx:MalloyParser.DebugExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#debugPartial.
    def visitDebugPartial(self, ctx:MalloyParser.DebugPartialContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by MalloyParser#experimentalStatementForTesting.
    def visitExperimentalStatementForTesting(self, ctx:MalloyParser.ExperimentalStatementForTestingContext):
        return self.visitChildren(ctx)



del MalloyParser