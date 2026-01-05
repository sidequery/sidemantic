# Generated from MalloyParser.g4 by ANTLR 4.13.2
from antlr4 import *
if "." in __name__:
    from .MalloyParser import MalloyParser
else:
    from MalloyParser import MalloyParser

# This class defines a complete listener for a parse tree produced by MalloyParser.
class MalloyParserListener(ParseTreeListener):

    # Enter a parse tree produced by MalloyParser#malloyDocument.
    def enterMalloyDocument(self, ctx:MalloyParser.MalloyDocumentContext):
        pass

    # Exit a parse tree produced by MalloyParser#malloyDocument.
    def exitMalloyDocument(self, ctx:MalloyParser.MalloyDocumentContext):
        pass


    # Enter a parse tree produced by MalloyParser#malloyStatement.
    def enterMalloyStatement(self, ctx:MalloyParser.MalloyStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#malloyStatement.
    def exitMalloyStatement(self, ctx:MalloyParser.MalloyStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#defineSourceStatement.
    def enterDefineSourceStatement(self, ctx:MalloyParser.DefineSourceStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#defineSourceStatement.
    def exitDefineSourceStatement(self, ctx:MalloyParser.DefineSourceStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#use_top_level_query_defs.
    def enterUse_top_level_query_defs(self, ctx:MalloyParser.Use_top_level_query_defsContext):
        pass

    # Exit a parse tree produced by MalloyParser#use_top_level_query_defs.
    def exitUse_top_level_query_defs(self, ctx:MalloyParser.Use_top_level_query_defsContext):
        pass


    # Enter a parse tree produced by MalloyParser#topLevelAnonQueryDef.
    def enterTopLevelAnonQueryDef(self, ctx:MalloyParser.TopLevelAnonQueryDefContext):
        pass

    # Exit a parse tree produced by MalloyParser#topLevelAnonQueryDef.
    def exitTopLevelAnonQueryDef(self, ctx:MalloyParser.TopLevelAnonQueryDefContext):
        pass


    # Enter a parse tree produced by MalloyParser#tags.
    def enterTags(self, ctx:MalloyParser.TagsContext):
        pass

    # Exit a parse tree produced by MalloyParser#tags.
    def exitTags(self, ctx:MalloyParser.TagsContext):
        pass


    # Enter a parse tree produced by MalloyParser#isDefine.
    def enterIsDefine(self, ctx:MalloyParser.IsDefineContext):
        pass

    # Exit a parse tree produced by MalloyParser#isDefine.
    def exitIsDefine(self, ctx:MalloyParser.IsDefineContext):
        pass


    # Enter a parse tree produced by MalloyParser#runStatement.
    def enterRunStatement(self, ctx:MalloyParser.RunStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#runStatement.
    def exitRunStatement(self, ctx:MalloyParser.RunStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#sqlString.
    def enterSqlString(self, ctx:MalloyParser.SqlStringContext):
        pass

    # Exit a parse tree produced by MalloyParser#sqlString.
    def exitSqlString(self, ctx:MalloyParser.SqlStringContext):
        pass


    # Enter a parse tree produced by MalloyParser#sqlInterpolation.
    def enterSqlInterpolation(self, ctx:MalloyParser.SqlInterpolationContext):
        pass

    # Exit a parse tree produced by MalloyParser#sqlInterpolation.
    def exitSqlInterpolation(self, ctx:MalloyParser.SqlInterpolationContext):
        pass


    # Enter a parse tree produced by MalloyParser#importStatement.
    def enterImportStatement(self, ctx:MalloyParser.ImportStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#importStatement.
    def exitImportStatement(self, ctx:MalloyParser.ImportStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#importSelect.
    def enterImportSelect(self, ctx:MalloyParser.ImportSelectContext):
        pass

    # Exit a parse tree produced by MalloyParser#importSelect.
    def exitImportSelect(self, ctx:MalloyParser.ImportSelectContext):
        pass


    # Enter a parse tree produced by MalloyParser#importItem.
    def enterImportItem(self, ctx:MalloyParser.ImportItemContext):
        pass

    # Exit a parse tree produced by MalloyParser#importItem.
    def exitImportItem(self, ctx:MalloyParser.ImportItemContext):
        pass


    # Enter a parse tree produced by MalloyParser#importURL.
    def enterImportURL(self, ctx:MalloyParser.ImportURLContext):
        pass

    # Exit a parse tree produced by MalloyParser#importURL.
    def exitImportURL(self, ctx:MalloyParser.ImportURLContext):
        pass


    # Enter a parse tree produced by MalloyParser#docAnnotations.
    def enterDocAnnotations(self, ctx:MalloyParser.DocAnnotationsContext):
        pass

    # Exit a parse tree produced by MalloyParser#docAnnotations.
    def exitDocAnnotations(self, ctx:MalloyParser.DocAnnotationsContext):
        pass


    # Enter a parse tree produced by MalloyParser#ignoredObjectAnnotations.
    def enterIgnoredObjectAnnotations(self, ctx:MalloyParser.IgnoredObjectAnnotationsContext):
        pass

    # Exit a parse tree produced by MalloyParser#ignoredObjectAnnotations.
    def exitIgnoredObjectAnnotations(self, ctx:MalloyParser.IgnoredObjectAnnotationsContext):
        pass


    # Enter a parse tree produced by MalloyParser#ignoredModelAnnotations.
    def enterIgnoredModelAnnotations(self, ctx:MalloyParser.IgnoredModelAnnotationsContext):
        pass

    # Exit a parse tree produced by MalloyParser#ignoredModelAnnotations.
    def exitIgnoredModelAnnotations(self, ctx:MalloyParser.IgnoredModelAnnotationsContext):
        pass


    # Enter a parse tree produced by MalloyParser#topLevelQueryDefs.
    def enterTopLevelQueryDefs(self, ctx:MalloyParser.TopLevelQueryDefsContext):
        pass

    # Exit a parse tree produced by MalloyParser#topLevelQueryDefs.
    def exitTopLevelQueryDefs(self, ctx:MalloyParser.TopLevelQueryDefsContext):
        pass


    # Enter a parse tree produced by MalloyParser#topLevelQueryDef.
    def enterTopLevelQueryDef(self, ctx:MalloyParser.TopLevelQueryDefContext):
        pass

    # Exit a parse tree produced by MalloyParser#topLevelQueryDef.
    def exitTopLevelQueryDef(self, ctx:MalloyParser.TopLevelQueryDefContext):
        pass


    # Enter a parse tree produced by MalloyParser#refineOperator.
    def enterRefineOperator(self, ctx:MalloyParser.RefineOperatorContext):
        pass

    # Exit a parse tree produced by MalloyParser#refineOperator.
    def exitRefineOperator(self, ctx:MalloyParser.RefineOperatorContext):
        pass


    # Enter a parse tree produced by MalloyParser#turtleName.
    def enterTurtleName(self, ctx:MalloyParser.TurtleNameContext):
        pass

    # Exit a parse tree produced by MalloyParser#turtleName.
    def exitTurtleName(self, ctx:MalloyParser.TurtleNameContext):
        pass


    # Enter a parse tree produced by MalloyParser#sqlSource.
    def enterSqlSource(self, ctx:MalloyParser.SqlSourceContext):
        pass

    # Exit a parse tree produced by MalloyParser#sqlSource.
    def exitSqlSource(self, ctx:MalloyParser.SqlSourceContext):
        pass


    # Enter a parse tree produced by MalloyParser#exploreTable.
    def enterExploreTable(self, ctx:MalloyParser.ExploreTableContext):
        pass

    # Exit a parse tree produced by MalloyParser#exploreTable.
    def exitExploreTable(self, ctx:MalloyParser.ExploreTableContext):
        pass


    # Enter a parse tree produced by MalloyParser#connectionId.
    def enterConnectionId(self, ctx:MalloyParser.ConnectionIdContext):
        pass

    # Exit a parse tree produced by MalloyParser#connectionId.
    def exitConnectionId(self, ctx:MalloyParser.ConnectionIdContext):
        pass


    # Enter a parse tree produced by MalloyParser#queryProperties.
    def enterQueryProperties(self, ctx:MalloyParser.QueryPropertiesContext):
        pass

    # Exit a parse tree produced by MalloyParser#queryProperties.
    def exitQueryProperties(self, ctx:MalloyParser.QueryPropertiesContext):
        pass


    # Enter a parse tree produced by MalloyParser#queryName.
    def enterQueryName(self, ctx:MalloyParser.QueryNameContext):
        pass

    # Exit a parse tree produced by MalloyParser#queryName.
    def exitQueryName(self, ctx:MalloyParser.QueryNameContext):
        pass


    # Enter a parse tree produced by MalloyParser#sourcePropertyList.
    def enterSourcePropertyList(self, ctx:MalloyParser.SourcePropertyListContext):
        pass

    # Exit a parse tree produced by MalloyParser#sourcePropertyList.
    def exitSourcePropertyList(self, ctx:MalloyParser.SourcePropertyListContext):
        pass


    # Enter a parse tree produced by MalloyParser#sourceDefinition.
    def enterSourceDefinition(self, ctx:MalloyParser.SourceDefinitionContext):
        pass

    # Exit a parse tree produced by MalloyParser#sourceDefinition.
    def exitSourceDefinition(self, ctx:MalloyParser.SourceDefinitionContext):
        pass


    # Enter a parse tree produced by MalloyParser#sqExplore.
    def enterSqExplore(self, ctx:MalloyParser.SqExploreContext):
        pass

    # Exit a parse tree produced by MalloyParser#sqExplore.
    def exitSqExplore(self, ctx:MalloyParser.SqExploreContext):
        pass


    # Enter a parse tree produced by MalloyParser#sourceParameters.
    def enterSourceParameters(self, ctx:MalloyParser.SourceParametersContext):
        pass

    # Exit a parse tree produced by MalloyParser#sourceParameters.
    def exitSourceParameters(self, ctx:MalloyParser.SourceParametersContext):
        pass


    # Enter a parse tree produced by MalloyParser#legalParamType.
    def enterLegalParamType(self, ctx:MalloyParser.LegalParamTypeContext):
        pass

    # Exit a parse tree produced by MalloyParser#legalParamType.
    def exitLegalParamType(self, ctx:MalloyParser.LegalParamTypeContext):
        pass


    # Enter a parse tree produced by MalloyParser#sourceParameter.
    def enterSourceParameter(self, ctx:MalloyParser.SourceParameterContext):
        pass

    # Exit a parse tree produced by MalloyParser#sourceParameter.
    def exitSourceParameter(self, ctx:MalloyParser.SourceParameterContext):
        pass


    # Enter a parse tree produced by MalloyParser#parameterNameDef.
    def enterParameterNameDef(self, ctx:MalloyParser.ParameterNameDefContext):
        pass

    # Exit a parse tree produced by MalloyParser#parameterNameDef.
    def exitParameterNameDef(self, ctx:MalloyParser.ParameterNameDefContext):
        pass


    # Enter a parse tree produced by MalloyParser#sourceNameDef.
    def enterSourceNameDef(self, ctx:MalloyParser.SourceNameDefContext):
        pass

    # Exit a parse tree produced by MalloyParser#sourceNameDef.
    def exitSourceNameDef(self, ctx:MalloyParser.SourceNameDefContext):
        pass


    # Enter a parse tree produced by MalloyParser#exploreProperties.
    def enterExploreProperties(self, ctx:MalloyParser.ExplorePropertiesContext):
        pass

    # Exit a parse tree produced by MalloyParser#exploreProperties.
    def exitExploreProperties(self, ctx:MalloyParser.ExplorePropertiesContext):
        pass


    # Enter a parse tree produced by MalloyParser#defExploreDimension_stub.
    def enterDefExploreDimension_stub(self, ctx:MalloyParser.DefExploreDimension_stubContext):
        pass

    # Exit a parse tree produced by MalloyParser#defExploreDimension_stub.
    def exitDefExploreDimension_stub(self, ctx:MalloyParser.DefExploreDimension_stubContext):
        pass


    # Enter a parse tree produced by MalloyParser#defExploreMeasure_stub.
    def enterDefExploreMeasure_stub(self, ctx:MalloyParser.DefExploreMeasure_stubContext):
        pass

    # Exit a parse tree produced by MalloyParser#defExploreMeasure_stub.
    def exitDefExploreMeasure_stub(self, ctx:MalloyParser.DefExploreMeasure_stubContext):
        pass


    # Enter a parse tree produced by MalloyParser#defJoin_stub.
    def enterDefJoin_stub(self, ctx:MalloyParser.DefJoin_stubContext):
        pass

    # Exit a parse tree produced by MalloyParser#defJoin_stub.
    def exitDefJoin_stub(self, ctx:MalloyParser.DefJoin_stubContext):
        pass


    # Enter a parse tree produced by MalloyParser#defExploreWhere_stub.
    def enterDefExploreWhere_stub(self, ctx:MalloyParser.DefExploreWhere_stubContext):
        pass

    # Exit a parse tree produced by MalloyParser#defExploreWhere_stub.
    def exitDefExploreWhere_stub(self, ctx:MalloyParser.DefExploreWhere_stubContext):
        pass


    # Enter a parse tree produced by MalloyParser#defExplorePrimaryKey.
    def enterDefExplorePrimaryKey(self, ctx:MalloyParser.DefExplorePrimaryKeyContext):
        pass

    # Exit a parse tree produced by MalloyParser#defExplorePrimaryKey.
    def exitDefExplorePrimaryKey(self, ctx:MalloyParser.DefExplorePrimaryKeyContext):
        pass


    # Enter a parse tree produced by MalloyParser#defExploreRename.
    def enterDefExploreRename(self, ctx:MalloyParser.DefExploreRenameContext):
        pass

    # Exit a parse tree produced by MalloyParser#defExploreRename.
    def exitDefExploreRename(self, ctx:MalloyParser.DefExploreRenameContext):
        pass


    # Enter a parse tree produced by MalloyParser#defExploreEditField.
    def enterDefExploreEditField(self, ctx:MalloyParser.DefExploreEditFieldContext):
        pass

    # Exit a parse tree produced by MalloyParser#defExploreEditField.
    def exitDefExploreEditField(self, ctx:MalloyParser.DefExploreEditFieldContext):
        pass


    # Enter a parse tree produced by MalloyParser#defExploreQuery.
    def enterDefExploreQuery(self, ctx:MalloyParser.DefExploreQueryContext):
        pass

    # Exit a parse tree produced by MalloyParser#defExploreQuery.
    def exitDefExploreQuery(self, ctx:MalloyParser.DefExploreQueryContext):
        pass


    # Enter a parse tree produced by MalloyParser#defExploreTimezone.
    def enterDefExploreTimezone(self, ctx:MalloyParser.DefExploreTimezoneContext):
        pass

    # Exit a parse tree produced by MalloyParser#defExploreTimezone.
    def exitDefExploreTimezone(self, ctx:MalloyParser.DefExploreTimezoneContext):
        pass


    # Enter a parse tree produced by MalloyParser#defExploreAnnotation.
    def enterDefExploreAnnotation(self, ctx:MalloyParser.DefExploreAnnotationContext):
        pass

    # Exit a parse tree produced by MalloyParser#defExploreAnnotation.
    def exitDefExploreAnnotation(self, ctx:MalloyParser.DefExploreAnnotationContext):
        pass


    # Enter a parse tree produced by MalloyParser#defIgnoreModel_stub.
    def enterDefIgnoreModel_stub(self, ctx:MalloyParser.DefIgnoreModel_stubContext):
        pass

    # Exit a parse tree produced by MalloyParser#defIgnoreModel_stub.
    def exitDefIgnoreModel_stub(self, ctx:MalloyParser.DefIgnoreModel_stubContext):
        pass


    # Enter a parse tree produced by MalloyParser#accessLabel.
    def enterAccessLabel(self, ctx:MalloyParser.AccessLabelContext):
        pass

    # Exit a parse tree produced by MalloyParser#accessLabel.
    def exitAccessLabel(self, ctx:MalloyParser.AccessLabelContext):
        pass


    # Enter a parse tree produced by MalloyParser#accessModifierList.
    def enterAccessModifierList(self, ctx:MalloyParser.AccessModifierListContext):
        pass

    # Exit a parse tree produced by MalloyParser#accessModifierList.
    def exitAccessModifierList(self, ctx:MalloyParser.AccessModifierListContext):
        pass


    # Enter a parse tree produced by MalloyParser#defMeasures.
    def enterDefMeasures(self, ctx:MalloyParser.DefMeasuresContext):
        pass

    # Exit a parse tree produced by MalloyParser#defMeasures.
    def exitDefMeasures(self, ctx:MalloyParser.DefMeasuresContext):
        pass


    # Enter a parse tree produced by MalloyParser#defDimensions.
    def enterDefDimensions(self, ctx:MalloyParser.DefDimensionsContext):
        pass

    # Exit a parse tree produced by MalloyParser#defDimensions.
    def exitDefDimensions(self, ctx:MalloyParser.DefDimensionsContext):
        pass


    # Enter a parse tree produced by MalloyParser#renameList.
    def enterRenameList(self, ctx:MalloyParser.RenameListContext):
        pass

    # Exit a parse tree produced by MalloyParser#renameList.
    def exitRenameList(self, ctx:MalloyParser.RenameListContext):
        pass


    # Enter a parse tree produced by MalloyParser#renameEntry.
    def enterRenameEntry(self, ctx:MalloyParser.RenameEntryContext):
        pass

    # Exit a parse tree produced by MalloyParser#renameEntry.
    def exitRenameEntry(self, ctx:MalloyParser.RenameEntryContext):
        pass


    # Enter a parse tree produced by MalloyParser#defList.
    def enterDefList(self, ctx:MalloyParser.DefListContext):
        pass

    # Exit a parse tree produced by MalloyParser#defList.
    def exitDefList(self, ctx:MalloyParser.DefListContext):
        pass


    # Enter a parse tree produced by MalloyParser#fieldDef.
    def enterFieldDef(self, ctx:MalloyParser.FieldDefContext):
        pass

    # Exit a parse tree produced by MalloyParser#fieldDef.
    def exitFieldDef(self, ctx:MalloyParser.FieldDefContext):
        pass


    # Enter a parse tree produced by MalloyParser#fieldNameDef.
    def enterFieldNameDef(self, ctx:MalloyParser.FieldNameDefContext):
        pass

    # Exit a parse tree produced by MalloyParser#fieldNameDef.
    def exitFieldNameDef(self, ctx:MalloyParser.FieldNameDefContext):
        pass


    # Enter a parse tree produced by MalloyParser#joinNameDef.
    def enterJoinNameDef(self, ctx:MalloyParser.JoinNameDefContext):
        pass

    # Exit a parse tree produced by MalloyParser#joinNameDef.
    def exitJoinNameDef(self, ctx:MalloyParser.JoinNameDefContext):
        pass


    # Enter a parse tree produced by MalloyParser#declareStatement.
    def enterDeclareStatement(self, ctx:MalloyParser.DeclareStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#declareStatement.
    def exitDeclareStatement(self, ctx:MalloyParser.DeclareStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#defJoinOne.
    def enterDefJoinOne(self, ctx:MalloyParser.DefJoinOneContext):
        pass

    # Exit a parse tree produced by MalloyParser#defJoinOne.
    def exitDefJoinOne(self, ctx:MalloyParser.DefJoinOneContext):
        pass


    # Enter a parse tree produced by MalloyParser#defJoinMany.
    def enterDefJoinMany(self, ctx:MalloyParser.DefJoinManyContext):
        pass

    # Exit a parse tree produced by MalloyParser#defJoinMany.
    def exitDefJoinMany(self, ctx:MalloyParser.DefJoinManyContext):
        pass


    # Enter a parse tree produced by MalloyParser#defJoinCross.
    def enterDefJoinCross(self, ctx:MalloyParser.DefJoinCrossContext):
        pass

    # Exit a parse tree produced by MalloyParser#defJoinCross.
    def exitDefJoinCross(self, ctx:MalloyParser.DefJoinCrossContext):
        pass


    # Enter a parse tree produced by MalloyParser#queryExtend.
    def enterQueryExtend(self, ctx:MalloyParser.QueryExtendContext):
        pass

    # Exit a parse tree produced by MalloyParser#queryExtend.
    def exitQueryExtend(self, ctx:MalloyParser.QueryExtendContext):
        pass


    # Enter a parse tree produced by MalloyParser#modEither.
    def enterModEither(self, ctx:MalloyParser.ModEitherContext):
        pass

    # Exit a parse tree produced by MalloyParser#modEither.
    def exitModEither(self, ctx:MalloyParser.ModEitherContext):
        pass


    # Enter a parse tree produced by MalloyParser#sourceArguments.
    def enterSourceArguments(self, ctx:MalloyParser.SourceArgumentsContext):
        pass

    # Exit a parse tree produced by MalloyParser#sourceArguments.
    def exitSourceArguments(self, ctx:MalloyParser.SourceArgumentsContext):
        pass


    # Enter a parse tree produced by MalloyParser#argumentId.
    def enterArgumentId(self, ctx:MalloyParser.ArgumentIdContext):
        pass

    # Exit a parse tree produced by MalloyParser#argumentId.
    def exitArgumentId(self, ctx:MalloyParser.ArgumentIdContext):
        pass


    # Enter a parse tree produced by MalloyParser#sourceArgument.
    def enterSourceArgument(self, ctx:MalloyParser.SourceArgumentContext):
        pass

    # Exit a parse tree produced by MalloyParser#sourceArgument.
    def exitSourceArgument(self, ctx:MalloyParser.SourceArgumentContext):
        pass


    # Enter a parse tree produced by MalloyParser#SQRefinedQuery.
    def enterSQRefinedQuery(self, ctx:MalloyParser.SQRefinedQueryContext):
        pass

    # Exit a parse tree produced by MalloyParser#SQRefinedQuery.
    def exitSQRefinedQuery(self, ctx:MalloyParser.SQRefinedQueryContext):
        pass


    # Enter a parse tree produced by MalloyParser#SQSQL.
    def enterSQSQL(self, ctx:MalloyParser.SQSQLContext):
        pass

    # Exit a parse tree produced by MalloyParser#SQSQL.
    def exitSQSQL(self, ctx:MalloyParser.SQSQLContext):
        pass


    # Enter a parse tree produced by MalloyParser#SQExtendedSource.
    def enterSQExtendedSource(self, ctx:MalloyParser.SQExtendedSourceContext):
        pass

    # Exit a parse tree produced by MalloyParser#SQExtendedSource.
    def exitSQExtendedSource(self, ctx:MalloyParser.SQExtendedSourceContext):
        pass


    # Enter a parse tree produced by MalloyParser#SQCompose.
    def enterSQCompose(self, ctx:MalloyParser.SQComposeContext):
        pass

    # Exit a parse tree produced by MalloyParser#SQCompose.
    def exitSQCompose(self, ctx:MalloyParser.SQComposeContext):
        pass


    # Enter a parse tree produced by MalloyParser#SQTable.
    def enterSQTable(self, ctx:MalloyParser.SQTableContext):
        pass

    # Exit a parse tree produced by MalloyParser#SQTable.
    def exitSQTable(self, ctx:MalloyParser.SQTableContext):
        pass


    # Enter a parse tree produced by MalloyParser#SQInclude.
    def enterSQInclude(self, ctx:MalloyParser.SQIncludeContext):
        pass

    # Exit a parse tree produced by MalloyParser#SQInclude.
    def exitSQInclude(self, ctx:MalloyParser.SQIncludeContext):
        pass


    # Enter a parse tree produced by MalloyParser#SQArrow.
    def enterSQArrow(self, ctx:MalloyParser.SQArrowContext):
        pass

    # Exit a parse tree produced by MalloyParser#SQArrow.
    def exitSQArrow(self, ctx:MalloyParser.SQArrowContext):
        pass


    # Enter a parse tree produced by MalloyParser#SQParens.
    def enterSQParens(self, ctx:MalloyParser.SQParensContext):
        pass

    # Exit a parse tree produced by MalloyParser#SQParens.
    def exitSQParens(self, ctx:MalloyParser.SQParensContext):
        pass


    # Enter a parse tree produced by MalloyParser#SQID.
    def enterSQID(self, ctx:MalloyParser.SQIDContext):
        pass

    # Exit a parse tree produced by MalloyParser#SQID.
    def exitSQID(self, ctx:MalloyParser.SQIDContext):
        pass


    # Enter a parse tree produced by MalloyParser#includeBlock.
    def enterIncludeBlock(self, ctx:MalloyParser.IncludeBlockContext):
        pass

    # Exit a parse tree produced by MalloyParser#includeBlock.
    def exitIncludeBlock(self, ctx:MalloyParser.IncludeBlockContext):
        pass


    # Enter a parse tree produced by MalloyParser#includeItem.
    def enterIncludeItem(self, ctx:MalloyParser.IncludeItemContext):
        pass

    # Exit a parse tree produced by MalloyParser#includeItem.
    def exitIncludeItem(self, ctx:MalloyParser.IncludeItemContext):
        pass


    # Enter a parse tree produced by MalloyParser#orphanedAnnotation.
    def enterOrphanedAnnotation(self, ctx:MalloyParser.OrphanedAnnotationContext):
        pass

    # Exit a parse tree produced by MalloyParser#orphanedAnnotation.
    def exitOrphanedAnnotation(self, ctx:MalloyParser.OrphanedAnnotationContext):
        pass


    # Enter a parse tree produced by MalloyParser#accessLabelProp.
    def enterAccessLabelProp(self, ctx:MalloyParser.AccessLabelPropContext):
        pass

    # Exit a parse tree produced by MalloyParser#accessLabelProp.
    def exitAccessLabelProp(self, ctx:MalloyParser.AccessLabelPropContext):
        pass


    # Enter a parse tree produced by MalloyParser#includeExceptList.
    def enterIncludeExceptList(self, ctx:MalloyParser.IncludeExceptListContext):
        pass

    # Exit a parse tree produced by MalloyParser#includeExceptList.
    def exitIncludeExceptList(self, ctx:MalloyParser.IncludeExceptListContext):
        pass


    # Enter a parse tree produced by MalloyParser#includeExceptListItem.
    def enterIncludeExceptListItem(self, ctx:MalloyParser.IncludeExceptListItemContext):
        pass

    # Exit a parse tree produced by MalloyParser#includeExceptListItem.
    def exitIncludeExceptListItem(self, ctx:MalloyParser.IncludeExceptListItemContext):
        pass


    # Enter a parse tree produced by MalloyParser#includeList.
    def enterIncludeList(self, ctx:MalloyParser.IncludeListContext):
        pass

    # Exit a parse tree produced by MalloyParser#includeList.
    def exitIncludeList(self, ctx:MalloyParser.IncludeListContext):
        pass


    # Enter a parse tree produced by MalloyParser#includeField.
    def enterIncludeField(self, ctx:MalloyParser.IncludeFieldContext):
        pass

    # Exit a parse tree produced by MalloyParser#includeField.
    def exitIncludeField(self, ctx:MalloyParser.IncludeFieldContext):
        pass


    # Enter a parse tree produced by MalloyParser#SegField.
    def enterSegField(self, ctx:MalloyParser.SegFieldContext):
        pass

    # Exit a parse tree produced by MalloyParser#SegField.
    def exitSegField(self, ctx:MalloyParser.SegFieldContext):
        pass


    # Enter a parse tree produced by MalloyParser#SegRefine.
    def enterSegRefine(self, ctx:MalloyParser.SegRefineContext):
        pass

    # Exit a parse tree produced by MalloyParser#SegRefine.
    def exitSegRefine(self, ctx:MalloyParser.SegRefineContext):
        pass


    # Enter a parse tree produced by MalloyParser#SegParen.
    def enterSegParen(self, ctx:MalloyParser.SegParenContext):
        pass

    # Exit a parse tree produced by MalloyParser#SegParen.
    def exitSegParen(self, ctx:MalloyParser.SegParenContext):
        pass


    # Enter a parse tree produced by MalloyParser#SegOps.
    def enterSegOps(self, ctx:MalloyParser.SegOpsContext):
        pass

    # Exit a parse tree produced by MalloyParser#SegOps.
    def exitSegOps(self, ctx:MalloyParser.SegOpsContext):
        pass


    # Enter a parse tree produced by MalloyParser#VSeg.
    def enterVSeg(self, ctx:MalloyParser.VSegContext):
        pass

    # Exit a parse tree produced by MalloyParser#VSeg.
    def exitVSeg(self, ctx:MalloyParser.VSegContext):
        pass


    # Enter a parse tree produced by MalloyParser#VArrow.
    def enterVArrow(self, ctx:MalloyParser.VArrowContext):
        pass

    # Exit a parse tree produced by MalloyParser#VArrow.
    def exitVArrow(self, ctx:MalloyParser.VArrowContext):
        pass


    # Enter a parse tree produced by MalloyParser#queryExtendStatement.
    def enterQueryExtendStatement(self, ctx:MalloyParser.QueryExtendStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#queryExtendStatement.
    def exitQueryExtendStatement(self, ctx:MalloyParser.QueryExtendStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#queryExtendStatementList.
    def enterQueryExtendStatementList(self, ctx:MalloyParser.QueryExtendStatementListContext):
        pass

    # Exit a parse tree produced by MalloyParser#queryExtendStatementList.
    def exitQueryExtendStatementList(self, ctx:MalloyParser.QueryExtendStatementListContext):
        pass


    # Enter a parse tree produced by MalloyParser#joinList.
    def enterJoinList(self, ctx:MalloyParser.JoinListContext):
        pass

    # Exit a parse tree produced by MalloyParser#joinList.
    def exitJoinList(self, ctx:MalloyParser.JoinListContext):
        pass


    # Enter a parse tree produced by MalloyParser#isExplore.
    def enterIsExplore(self, ctx:MalloyParser.IsExploreContext):
        pass

    # Exit a parse tree produced by MalloyParser#isExplore.
    def exitIsExplore(self, ctx:MalloyParser.IsExploreContext):
        pass


    # Enter a parse tree produced by MalloyParser#matrixOperation.
    def enterMatrixOperation(self, ctx:MalloyParser.MatrixOperationContext):
        pass

    # Exit a parse tree produced by MalloyParser#matrixOperation.
    def exitMatrixOperation(self, ctx:MalloyParser.MatrixOperationContext):
        pass


    # Enter a parse tree produced by MalloyParser#joinFrom.
    def enterJoinFrom(self, ctx:MalloyParser.JoinFromContext):
        pass

    # Exit a parse tree produced by MalloyParser#joinFrom.
    def exitJoinFrom(self, ctx:MalloyParser.JoinFromContext):
        pass


    # Enter a parse tree produced by MalloyParser#joinWith.
    def enterJoinWith(self, ctx:MalloyParser.JoinWithContext):
        pass

    # Exit a parse tree produced by MalloyParser#joinWith.
    def exitJoinWith(self, ctx:MalloyParser.JoinWithContext):
        pass


    # Enter a parse tree produced by MalloyParser#joinOn.
    def enterJoinOn(self, ctx:MalloyParser.JoinOnContext):
        pass

    # Exit a parse tree produced by MalloyParser#joinOn.
    def exitJoinOn(self, ctx:MalloyParser.JoinOnContext):
        pass


    # Enter a parse tree produced by MalloyParser#joinExpression.
    def enterJoinExpression(self, ctx:MalloyParser.JoinExpressionContext):
        pass

    # Exit a parse tree produced by MalloyParser#joinExpression.
    def exitJoinExpression(self, ctx:MalloyParser.JoinExpressionContext):
        pass


    # Enter a parse tree produced by MalloyParser#filterStatement.
    def enterFilterStatement(self, ctx:MalloyParser.FilterStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#filterStatement.
    def exitFilterStatement(self, ctx:MalloyParser.FilterStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#fieldProperties.
    def enterFieldProperties(self, ctx:MalloyParser.FieldPropertiesContext):
        pass

    # Exit a parse tree produced by MalloyParser#fieldProperties.
    def exitFieldProperties(self, ctx:MalloyParser.FieldPropertiesContext):
        pass


    # Enter a parse tree produced by MalloyParser#aggregateOrdering.
    def enterAggregateOrdering(self, ctx:MalloyParser.AggregateOrderingContext):
        pass

    # Exit a parse tree produced by MalloyParser#aggregateOrdering.
    def exitAggregateOrdering(self, ctx:MalloyParser.AggregateOrderingContext):
        pass


    # Enter a parse tree produced by MalloyParser#aggregateOrderBySpec.
    def enterAggregateOrderBySpec(self, ctx:MalloyParser.AggregateOrderBySpecContext):
        pass

    # Exit a parse tree produced by MalloyParser#aggregateOrderBySpec.
    def exitAggregateOrderBySpec(self, ctx:MalloyParser.AggregateOrderBySpecContext):
        pass


    # Enter a parse tree produced by MalloyParser#aggregateOrderByStatement.
    def enterAggregateOrderByStatement(self, ctx:MalloyParser.AggregateOrderByStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#aggregateOrderByStatement.
    def exitAggregateOrderByStatement(self, ctx:MalloyParser.AggregateOrderByStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#fieldPropertyLimitStatement.
    def enterFieldPropertyLimitStatement(self, ctx:MalloyParser.FieldPropertyLimitStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#fieldPropertyLimitStatement.
    def exitFieldPropertyLimitStatement(self, ctx:MalloyParser.FieldPropertyLimitStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#fieldPropertyStatement.
    def enterFieldPropertyStatement(self, ctx:MalloyParser.FieldPropertyStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#fieldPropertyStatement.
    def exitFieldPropertyStatement(self, ctx:MalloyParser.FieldPropertyStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#filterClauseList.
    def enterFilterClauseList(self, ctx:MalloyParser.FilterClauseListContext):
        pass

    # Exit a parse tree produced by MalloyParser#filterClauseList.
    def exitFilterClauseList(self, ctx:MalloyParser.FilterClauseListContext):
        pass


    # Enter a parse tree produced by MalloyParser#whereStatement.
    def enterWhereStatement(self, ctx:MalloyParser.WhereStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#whereStatement.
    def exitWhereStatement(self, ctx:MalloyParser.WhereStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#havingStatement.
    def enterHavingStatement(self, ctx:MalloyParser.HavingStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#havingStatement.
    def exitHavingStatement(self, ctx:MalloyParser.HavingStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#subQueryDefList.
    def enterSubQueryDefList(self, ctx:MalloyParser.SubQueryDefListContext):
        pass

    # Exit a parse tree produced by MalloyParser#subQueryDefList.
    def exitSubQueryDefList(self, ctx:MalloyParser.SubQueryDefListContext):
        pass


    # Enter a parse tree produced by MalloyParser#exploreQueryNameDef.
    def enterExploreQueryNameDef(self, ctx:MalloyParser.ExploreQueryNameDefContext):
        pass

    # Exit a parse tree produced by MalloyParser#exploreQueryNameDef.
    def exitExploreQueryNameDef(self, ctx:MalloyParser.ExploreQueryNameDefContext):
        pass


    # Enter a parse tree produced by MalloyParser#exploreQueryDef.
    def enterExploreQueryDef(self, ctx:MalloyParser.ExploreQueryDefContext):
        pass

    # Exit a parse tree produced by MalloyParser#exploreQueryDef.
    def exitExploreQueryDef(self, ctx:MalloyParser.ExploreQueryDefContext):
        pass


    # Enter a parse tree produced by MalloyParser#drillStatement.
    def enterDrillStatement(self, ctx:MalloyParser.DrillStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#drillStatement.
    def exitDrillStatement(self, ctx:MalloyParser.DrillStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#drillClauseList.
    def enterDrillClauseList(self, ctx:MalloyParser.DrillClauseListContext):
        pass

    # Exit a parse tree produced by MalloyParser#drillClauseList.
    def exitDrillClauseList(self, ctx:MalloyParser.DrillClauseListContext):
        pass


    # Enter a parse tree produced by MalloyParser#queryStatement.
    def enterQueryStatement(self, ctx:MalloyParser.QueryStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#queryStatement.
    def exitQueryStatement(self, ctx:MalloyParser.QueryStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#queryJoinStatement.
    def enterQueryJoinStatement(self, ctx:MalloyParser.QueryJoinStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#queryJoinStatement.
    def exitQueryJoinStatement(self, ctx:MalloyParser.QueryJoinStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#groupByStatement.
    def enterGroupByStatement(self, ctx:MalloyParser.GroupByStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#groupByStatement.
    def exitGroupByStatement(self, ctx:MalloyParser.GroupByStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#queryFieldList.
    def enterQueryFieldList(self, ctx:MalloyParser.QueryFieldListContext):
        pass

    # Exit a parse tree produced by MalloyParser#queryFieldList.
    def exitQueryFieldList(self, ctx:MalloyParser.QueryFieldListContext):
        pass


    # Enter a parse tree produced by MalloyParser#queryFieldEntry.
    def enterQueryFieldEntry(self, ctx:MalloyParser.QueryFieldEntryContext):
        pass

    # Exit a parse tree produced by MalloyParser#queryFieldEntry.
    def exitQueryFieldEntry(self, ctx:MalloyParser.QueryFieldEntryContext):
        pass


    # Enter a parse tree produced by MalloyParser#nestStatement.
    def enterNestStatement(self, ctx:MalloyParser.NestStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#nestStatement.
    def exitNestStatement(self, ctx:MalloyParser.NestStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#nestedQueryList.
    def enterNestedQueryList(self, ctx:MalloyParser.NestedQueryListContext):
        pass

    # Exit a parse tree produced by MalloyParser#nestedQueryList.
    def exitNestedQueryList(self, ctx:MalloyParser.NestedQueryListContext):
        pass


    # Enter a parse tree produced by MalloyParser#nestDef.
    def enterNestDef(self, ctx:MalloyParser.NestDefContext):
        pass

    # Exit a parse tree produced by MalloyParser#nestDef.
    def exitNestDef(self, ctx:MalloyParser.NestDefContext):
        pass


    # Enter a parse tree produced by MalloyParser#aggregateStatement.
    def enterAggregateStatement(self, ctx:MalloyParser.AggregateStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#aggregateStatement.
    def exitAggregateStatement(self, ctx:MalloyParser.AggregateStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#calculateStatement.
    def enterCalculateStatement(self, ctx:MalloyParser.CalculateStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#calculateStatement.
    def exitCalculateStatement(self, ctx:MalloyParser.CalculateStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#projectStatement.
    def enterProjectStatement(self, ctx:MalloyParser.ProjectStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#projectStatement.
    def exitProjectStatement(self, ctx:MalloyParser.ProjectStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#partitionByStatement.
    def enterPartitionByStatement(self, ctx:MalloyParser.PartitionByStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#partitionByStatement.
    def exitPartitionByStatement(self, ctx:MalloyParser.PartitionByStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#groupedByStatement.
    def enterGroupedByStatement(self, ctx:MalloyParser.GroupedByStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#groupedByStatement.
    def exitGroupedByStatement(self, ctx:MalloyParser.GroupedByStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#orderByStatement.
    def enterOrderByStatement(self, ctx:MalloyParser.OrderByStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#orderByStatement.
    def exitOrderByStatement(self, ctx:MalloyParser.OrderByStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#ordering.
    def enterOrdering(self, ctx:MalloyParser.OrderingContext):
        pass

    # Exit a parse tree produced by MalloyParser#ordering.
    def exitOrdering(self, ctx:MalloyParser.OrderingContext):
        pass


    # Enter a parse tree produced by MalloyParser#orderBySpec.
    def enterOrderBySpec(self, ctx:MalloyParser.OrderBySpecContext):
        pass

    # Exit a parse tree produced by MalloyParser#orderBySpec.
    def exitOrderBySpec(self, ctx:MalloyParser.OrderBySpecContext):
        pass


    # Enter a parse tree produced by MalloyParser#limitStatement.
    def enterLimitStatement(self, ctx:MalloyParser.LimitStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#limitStatement.
    def exitLimitStatement(self, ctx:MalloyParser.LimitStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#bySpec.
    def enterBySpec(self, ctx:MalloyParser.BySpecContext):
        pass

    # Exit a parse tree produced by MalloyParser#bySpec.
    def exitBySpec(self, ctx:MalloyParser.BySpecContext):
        pass


    # Enter a parse tree produced by MalloyParser#topStatement.
    def enterTopStatement(self, ctx:MalloyParser.TopStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#topStatement.
    def exitTopStatement(self, ctx:MalloyParser.TopStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#indexElement.
    def enterIndexElement(self, ctx:MalloyParser.IndexElementContext):
        pass

    # Exit a parse tree produced by MalloyParser#indexElement.
    def exitIndexElement(self, ctx:MalloyParser.IndexElementContext):
        pass


    # Enter a parse tree produced by MalloyParser#indexFields.
    def enterIndexFields(self, ctx:MalloyParser.IndexFieldsContext):
        pass

    # Exit a parse tree produced by MalloyParser#indexFields.
    def exitIndexFields(self, ctx:MalloyParser.IndexFieldsContext):
        pass


    # Enter a parse tree produced by MalloyParser#indexStatement.
    def enterIndexStatement(self, ctx:MalloyParser.IndexStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#indexStatement.
    def exitIndexStatement(self, ctx:MalloyParser.IndexStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#sampleStatement.
    def enterSampleStatement(self, ctx:MalloyParser.SampleStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#sampleStatement.
    def exitSampleStatement(self, ctx:MalloyParser.SampleStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#timezoneStatement.
    def enterTimezoneStatement(self, ctx:MalloyParser.TimezoneStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#timezoneStatement.
    def exitTimezoneStatement(self, ctx:MalloyParser.TimezoneStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#queryAnnotation.
    def enterQueryAnnotation(self, ctx:MalloyParser.QueryAnnotationContext):
        pass

    # Exit a parse tree produced by MalloyParser#queryAnnotation.
    def exitQueryAnnotation(self, ctx:MalloyParser.QueryAnnotationContext):
        pass


    # Enter a parse tree produced by MalloyParser#sampleSpec.
    def enterSampleSpec(self, ctx:MalloyParser.SampleSpecContext):
        pass

    # Exit a parse tree produced by MalloyParser#sampleSpec.
    def exitSampleSpec(self, ctx:MalloyParser.SampleSpecContext):
        pass


    # Enter a parse tree produced by MalloyParser#aggregate.
    def enterAggregate(self, ctx:MalloyParser.AggregateContext):
        pass

    # Exit a parse tree produced by MalloyParser#aggregate.
    def exitAggregate(self, ctx:MalloyParser.AggregateContext):
        pass


    # Enter a parse tree produced by MalloyParser#malloyType.
    def enterMalloyType(self, ctx:MalloyParser.MalloyTypeContext):
        pass

    # Exit a parse tree produced by MalloyParser#malloyType.
    def exitMalloyType(self, ctx:MalloyParser.MalloyTypeContext):
        pass


    # Enter a parse tree produced by MalloyParser#compareOp.
    def enterCompareOp(self, ctx:MalloyParser.CompareOpContext):
        pass

    # Exit a parse tree produced by MalloyParser#compareOp.
    def exitCompareOp(self, ctx:MalloyParser.CompareOpContext):
        pass


    # Enter a parse tree produced by MalloyParser#string.
    def enterString(self, ctx:MalloyParser.StringContext):
        pass

    # Exit a parse tree produced by MalloyParser#string.
    def exitString(self, ctx:MalloyParser.StringContext):
        pass


    # Enter a parse tree produced by MalloyParser#shortString.
    def enterShortString(self, ctx:MalloyParser.ShortStringContext):
        pass

    # Exit a parse tree produced by MalloyParser#shortString.
    def exitShortString(self, ctx:MalloyParser.ShortStringContext):
        pass


    # Enter a parse tree produced by MalloyParser#rawString.
    def enterRawString(self, ctx:MalloyParser.RawStringContext):
        pass

    # Exit a parse tree produced by MalloyParser#rawString.
    def exitRawString(self, ctx:MalloyParser.RawStringContext):
        pass


    # Enter a parse tree produced by MalloyParser#numericLiteral.
    def enterNumericLiteral(self, ctx:MalloyParser.NumericLiteralContext):
        pass

    # Exit a parse tree produced by MalloyParser#numericLiteral.
    def exitNumericLiteral(self, ctx:MalloyParser.NumericLiteralContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprString.
    def enterExprString(self, ctx:MalloyParser.ExprStringContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprString.
    def exitExprString(self, ctx:MalloyParser.ExprStringContext):
        pass


    # Enter a parse tree produced by MalloyParser#stub_rawString.
    def enterStub_rawString(self, ctx:MalloyParser.Stub_rawStringContext):
        pass

    # Exit a parse tree produced by MalloyParser#stub_rawString.
    def exitStub_rawString(self, ctx:MalloyParser.Stub_rawStringContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprNumber.
    def enterExprNumber(self, ctx:MalloyParser.ExprNumberContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprNumber.
    def exitExprNumber(self, ctx:MalloyParser.ExprNumberContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprTime.
    def enterExprTime(self, ctx:MalloyParser.ExprTimeContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprTime.
    def exitExprTime(self, ctx:MalloyParser.ExprTimeContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprNULL.
    def enterExprNULL(self, ctx:MalloyParser.ExprNULLContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprNULL.
    def exitExprNULL(self, ctx:MalloyParser.ExprNULLContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprBool.
    def enterExprBool(self, ctx:MalloyParser.ExprBoolContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprBool.
    def exitExprBool(self, ctx:MalloyParser.ExprBoolContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprRegex.
    def enterExprRegex(self, ctx:MalloyParser.ExprRegexContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprRegex.
    def exitExprRegex(self, ctx:MalloyParser.ExprRegexContext):
        pass


    # Enter a parse tree produced by MalloyParser#filterString_stub.
    def enterFilterString_stub(self, ctx:MalloyParser.FilterString_stubContext):
        pass

    # Exit a parse tree produced by MalloyParser#filterString_stub.
    def exitFilterString_stub(self, ctx:MalloyParser.FilterString_stubContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprNow.
    def enterExprNow(self, ctx:MalloyParser.ExprNowContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprNow.
    def exitExprNow(self, ctx:MalloyParser.ExprNowContext):
        pass


    # Enter a parse tree produced by MalloyParser#literalTimestamp.
    def enterLiteralTimestamp(self, ctx:MalloyParser.LiteralTimestampContext):
        pass

    # Exit a parse tree produced by MalloyParser#literalTimestamp.
    def exitLiteralTimestamp(self, ctx:MalloyParser.LiteralTimestampContext):
        pass


    # Enter a parse tree produced by MalloyParser#literalHour.
    def enterLiteralHour(self, ctx:MalloyParser.LiteralHourContext):
        pass

    # Exit a parse tree produced by MalloyParser#literalHour.
    def exitLiteralHour(self, ctx:MalloyParser.LiteralHourContext):
        pass


    # Enter a parse tree produced by MalloyParser#literalDay.
    def enterLiteralDay(self, ctx:MalloyParser.LiteralDayContext):
        pass

    # Exit a parse tree produced by MalloyParser#literalDay.
    def exitLiteralDay(self, ctx:MalloyParser.LiteralDayContext):
        pass


    # Enter a parse tree produced by MalloyParser#literalWeek.
    def enterLiteralWeek(self, ctx:MalloyParser.LiteralWeekContext):
        pass

    # Exit a parse tree produced by MalloyParser#literalWeek.
    def exitLiteralWeek(self, ctx:MalloyParser.LiteralWeekContext):
        pass


    # Enter a parse tree produced by MalloyParser#literalMonth.
    def enterLiteralMonth(self, ctx:MalloyParser.LiteralMonthContext):
        pass

    # Exit a parse tree produced by MalloyParser#literalMonth.
    def exitLiteralMonth(self, ctx:MalloyParser.LiteralMonthContext):
        pass


    # Enter a parse tree produced by MalloyParser#literalQuarter.
    def enterLiteralQuarter(self, ctx:MalloyParser.LiteralQuarterContext):
        pass

    # Exit a parse tree produced by MalloyParser#literalQuarter.
    def exitLiteralQuarter(self, ctx:MalloyParser.LiteralQuarterContext):
        pass


    # Enter a parse tree produced by MalloyParser#literalYear.
    def enterLiteralYear(self, ctx:MalloyParser.LiteralYearContext):
        pass

    # Exit a parse tree produced by MalloyParser#literalYear.
    def exitLiteralYear(self, ctx:MalloyParser.LiteralYearContext):
        pass


    # Enter a parse tree produced by MalloyParser#tablePath.
    def enterTablePath(self, ctx:MalloyParser.TablePathContext):
        pass

    # Exit a parse tree produced by MalloyParser#tablePath.
    def exitTablePath(self, ctx:MalloyParser.TablePathContext):
        pass


    # Enter a parse tree produced by MalloyParser#tableURI.
    def enterTableURI(self, ctx:MalloyParser.TableURIContext):
        pass

    # Exit a parse tree produced by MalloyParser#tableURI.
    def exitTableURI(self, ctx:MalloyParser.TableURIContext):
        pass


    # Enter a parse tree produced by MalloyParser#id.
    def enterId(self, ctx:MalloyParser.IdContext):
        pass

    # Exit a parse tree produced by MalloyParser#id.
    def exitId(self, ctx:MalloyParser.IdContext):
        pass


    # Enter a parse tree produced by MalloyParser#timeframe.
    def enterTimeframe(self, ctx:MalloyParser.TimeframeContext):
        pass

    # Exit a parse tree produced by MalloyParser#timeframe.
    def exitTimeframe(self, ctx:MalloyParser.TimeframeContext):
        pass


    # Enter a parse tree produced by MalloyParser#ungroup.
    def enterUngroup(self, ctx:MalloyParser.UngroupContext):
        pass

    # Exit a parse tree produced by MalloyParser#ungroup.
    def exitUngroup(self, ctx:MalloyParser.UngroupContext):
        pass


    # Enter a parse tree produced by MalloyParser#malloyOrSQLType.
    def enterMalloyOrSQLType(self, ctx:MalloyParser.MalloyOrSQLTypeContext):
        pass

    # Exit a parse tree produced by MalloyParser#malloyOrSQLType.
    def exitMalloyOrSQLType(self, ctx:MalloyParser.MalloyOrSQLTypeContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprExpr.
    def enterExprExpr(self, ctx:MalloyParser.ExprExprContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprExpr.
    def exitExprExpr(self, ctx:MalloyParser.ExprExprContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprCase.
    def enterExprCase(self, ctx:MalloyParser.ExprCaseContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprCase.
    def exitExprCase(self, ctx:MalloyParser.ExprCaseContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprMinus.
    def enterExprMinus(self, ctx:MalloyParser.ExprMinusContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprMinus.
    def exitExprMinus(self, ctx:MalloyParser.ExprMinusContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprAddSub.
    def enterExprAddSub(self, ctx:MalloyParser.ExprAddSubContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprAddSub.
    def exitExprAddSub(self, ctx:MalloyParser.ExprAddSubContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprNullCheck.
    def enterExprNullCheck(self, ctx:MalloyParser.ExprNullCheckContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprNullCheck.
    def exitExprNullCheck(self, ctx:MalloyParser.ExprNullCheckContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprArrayLiteral.
    def enterExprArrayLiteral(self, ctx:MalloyParser.ExprArrayLiteralContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprArrayLiteral.
    def exitExprArrayLiteral(self, ctx:MalloyParser.ExprArrayLiteralContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprRange.
    def enterExprRange(self, ctx:MalloyParser.ExprRangeContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprRange.
    def exitExprRange(self, ctx:MalloyParser.ExprRangeContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprLogicalOr.
    def enterExprLogicalOr(self, ctx:MalloyParser.ExprLogicalOrContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprLogicalOr.
    def exitExprLogicalOr(self, ctx:MalloyParser.ExprLogicalOrContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprLiteral.
    def enterExprLiteral(self, ctx:MalloyParser.ExprLiteralContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprLiteral.
    def exitExprLiteral(self, ctx:MalloyParser.ExprLiteralContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprCompare.
    def enterExprCompare(self, ctx:MalloyParser.ExprCompareContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprCompare.
    def exitExprCompare(self, ctx:MalloyParser.ExprCompareContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprForRange.
    def enterExprForRange(self, ctx:MalloyParser.ExprForRangeContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprForRange.
    def exitExprForRange(self, ctx:MalloyParser.ExprForRangeContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprFunc.
    def enterExprFunc(self, ctx:MalloyParser.ExprFuncContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprFunc.
    def exitExprFunc(self, ctx:MalloyParser.ExprFuncContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprAndTree.
    def enterExprAndTree(self, ctx:MalloyParser.ExprAndTreeContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprAndTree.
    def exitExprAndTree(self, ctx:MalloyParser.ExprAndTreeContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprCast.
    def enterExprCast(self, ctx:MalloyParser.ExprCastContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprCast.
    def exitExprCast(self, ctx:MalloyParser.ExprCastContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprAggFunc.
    def enterExprAggFunc(self, ctx:MalloyParser.ExprAggFuncContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprAggFunc.
    def exitExprAggFunc(self, ctx:MalloyParser.ExprAggFuncContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprTimeTrunc.
    def enterExprTimeTrunc(self, ctx:MalloyParser.ExprTimeTruncContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprTimeTrunc.
    def exitExprTimeTrunc(self, ctx:MalloyParser.ExprTimeTruncContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprAggregate.
    def enterExprAggregate(self, ctx:MalloyParser.ExprAggregateContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprAggregate.
    def exitExprAggregate(self, ctx:MalloyParser.ExprAggregateContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprLogicalAnd.
    def enterExprLogicalAnd(self, ctx:MalloyParser.ExprLogicalAndContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprLogicalAnd.
    def exitExprLogicalAnd(self, ctx:MalloyParser.ExprLogicalAndContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprFieldPath.
    def enterExprFieldPath(self, ctx:MalloyParser.ExprFieldPathContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprFieldPath.
    def exitExprFieldPath(self, ctx:MalloyParser.ExprFieldPathContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprMulDiv.
    def enterExprMulDiv(self, ctx:MalloyParser.ExprMulDivContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprMulDiv.
    def exitExprMulDiv(self, ctx:MalloyParser.ExprMulDivContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprSafeCast.
    def enterExprSafeCast(self, ctx:MalloyParser.ExprSafeCastContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprSafeCast.
    def exitExprSafeCast(self, ctx:MalloyParser.ExprSafeCastContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprOrTree.
    def enterExprOrTree(self, ctx:MalloyParser.ExprOrTreeContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprOrTree.
    def exitExprOrTree(self, ctx:MalloyParser.ExprOrTreeContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprNot.
    def enterExprNot(self, ctx:MalloyParser.ExprNotContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprNot.
    def exitExprNot(self, ctx:MalloyParser.ExprNotContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprDuration.
    def enterExprDuration(self, ctx:MalloyParser.ExprDurationContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprDuration.
    def exitExprDuration(self, ctx:MalloyParser.ExprDurationContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprApply.
    def enterExprApply(self, ctx:MalloyParser.ExprApplyContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprApply.
    def exitExprApply(self, ctx:MalloyParser.ExprApplyContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprWarnLike.
    def enterExprWarnLike(self, ctx:MalloyParser.ExprWarnLikeContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprWarnLike.
    def exitExprWarnLike(self, ctx:MalloyParser.ExprWarnLikeContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprFieldProps.
    def enterExprFieldProps(self, ctx:MalloyParser.ExprFieldPropsContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprFieldProps.
    def exitExprFieldProps(self, ctx:MalloyParser.ExprFieldPropsContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprCoalesce.
    def enterExprCoalesce(self, ctx:MalloyParser.ExprCoalesceContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprCoalesce.
    def exitExprCoalesce(self, ctx:MalloyParser.ExprCoalesceContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprUngroup.
    def enterExprUngroup(self, ctx:MalloyParser.ExprUngroupContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprUngroup.
    def exitExprUngroup(self, ctx:MalloyParser.ExprUngroupContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprLiteralRecord.
    def enterExprLiteralRecord(self, ctx:MalloyParser.ExprLiteralRecordContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprLiteralRecord.
    def exitExprLiteralRecord(self, ctx:MalloyParser.ExprLiteralRecordContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprWarnIn.
    def enterExprWarnIn(self, ctx:MalloyParser.ExprWarnInContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprWarnIn.
    def exitExprWarnIn(self, ctx:MalloyParser.ExprWarnInContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprPick.
    def enterExprPick(self, ctx:MalloyParser.ExprPickContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprPick.
    def exitExprPick(self, ctx:MalloyParser.ExprPickContext):
        pass


    # Enter a parse tree produced by MalloyParser#exprPathlessAggregate.
    def enterExprPathlessAggregate(self, ctx:MalloyParser.ExprPathlessAggregateContext):
        pass

    # Exit a parse tree produced by MalloyParser#exprPathlessAggregate.
    def exitExprPathlessAggregate(self, ctx:MalloyParser.ExprPathlessAggregateContext):
        pass


    # Enter a parse tree produced by MalloyParser#partialCompare.
    def enterPartialCompare(self, ctx:MalloyParser.PartialCompareContext):
        pass

    # Exit a parse tree produced by MalloyParser#partialCompare.
    def exitPartialCompare(self, ctx:MalloyParser.PartialCompareContext):
        pass


    # Enter a parse tree produced by MalloyParser#partialTest.
    def enterPartialTest(self, ctx:MalloyParser.PartialTestContext):
        pass

    # Exit a parse tree produced by MalloyParser#partialTest.
    def exitPartialTest(self, ctx:MalloyParser.PartialTestContext):
        pass


    # Enter a parse tree produced by MalloyParser#partialAllowedFieldExpr.
    def enterPartialAllowedFieldExpr(self, ctx:MalloyParser.PartialAllowedFieldExprContext):
        pass

    # Exit a parse tree produced by MalloyParser#partialAllowedFieldExpr.
    def exitPartialAllowedFieldExpr(self, ctx:MalloyParser.PartialAllowedFieldExprContext):
        pass


    # Enter a parse tree produced by MalloyParser#fieldExprList.
    def enterFieldExprList(self, ctx:MalloyParser.FieldExprListContext):
        pass

    # Exit a parse tree produced by MalloyParser#fieldExprList.
    def exitFieldExprList(self, ctx:MalloyParser.FieldExprListContext):
        pass


    # Enter a parse tree produced by MalloyParser#pickStatement.
    def enterPickStatement(self, ctx:MalloyParser.PickStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#pickStatement.
    def exitPickStatement(self, ctx:MalloyParser.PickStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#pick.
    def enterPick(self, ctx:MalloyParser.PickContext):
        pass

    # Exit a parse tree produced by MalloyParser#pick.
    def exitPick(self, ctx:MalloyParser.PickContext):
        pass


    # Enter a parse tree produced by MalloyParser#caseStatement.
    def enterCaseStatement(self, ctx:MalloyParser.CaseStatementContext):
        pass

    # Exit a parse tree produced by MalloyParser#caseStatement.
    def exitCaseStatement(self, ctx:MalloyParser.CaseStatementContext):
        pass


    # Enter a parse tree produced by MalloyParser#caseWhen.
    def enterCaseWhen(self, ctx:MalloyParser.CaseWhenContext):
        pass

    # Exit a parse tree produced by MalloyParser#caseWhen.
    def exitCaseWhen(self, ctx:MalloyParser.CaseWhenContext):
        pass


    # Enter a parse tree produced by MalloyParser#recordKey.
    def enterRecordKey(self, ctx:MalloyParser.RecordKeyContext):
        pass

    # Exit a parse tree produced by MalloyParser#recordKey.
    def exitRecordKey(self, ctx:MalloyParser.RecordKeyContext):
        pass


    # Enter a parse tree produced by MalloyParser#recordRef.
    def enterRecordRef(self, ctx:MalloyParser.RecordRefContext):
        pass

    # Exit a parse tree produced by MalloyParser#recordRef.
    def exitRecordRef(self, ctx:MalloyParser.RecordRefContext):
        pass


    # Enter a parse tree produced by MalloyParser#recordExpr.
    def enterRecordExpr(self, ctx:MalloyParser.RecordExprContext):
        pass

    # Exit a parse tree produced by MalloyParser#recordExpr.
    def exitRecordExpr(self, ctx:MalloyParser.RecordExprContext):
        pass


    # Enter a parse tree produced by MalloyParser#argumentList.
    def enterArgumentList(self, ctx:MalloyParser.ArgumentListContext):
        pass

    # Exit a parse tree produced by MalloyParser#argumentList.
    def exitArgumentList(self, ctx:MalloyParser.ArgumentListContext):
        pass


    # Enter a parse tree produced by MalloyParser#fieldNameList.
    def enterFieldNameList(self, ctx:MalloyParser.FieldNameListContext):
        pass

    # Exit a parse tree produced by MalloyParser#fieldNameList.
    def exitFieldNameList(self, ctx:MalloyParser.FieldNameListContext):
        pass


    # Enter a parse tree produced by MalloyParser#fieldCollection.
    def enterFieldCollection(self, ctx:MalloyParser.FieldCollectionContext):
        pass

    # Exit a parse tree produced by MalloyParser#fieldCollection.
    def exitFieldCollection(self, ctx:MalloyParser.FieldCollectionContext):
        pass


    # Enter a parse tree produced by MalloyParser#collectionWildCard.
    def enterCollectionWildCard(self, ctx:MalloyParser.CollectionWildCardContext):
        pass

    # Exit a parse tree produced by MalloyParser#collectionWildCard.
    def exitCollectionWildCard(self, ctx:MalloyParser.CollectionWildCardContext):
        pass


    # Enter a parse tree produced by MalloyParser#starQualified.
    def enterStarQualified(self, ctx:MalloyParser.StarQualifiedContext):
        pass

    # Exit a parse tree produced by MalloyParser#starQualified.
    def exitStarQualified(self, ctx:MalloyParser.StarQualifiedContext):
        pass


    # Enter a parse tree produced by MalloyParser#taggedRef.
    def enterTaggedRef(self, ctx:MalloyParser.TaggedRefContext):
        pass

    # Exit a parse tree produced by MalloyParser#taggedRef.
    def exitTaggedRef(self, ctx:MalloyParser.TaggedRefContext):
        pass


    # Enter a parse tree produced by MalloyParser#refExpr.
    def enterRefExpr(self, ctx:MalloyParser.RefExprContext):
        pass

    # Exit a parse tree produced by MalloyParser#refExpr.
    def exitRefExpr(self, ctx:MalloyParser.RefExprContext):
        pass


    # Enter a parse tree produced by MalloyParser#collectionMember.
    def enterCollectionMember(self, ctx:MalloyParser.CollectionMemberContext):
        pass

    # Exit a parse tree produced by MalloyParser#collectionMember.
    def exitCollectionMember(self, ctx:MalloyParser.CollectionMemberContext):
        pass


    # Enter a parse tree produced by MalloyParser#fieldPath.
    def enterFieldPath(self, ctx:MalloyParser.FieldPathContext):
        pass

    # Exit a parse tree produced by MalloyParser#fieldPath.
    def exitFieldPath(self, ctx:MalloyParser.FieldPathContext):
        pass


    # Enter a parse tree produced by MalloyParser#joinName.
    def enterJoinName(self, ctx:MalloyParser.JoinNameContext):
        pass

    # Exit a parse tree produced by MalloyParser#joinName.
    def exitJoinName(self, ctx:MalloyParser.JoinNameContext):
        pass


    # Enter a parse tree produced by MalloyParser#fieldName.
    def enterFieldName(self, ctx:MalloyParser.FieldNameContext):
        pass

    # Exit a parse tree produced by MalloyParser#fieldName.
    def exitFieldName(self, ctx:MalloyParser.FieldNameContext):
        pass


    # Enter a parse tree produced by MalloyParser#sqlExploreNameRef.
    def enterSqlExploreNameRef(self, ctx:MalloyParser.SqlExploreNameRefContext):
        pass

    # Exit a parse tree produced by MalloyParser#sqlExploreNameRef.
    def exitSqlExploreNameRef(self, ctx:MalloyParser.SqlExploreNameRefContext):
        pass


    # Enter a parse tree produced by MalloyParser#nameSQLBlock.
    def enterNameSQLBlock(self, ctx:MalloyParser.NameSQLBlockContext):
        pass

    # Exit a parse tree produced by MalloyParser#nameSQLBlock.
    def exitNameSQLBlock(self, ctx:MalloyParser.NameSQLBlockContext):
        pass


    # Enter a parse tree produced by MalloyParser#connectionName.
    def enterConnectionName(self, ctx:MalloyParser.ConnectionNameContext):
        pass

    # Exit a parse tree produced by MalloyParser#connectionName.
    def exitConnectionName(self, ctx:MalloyParser.ConnectionNameContext):
        pass


    # Enter a parse tree produced by MalloyParser#tripFilterString.
    def enterTripFilterString(self, ctx:MalloyParser.TripFilterStringContext):
        pass

    # Exit a parse tree produced by MalloyParser#tripFilterString.
    def exitTripFilterString(self, ctx:MalloyParser.TripFilterStringContext):
        pass


    # Enter a parse tree produced by MalloyParser#tickFilterString.
    def enterTickFilterString(self, ctx:MalloyParser.TickFilterStringContext):
        pass

    # Exit a parse tree produced by MalloyParser#tickFilterString.
    def exitTickFilterString(self, ctx:MalloyParser.TickFilterStringContext):
        pass


    # Enter a parse tree produced by MalloyParser#filterString.
    def enterFilterString(self, ctx:MalloyParser.FilterStringContext):
        pass

    # Exit a parse tree produced by MalloyParser#filterString.
    def exitFilterString(self, ctx:MalloyParser.FilterStringContext):
        pass


    # Enter a parse tree produced by MalloyParser#debugExpr.
    def enterDebugExpr(self, ctx:MalloyParser.DebugExprContext):
        pass

    # Exit a parse tree produced by MalloyParser#debugExpr.
    def exitDebugExpr(self, ctx:MalloyParser.DebugExprContext):
        pass


    # Enter a parse tree produced by MalloyParser#debugPartial.
    def enterDebugPartial(self, ctx:MalloyParser.DebugPartialContext):
        pass

    # Exit a parse tree produced by MalloyParser#debugPartial.
    def exitDebugPartial(self, ctx:MalloyParser.DebugPartialContext):
        pass


    # Enter a parse tree produced by MalloyParser#experimentalStatementForTesting.
    def enterExperimentalStatementForTesting(self, ctx:MalloyParser.ExperimentalStatementForTestingContext):
        pass

    # Exit a parse tree produced by MalloyParser#experimentalStatementForTesting.
    def exitExperimentalStatementForTesting(self, ctx:MalloyParser.ExperimentalStatementForTestingContext):
        pass



del MalloyParser