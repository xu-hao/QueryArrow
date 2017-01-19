{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleContexts, ExistentialQuantification, FlexibleInstances, OverloadedStrings #-}
module QueryArrow.Parser (progp, rulesp, processActions) where

import QueryArrow.FO.Data
import QueryArrow.DB.DB
import QueryArrow.Rewriting

import Prelude hiding (lookup)
import Text.ParserCombinators.Parsec hiding (State)
import Data.Maybe
import Data.Either (either)
import Control.Applicative ((<$>), (<*>), (<*), (*>))
import qualified Text.Parsec.Token as T
import qualified Data.Text as TE
import Data.Namespace.Path
import Data.Namespace.Namespace
import Data.Tree
import Debug.Trace
import Data.Map.Strict (lookup)
import Data.List (partition)
import Control.Monad.State
import Data.Monoid ((<>))

lexer = T.makeTokenParser T.LanguageDef {
    T.commentStart = "/*",
    T.commentEnd = "*/",
    T.commentLine = "//",
    T.nestedComments = False,
    T.identStart = letter <|> char '_',
    T.identLetter = alphaNum <|> char '_',
    T.opStart = oneOf "=~|⊗⊕∧∨∀∃¬⟶𝟏𝟎⊤⊥",
    T.opLetter = oneOf "=~|⊗⊕∧∨∀∃¬⟶𝟏𝟎⊤⊥",
    T.reservedNames = ["commit", "insert", "return", "delete", "key", "object", "property", "input", "output", "rewrite", "predicate", "exists", "import", "export", "qualified", "all", "from", "except", "if", "then", "else", "one", "zero", "max", "min", "sum", "average", "count", "limit", "group", "order", "by", "asc", "desc", "let", "distinct", "integer", "text", "ref", "null"],
    T.reservedOpNames = ["=", "~", "|", "||", "⊗", "⊕", "‖", "∃", "¬", "⟶","𝟏","𝟎"],
    T.caseSensitive = True
}

identifier = T.identifier lexer
reserved = T.reserved lexer
reservedOp = T.reservedOp lexer

parens :: FOParser a -> FOParser a
parens = T.parens lexer

brackets :: FOParser a -> FOParser a
brackets = T.brackets lexer

braces :: FOParser a -> FOParser a
braces = T.braces lexer

integer = T.integer lexer
stringp = T.stringLiteral lexer
comma = T.comma lexer
semi = T.semi lexer
symbol = T.symbol lexer
whiteSpace = T.whiteSpace lexer
dot = T.dot lexer

-- parser
type FOParser = GenParser Char ()

casttypep :: FOParser CastType
casttypep = (reserved "text" >> return TextType) <|> (reserved "integer" >> return NumberType) <|> (reserved "ref" >> RefType <$> identifier)

argp :: FOParser Expr
argp =
    (reserved "null" >> return NullExpr)
    <|> (CastExpr <$> casttypep <*> argp)
    <|> (VarExpr <$> Var <$> identifier)
    <|> (IntExpr . fromIntegral <$> integer)
    <|> (reserved "pattern" >> PatternExpr. TE.pack <$> stringp )
    <|> (StringExpr . TE.pack <$> stringp)

arglistp :: FOParser [Expr]
arglistp =
    parens (sepBy argp comma)

atomp :: FOParser Atom
atomp = do
    predname <- prednamep
    arglist <- arglistp
    -- let thepred = fromMaybe (error ("atomp: undefined predicate " ++ show predname ++ ", available " ++ show2 workspace)) (lookupObject predname workspace)
    return (Atom predname arglist)

negp :: FOParser ()
negp = reservedOp "~" <|> reservedOp "¬"

existsp :: FOParser ()
existsp = reserved "exists" <|> reservedOp "∃"

timesp :: FOParser ()
timesp = optional (reservedOp "⊗")

plusp :: FOParser ()
plusp = reservedOp "|" <|> reservedOp "⊕"

parp :: FOParser ()
parp = reservedOp "||" <|> reservedOp "‖"

onep :: FOParser ()
onep = reserved "one" <|> reservedOp "𝟏"

zerop :: FOParser ()
zerop  = reserved "zero" <|> reservedOp "𝟎"

rarrowp :: FOParser ()
rarrowp = optional (reservedOp "⟶")

litp :: FOParser Lit
litp = Lit <$> (negp *> return Neg <|> return Pos) <*> atomp

litsp :: FOParser [Lit]
litsp = many1 litp

neg :: Lit -> Lit
neg (Lit Pos a) = Lit Neg a
neg (Lit Neg a) = Lit Pos a

returnp :: FOParser [Var]
returnp = reserved "return" *> varsp

letp :: FOParser (Var, Summary)
letp = do
    v <- varp
    reservedOp "="
    s <- (reserved "max" >> Max <$> varp)
      <|> (reserved "min" >> Min <$> varp)
      <|> (reserved "sum" >> Max <$> varp)
      <|> (reserved "average" >> Min <$> varp)
      <|> (reserved "count" >> (try (reserved "distinct" >> CountDistinct <$> varp) <|> return Count))
    return (v,s)

formula1p :: FOParser Formula
formula1p = try (parens formulap)
       <|> (FAtomic <$> try atomp)
       <|> (Aggregate Not <$> (negp >> formula1p))
       <|> (Aggregate Exists <$> (existsp >> formula1p))
       <|> (Aggregate <$> (reserved "distinct" >> return Distinct) <*> formula1p)
       <|> (Aggregate <$> (Summarize <$> (reserved "let" >> sepBy1 letp comma) <*> ((reserved "group" >> reserved "by" >> many varp) <|> pure [])) <*> formula1p)
       <|> (Aggregate <$> (reserved "limit" >> Limit . fromIntegral <$> integer) <*> formula1p)
       <|> (Aggregate <$> (reserved "order" >> reserved "by" >> (try (OrderByAsc <$> varp <* reserved "asc") <|> (OrderByDesc <$> varp <* reserved "desc"))) <*> formula1p)
       <|> onep *> pure FOne
       <|> zerop *> pure FZero
       <|> (fsequencing . map FInsert) <$> (reserved "insert" >> litsp)
       <|> (fsequencing . map FInsert) <$> (reserved "delete" >> fmap (map neg) litsp)

formulap :: FOParser Formula
formulap = do
  formula1s <- sepBy1 formulaChoicep parp
  let formula1 = fpar formula1s
  (Aggregate . FReturn <$> returnp <*> pure formula1)
    <|> return formula1

formulaChoicep :: FOParser Formula
formulaChoicep = do
    formulaConjs <- sepBy1 formulaSequencingp plusp
    return (fchoice formulaConjs)


formulaSequencingp :: FOParser Formula
formulaSequencingp = do
  formulaConjs <- sepBy1 formula1p timesp
  return (fsequencing formulaConjs)

paramtypep :: FOParser ParamType
paramtypep = ParamType <$> ((reserved "key" >> return True) <|> return False) <*> ((reserved "input" >> return True) <|> return False) <*> ((reserved "output" >> return True) <|> return False) <*> casttypep

predtypep :: FOParser PredType
predtypep = PredType <$> (reserved "object" *> return ObjectPred <|> reserved "property" *> return PropertyPred) <*>
    parens (sepBy paramtypep comma)

prednamep :: FOParser PredName
prednamep = do
    name <- identifier
    (do
        _ <- dot
        name2 <- identifier
        return (QPredName name [] name2))
        <|> return (UQPredName name)

predp :: FOParser Action
predp = do
    reserved "predicate"
    name <- prednamep
    t <- predtypep
    let thepred = Pred name t
    return (PredDef thepred)

varp :: FOParser Var
varp = Var <$> identifier

varsp :: FOParser [Var]
varsp = many1 varp

progp :: FOParser [Command]
progp = do
    commands <- many (do
      whiteSpace
      (reserved "begin" *> return Begin)
          <|> (reserved "prepare" *> return Prepare)
          <|> (reserved "commit" *> return Commit)
          <|> (reserved "rollback" *> return Rollback)
          <|> (Execute <$> formulap))
    eof
    return commands


rulep :: FOParser Action
rulep = (do
    reserved "rewrite"
    reserved "insert" *> irulep
      <|> reserved "delete" *> drulep
      <|> qrule2p) where
          qrule2p = do
              r <- InsertRewritingRule <$> atomp <* rarrowp  <*> formulap
              return (Rule ([r], [], []))
          irulep = do
              r <- InsertRewritingRule <$> atomp <* rarrowp  <*> formulap
              return (Rule ([], [r], []))
          drulep = do
              r <- InsertRewritingRule <$> atomp <* rarrowp  <*> formulap
              return (Rule ([], [], [r]))

namespacepathp :: FOParser (NamespacePath String)
namespacepathp = do
  ids <- many identifier
  return (foldl extendNamespacePath mempty ids)

show2 :: PredMap -> String
show2 workspace =
  let show3 (k,a) = (case k of
                          Nothing -> ""
                          Just k -> k) ++ (case a of
                                    Nothing -> ""
                                    Just a -> ":" ++ show a)
  in
      drawTree (fmap show3 (toTree workspace))

importp :: FOParser Action
importp = do
    reserved "import"
    (do
        reserved "qualified"
        (do
            reserved "all"
            reserved "from"
            ns <- namespacepathp
            (do
                reserved "except"
                preds <- many1 identifier
                return (ImportQualifiedExceptFrom ns preds)
                ) <|> (
                return (ImportQualifiedAllFrom ns)
                )
            ) <|> (do
            prednames <- many1 identifier
            reserved "from"
            ns <- namespacepathp
            return (ImportQualifiedFrom ns prednames)
            )
        ) <|> (do
            reserved "all"
            reserved "from"
            ns <- namespacepathp
            (do
                reserved "except"
                preds <- many1 identifier
                return (ImportExceptFrom ns preds)
                ) <|> (
                return (ImportAllFrom ns)
                )
            ) <|> (do
            prednames <- many1 identifier
            reserved "from"
            ns <- namespacepathp
            return (ImportFrom ns prednames)
            )


data Action =
  ExportQualifiedExceptFrom (NamespacePath String) [String] |
  ExportQualifiedAllFrom (NamespacePath String) |
  ExportQualifiedFrom (NamespacePath String) [String] |
  ExportExceptFrom (NamespacePath String) [String] |
  ExportAllFrom (NamespacePath String) |
  ExportFrom (NamespacePath String) [String] |
  Export [String] |
  ImportQualifiedExceptFrom (NamespacePath String) [String] |
  ImportQualifiedAllFrom (NamespacePath String) |
  ImportQualifiedFrom (NamespacePath String) [String] |
  ImportExceptFrom (NamespacePath String) [String] |
  ImportAllFrom (NamespacePath String) |
  ImportFrom (NamespacePath String) [String] |
  PredDef Pred |
  Rule ([InsertRewritingRule], [InsertRewritingRule], [InsertRewritingRule])

isExport :: Action -> Bool
isExport (ExportQualifiedExceptFrom _ _) = True
isExport (ExportQualifiedAllFrom _) = True
isExport (ExportQualifiedFrom _ _) = True
isExport (ExportExceptFrom _ _) = True
isExport (ExportAllFrom _) = True
isExport (ExportFrom _ _) = True
isExport (Export _) = True
isExport _ = False

sortImportExport :: [Action] -> [Action]
sortImportExport acts =
  let (other, export) = partition (not . isExport) acts in
      other ++ export

execAction :: Action -> State (PredMap, PredMap, PredMap, ([InsertRewritingRule], [InsertRewritingRule], [InsertRewritingRule])) ()
execAction (ExportQualifiedExceptFrom ns prednames) = do
    (predmap, workspace, exports, r) <- get
    let exports' = importQualifiedExceptFromNamespaceE ns prednames predmap exports
    put (predmap, workspace, either (\e->error ("export1: " ++ e)) id exports',r)
execAction (ExportQualifiedAllFrom ns) = do
    (predmap, workspace, exports, r) <- get
    let exports' = importQualifiedAllFromNamespaceE ns predmap exports
    put (predmap, workspace, either (\e->error ("export2: " ++ e)) id exports',r)
execAction (ExportQualifiedFrom ns prednames) = do
    (predmap, workspace, exports,r) <- get
    let exports' = importQualifiedFromNamespaceE ns prednames predmap exports
    put (predmap, workspace, either (\e->error ("export3: " ++ e)) id exports',r)
execAction (ExportExceptFrom ns prednames) = do
    (predmap, workspace, exports,r) <- get
    let exports' = importExceptFromNamespaceE ns prednames predmap exports
    put (predmap, workspace, either (\e->error ("export4: " ++ e)) id exports',r)
execAction (ExportAllFrom ns) = do
    (predmap, workspace, exports,r) <- get
    let exports' = importAllFromNamespaceE ns predmap exports
    put (predmap, workspace, either (\e->error ("export5: " ++ e)) id exports',r)
execAction (ExportFrom ns prednames) = do
    (predmap, workspace, exports,r) <- get
    let exports' = importFromNamespaceE ns prednames predmap exports
    put (predmap, workspace, either (\e->error ("export6: " ++ e)) id exports',r)
execAction (Export prednames) = do
    (predmap, workspace, exports,r) <- get
    let exports' = importFromNamespaceE mempty prednames workspace exports
    put (predmap, workspace, either (\e->error ("execAction: Export: cannot export " ++ show prednames ++ ": " ++ e)) id exports',r)
execAction (ImportQualifiedExceptFrom ns prednames) = do
    (predmap, workspace, exports,r) <- get
    let workspace' = importQualifiedExceptFromNamespaceE ns prednames predmap workspace
    put (predmap, either (\e->error ("import1: " ++ e)) id workspace', exports,r)
execAction (ImportQualifiedAllFrom ns) = do
    (predmap, workspace, exports,r) <- get
    let workspace' = importAllFromNamespaceE ns predmap workspace
    put (predmap, either (\e->error ("import2: " ++ e)) id workspace', exports,r)
execAction (ImportQualifiedFrom ns prednames) = do
    (predmap, workspace, exports,r) <- get
    let workspace' = importQualifiedFromNamespaceE ns prednames predmap workspace
    put (predmap, either (\e->error ("import3: " ++ e)) id workspace', exports,r)
execAction (ImportExceptFrom ns prednames) = do
    (predmap, workspace, exports,r) <- get
    let workspace' = importExceptFromNamespaceE ns prednames predmap workspace
    put (predmap, either (\e->error ("import4: " ++ e)) id workspace', exports,r)
execAction (ImportAllFrom ns) = do
    (predmap, workspace, exports,r) <- get
    let workspace' = importAllFromNamespaceE ns predmap workspace
    put (predmap, either (\e->error ("import5: " ++ e)) id workspace', exports,r)
execAction (ImportFrom ns prednames) = do
    (predmap, workspace, exports,r) <- get
    let workspace' = importFromNamespaceE ns prednames predmap workspace
    put (predmap, either (\e->error ("import6: " ++ e)) id workspace', exports,r)
execAction (PredDef thepred@(Pred predname _)) =
    modify (\(predmap, predmap2, exports,r) -> (predmap, case lookupObject predname predmap2 of
                                                              Nothing -> insertObject predname thepred predmap2
                                                              _ -> error ("pred def: redefinition of predicate " ++ show predname), exports,r))
execAction (Rule r) =
    modify (\(predmap, predmap2, exports,rs) -> (predmap, predmap2, exports,rs<>r))


exportp :: FOParser Action
exportp = do
    reserved "export"
    (do
        reserved "qualified"
        (do
            reserved "all"
            reserved "from"
            ns <- namespacepathp
            (do
                reserved "except"
                preds <- many1 identifier
                return (ExportQualifiedExceptFrom ns preds)
                ) <|> (
                return (ExportQualifiedAllFrom ns)
                )
            ) <|> (do
            prednames <- many1 identifier
            reserved "from"
            ns <- namespacepathp
            return (ExportQualifiedFrom ns prednames)
            )
        ) <|> (do
        reserved "all"
        reserved "from"
        ns <- namespacepathp
        (do
            reserved "except"
            preds <- many1 identifier
            return (ExportExceptFrom ns preds)
            ) <|> (
            return (ExportAllFrom ns)
            )
        ) <|> (do
            prednames <- many1 identifier
            (do
                reserved "from"
                ns <- namespacepathp
                return (ExportFrom ns prednames)
                ) <|> (
                return (Export prednames)
                )
            )


rulesp :: FOParser [Action]
rulesp = do
    whiteSpace
    actions <- many ((importp <|> exportp <|> predp <|> rulep) <* whiteSpace)
    eof
    return actions

processActions :: PredMap -> [Action] -> (([InsertRewritingRule], [InsertRewritingRule], [InsertRewritingRule]), PredMap, PredMap)
processActions predmap actions =
    let (_, workspace, exports, rules) = execState (mapM_ execAction (sortImportExport actions)) (predmap, mempty, mempty, mempty)
    in (rules, workspace, exports)

samePredAndKey :: PredTypeMap -> Atom -> Atom -> Bool
samePredAndKey ptm (Atom p1 args1) (Atom p2 args2) | p1 == p2 =
    let predtype = fromMaybe (error ("samePredAndKey: cannot find predicate " ++ show p1)) (lookup p1 ptm) in
        keyComponents predtype args1 == keyComponents predtype args2
samePredAndKey _ _ _ = False

unionPred :: PredTypeMap -> [Atom] -> [Atom] -> [Atom]
unionPred ptm as1 as2 =
    as1 ++ filter (not . samePredAndKeys ptm as1) as2 where
        samePredAndKeys ptm as1 atom = any (samePredAndKey ptm atom) as1
