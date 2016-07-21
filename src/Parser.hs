{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleContexts, ExistentialQuantification, FlexibleInstances, OverloadedStrings #-}
module Parser (progp, rulesp) where

import FO.Data
import QueryPlan
import Rewriting

import Prelude hiding (lookup)
import Data.Map.Strict (Map, (!), member, insert, lookup, fromList, keys)
import Text.ParserCombinators.Parsec hiding (State)
import Data.Maybe
import Control.Applicative ((<$>), (<*>), (<*), (*>))
import Data.List (union, (\\), intercalate)
import qualified Text.Parsec.Token as T
import qualified Data.Text as TE
import Data.Namespace.Path
import Data.Namespace.Namespace

lexer = T.makeTokenParser T.LanguageDef {
    T.commentStart = "/*",
    T.commentEnd = "*/",
    T.commentLine = "//",
    T.nestedComments = False,
    T.identStart = letter <|> char '_',
    T.identLetter = alphaNum <|> char '_',
    T.opStart = oneOf "=~|⊗⊕∧∨∀∃¬⟶𝟏𝟎⊤⊥",
    T.opLetter = oneOf "=~|⊗⊕∧∨∀∃¬⟶𝟏𝟎⊤⊥",
    T.reservedNames = ["commit", "insert", "return", "delete", "key", "object", "property", "rewrite", "predicate", "exists", "import", "export", "transactional", "qualified", "all", "from", "except", "if", "then", "else", "one", "zero", "max", "min", "count", "limit", "order", "by", "asc", "desc", "let"],
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
type FOParser = GenParser Char (PredMap, PredMap, PredMap)

argp :: FOParser Expr
argp =
    (VarExpr <$> Var <$> identifier)
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
    (_, predmap, _) <- getState
    let thepred = fromMaybe (error ("atomp: undefined predicate " ++ show predname ++ ", available " ++ show predmap)) (lookupObject predname predmap)
    return (Atom thepred arglist)

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

transactionp :: FOParser ()
transactionp = reserved "transactional"

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
      <|> (reserved "count" >> return Count)
    return (v,s)

formula1p :: FOParser Formula
formula1p = try (parens formulap)
       <|> (FReturn <$> returnp)
       <|> (FAtomic <$> try atomp)
       <|> transactionp *> pure FTransaction
       <|> (Aggregate Not <$> (negp >> formula1p))
       <|> (Aggregate Exists <$> (existsp >> formula1p))
       <|> (Aggregate <$> (Summarize <$> (reserved "let" >> sepBy letp comma)) <*> formula1p)
       <|> (Aggregate <$> (reserved "limit" >> Limit . fromIntegral <$> integer) <*> formula1p)
       <|> (Aggregate <$> (reserved "order" >> reserved "by" >> (try (OrderByAsc <$> varp <* reserved "asc") <|> (OrderByDesc <$> varp <* reserved "desc"))) <*> formula1p)
       <|> onep *> pure FOne
       <|> zerop *> pure FZero
       <|> (fsequencing . map FInsert) <$> (reserved "insert" >> litsp)
       <|> (fsequencing . map FInsert) <$> (reserved "delete" >> fmap (map neg) litsp)

formulap :: FOParser Formula
formulap = do
  formula1s <- sepBy formulaChoicep parp
  return (fpar formula1s)

formulaChoicep :: FOParser Formula
formulaChoicep = do
    formulaConjs <- sepBy formulaSequencingp plusp
    return (fchoice formulaConjs)


formulaSequencingp :: FOParser Formula
formulaSequencingp = do
  formulaConjs <- sepBy formula1p timesp
  return (fsequencing formulaConjs)

paramtypep :: FOParser ParamType
paramtypep = ((reserved "key" >> return Key) <|> (reserved "property" >> return Property)) <*> identifier

predtypep :: FOParser PredType
predtypep = PredType <$> (reserved "object" *> return ObjectPred <|> reserved "property" *> return PropertyPred) <*>
    parens (sepBy paramtypep comma)

prednamep :: FOParser PredName
prednamep = do
    name <- identifier
    (do
        dot
        name2 <- identifier
        return (QPredName name [] name2))
        <|> return (UQPredName name)

predp :: FOParser ()
predp = do
    reserved "predicate"
    name <- prednamep
    t <- predtypep
    let thepred = Pred name t
    updateState (\(predmap, predmap2, exports) -> (predmap, insertObject name thepred predmap2, exports))

varp :: FOParser Var
varp = Var <$> identifier

varsp :: FOParser [Var]
varsp = many1 varp

progp :: FOParser (Either (Query, PredMap) Commit)
progp = do
    whiteSpace
    (do
        q <- Query <$> formulap
        eof
        (_, predmap, _) <- getState
        return (Left (q, predmap))) <|> (do
        reserved "commit"
        eof
        return (Right Commit))


rulep :: FOParser ([InsertRewritingRule], [InsertRewritingRule], [InsertRewritingRule])
rulep = (do
    whiteSpace
    _ <- many predp
    reserved "rewrite"
    reserved "insert" *> irulep
      <|> reserved "delete" *> drulep
      <|> qrule2p) where
          qrule2p = do
              r <- InsertRewritingRule <$> atomp <* rarrowp  <*> formulap
              return ([r], [], [])
          irulep = do
              r <- InsertRewritingRule <$> atomp <* rarrowp  <*> formulap
              return ([], [r], [])
          drulep = do
              r <- InsertRewritingRule <$> atomp <* rarrowp  <*> formulap
              return ([], [], [r])

namespacepathp :: FOParser (NamespacePath String)
namespacepathp = do
  ids <- many identifier
  return (foldl extendNamespacePath mempty ids)

importp :: FOParser ()
importp = do
    (predmap, workspace, predmap2) <- getState
    reserved "import"
    predmap2' <- (do
        reserved "qualified"
        (do
            reserved "all"
            reserved "from"
            ns <- namespacepathp
            (do
                reserved "except"
                preds <- many1 identifier
                return (importQualifiedExceptFromNamespace ns preds predmap predmap2)
                ) <|> (
                return (importAllFromNamespace ns predmap predmap2)
                )
            ) <|> (do
            prednames <- many1 identifier
            reserved "from"
            ns <- namespacepathp
            return (importQualifiedFromNamespace ns prednames predmap predmap2)
            )
        ) <|> (do
            reserved "all"
            reserved "from"
            ns <- namespacepathp
            (do
                reserved "except"
                preds <- many1 identifier
                return (importExceptFromNamespace ns preds predmap predmap2)
                ) <|> (
                return (importAllFromNamespace ns predmap predmap2)
                )
            ) <|> (do
            prednames <- many1 identifier
            reserved "from"
            ns <- namespacepathp
            return (importFromNamespace ns prednames predmap predmap2)
            )
    setState (predmap, workspace, fromMaybe (error "import") predmap2')


importsp :: FOParser ()
importsp = do
    many importp
    return ()

exportp :: FOParser ()
exportp = do
    (predmap, predmap2, workspace) <- getState
    reserved "export"
    exports' <- (do
        reserved "qualified"
        (do
            reserved "all"
            reserved "from"
            ns <- namespacepathp
            (do
                reserved "except"
                preds <- many1 identifier
                return (importQualifiedExceptFromNamespace ns preds predmap workspace)
                ) <|> (
                return (importQualifiedAllFromNamespace ns predmap workspace)
                )
            ) <|> (do
            prednames <- many1 identifier
            reserved "from"
            ns <- namespacepathp
            return (importQualifiedFromNamespace ns prednames predmap workspace)
            )
        ) <|> (do
        reserved "all"
        reserved "from"
        ns <- namespacepathp
        (do
            reserved "except"
            preds <- many1 identifier
            return (importExceptFromNamespace ns preds predmap workspace)
            ) <|> (
            return (importAllFromNamespace ns predmap workspace)
            )
        ) <|> (do
        prednames <- many1 identifier
        (do
            reserved "from"
            ns <- namespacepathp
            return (importFromNamespace ns prednames predmap workspace)
            ) <|> (return (importFromNamespace mempty prednames predmap2 workspace))
        )
    setState (predmap, predmap2, fromMaybe (error "export") exports')


exportsp :: FOParser ()
exportsp = do
   many exportp
   return ()

rulesp :: FOParser (([InsertRewritingRule], [InsertRewritingRule], [InsertRewritingRule]), PredMap, PredMap)
rulesp = do
    importsp
    exportsp
    rules <- many rulep
    eof
    (_, st, exports) <- getState
    return (mconcat rules, st, exports)

samePredAndKey :: Atom -> Atom -> Bool
samePredAndKey (Atom p1 args1) (Atom p2 args2) | p1 == p2 =
    keyComponents p1 args1 == keyComponents p2 args2
samePredAndKey _ _ = False

unionPred :: [Atom] -> [Atom] -> [Atom]
unionPred as1 as2 =
    as1 ++ filter (not . samePredAndKeys as1) as2 where
        samePredAndKeys as1 atom = any (samePredAndKey atom) as1
