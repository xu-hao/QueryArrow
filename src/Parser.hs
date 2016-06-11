{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleContexts, ExistentialQuantification, FlexibleInstances, OverloadedStrings #-}
module Parser (progp, rulesp, Export(..)) where

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

lexer = T.makeTokenParser T.LanguageDef {
    T.commentStart = "/*",
    T.commentEnd = "*/",
    T.commentLine = "//",
    T.nestedComments = False,
    T.identStart = letter <|> char '_',
    T.identLetter = alphaNum <|> char '_',
    T.opStart = oneOf "~|⊗⊕∧∨∀∃¬⟶𝟏𝟎⊤⊥",
    T.opLetter = oneOf "~|⊗⊕∧∨∀∃¬⟶𝟏𝟎⊤⊥",
    T.reservedNames = ["insert", "return", "delete", "key", "object", "property", "rewrite", "predicate", "exists", "import", "export", "transactional", "qualified", "all", "from", "except", "if", "then", "else", "one", "zero"],
    T.reservedOpNames = ["~", "|", "||", "⊗", "⊕", "‖", "∃", "¬", "⟶","𝟏","𝟎"],
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
type FOParser = GenParser Char (PredMap, PredMap)

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
    (_, predmap) <- getState
    let thepred = fromMaybe (error ("atomp: undefined predicate " ++ show predname ++ ", available " ++ intercalate "\n" (map show (keys predmap)))) (lookup predname predmap)
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

formula1p :: FOParser Formula
formula1p = try (parens formulap)
       <|> (FReturn <$> returnp)
       <|> (FAtomic <$> try atomp)
       <|> transactionp *> pure FTransaction
       <|> (Not <$> (negp >> formula1p))
       <|> (do
           existsp
           vars <- varsp
           _ <- dot
           form <- formulap
           return (foldr Exists form vars))
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
        return (PredName (Just name) name2))
        <|> return (PredName Nothing name)

predp :: FOParser ()
predp = do
    reserved "predicate"
    name <- prednamep
    t <- predtypep
    let thepred = Pred name t
    updateState (\(predmap, predmap2) -> (insert name thepred predmap, insert name thepred predmap2))

varp :: FOParser Var
varp = Var <$> identifier

varsp :: FOParser [Var]
varsp = many1 varp

progp :: FOParser (Query, PredMap)
progp = do
    whiteSpace
    q <- Query <$> formulap
    eof
    (_, predmap) <- getState
    return (q, predmap)


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

importp :: FOParser ()
importp = do
    (predmap, predmap2) <- getState
    reserved "import"
    predmap' <- (do
        reserved "qualified"
        (ns, prednames) <- (do
            reserved "all"
            reserved "from"
            ns <- identifier
            (do
                reserved "except"
                preds <- many1 prednamep
                return (ns, map (\n -> UQPredName (name n)) (filter (\x -> namespace x == Just ns) (keys predmap)) \\ preds)
                ) <|> (do
                return (ns, map (\n -> UQPredName (name n)) (filter (\x -> namespace x == Just ns) (keys predmap)))
                )
            ) <|> (do
            prednames <- many1 prednamep
            reserved "from"
            ns <- identifier
            return (ns, prednames)
            )
        let predmap' = foldr (\x predmap' ->
                        let
                            predname = setNamespace ns x
                        in case lookup predname predmap of
                            Nothing -> error ("cannot import " ++ show x ++ " from " ++ ns)
                            Just pred1 -> insert predname pred1 predmap') predmap2 prednames
        return predmap'
        ) <|> (do
        (ns, prednames) <- (do
            reserved "all"
            reserved "from"
            ns <- identifier
            (do
                reserved "except"
                preds <- many1 prednamep
                return (ns, map (\n -> UQPredName (name n)) (filter (\x -> namespace x == Just ns) (keys predmap)) \\ preds)
                ) <|> (do
                return (ns, map (\n -> UQPredName (name n)) (filter (\x -> namespace x == Just ns) (keys predmap)))
                )
            ) <|> (do
            prednames <- many1 prednamep
            reserved "from"
            ns <- identifier
            return (ns, prednames)
            )
        let predmap' = foldr (\x predmap' ->
                        let
                            predname = setNamespace ns x
                        in case lookup predname predmap of
                            Nothing -> error ("cannot import " ++ show x ++ " from " ++ ns)
                            Just pred1 -> case lookup x predmap' of
                                            Nothing -> insert x pred1 predmap'
                                            Just pred0 -> error ("ambiguous import for predicate " ++ show x)) predmap2 prednames
        return predmap'
        )
    setState (predmap, predmap')


importsp :: FOParser ()
importsp = do
    many importp
    return ()

data Export = ExportQualified PredName | ExportUnqualified PredName | ExportAdd PredName deriving Show
exportp :: FOParser [Export]
exportp = do
    (predmap, _) <- getState
    reserved "export"
    (do
        reserved "qualified"
        (do
            reserved "all"
            reserved "from"
            ns <- identifier
            (do
                reserved "except"
                preds <- many1 prednamep
                return (map ExportQualified (filter (\x -> namespace x == Just ns) (keys predmap) \\ (map (setNamespace ns) preds)))
                ) <|> (do
                return (map ExportQualified (filter (\x -> namespace x == Just ns) (keys predmap)))
                )
            ) <|> (do
            prednames <- many1 prednamep
            reserved "from"
            ns <- identifier
            return (map ExportQualified (map (setNamespace ns) prednames))
            )
        ) <|> (do
        reserved "all"
        reserved "from"
        ns <- identifier
        (do
            reserved "except"
            preds <- many1 prednamep
            return (map ExportUnqualified (filter (\x -> namespace x == Just ns) (keys predmap) \\ (map (setNamespace ns) preds)))
            ) <|> (do
            return (map ExportUnqualified (filter (\x -> namespace x == Just ns) (keys predmap)))
            )
        ) <|> (do
        prednames <- many1 prednamep
        (do
            reserved "from"
            ns <- identifier
            return (map ExportUnqualified (map (setNamespace ns) prednames))
            ) <|> (return (map ExportAdd prednames))
        )


exportsp :: FOParser [Export]
exportsp = concat <$> many exportp

rulesp :: FOParser (([InsertRewritingRule], [InsertRewritingRule], [InsertRewritingRule]), PredMap, [Export])
rulesp = do
    importsp
    exports <- exportsp
    rules <- many rulep
    eof
    (_, st) <- getState
    return (mconcat rules, st, exports)

samePredAndKey :: Atom -> Atom -> Bool
samePredAndKey (Atom p1 args1) (Atom p2 args2) | p1 == p2 =
    keyComponents p1 args1 == keyComponents p2 args2
samePredAndKey _ _ = False

unionPred :: [Atom] -> [Atom] -> [Atom]
unionPred as1 as2 =
    as1 ++ filter (not . samePredAndKeys as1) as2 where
        samePredAndKeys as1 atom = any (samePredAndKey atom) as1
