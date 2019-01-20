{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleContexts, ExistentialQuantification, FlexibleInstances, OverloadedStrings #-}
module QueryArrow.DB.Trans.RuleParser (rulesp, processActions) where

import QueryArrow.Syntax.Term
import QueryArrow.DB.Trans.Rewriting

import Prelude hiding (lookup)
import Text.ParserCombinators.Parsec hiding (State)
import Data.Either (either)
import Control.Applicative ((<$>), (<*>), (<*), (*>))
import Data.Namespace.Path
import Data.Namespace.Namespace
import Data.List (partition)
import Control.Monad.State
import Data.Monoid ((<>))
import QueryArrow.Parser

predp :: FOParser Action
predp = do
    reserved "predicate"
    name <- prednamep
    t <- predtypep
    let thepred = Pred name t
    return (PredDef thepred)


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
    let workspace' = importQualifiedAllFromNamespaceE ns predmap workspace
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
