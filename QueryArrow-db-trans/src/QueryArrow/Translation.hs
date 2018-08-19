{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, FlexibleInstances, UndecidableInstances, FlexibleContexts, DeriveGeneric #-}
module QueryArrow.Translation where

import QueryArrow.DB.DB
import QueryArrow.FO.Data
import QueryArrow.Syntax.Type
import QueryArrow.FO.TypeChecker
import QueryArrow.Semantics.Value
import QueryArrow.FO.Utils
import QueryArrow.QueryPlan
import QueryArrow.Rewriting
import QueryArrow.Config
import QueryArrow.Utils
import QueryArrow.ListUtils
import QueryArrow.Plugin
import QueryArrow.RuleParser

import Prelude hiding (lookup)
import Data.ByteString.Lazy.UTF8 (toString)
import Data.Map.Strict (foldrWithKey, elems, lookup, unionWithKey)
import Control.Monad.Except
import Control.Monad.Trans.Reader
import Control.Monad.Trans.Except
import Control.Monad.Trans.State
import qualified Data.ByteString.Lazy as B
import Text.ParserCombinators.Parsec hiding (State)
import Data.Namespace.Namespace
import Algebra.SemiBoundedLattice
import Algebra.Lattice
import Data.Set (toAscList, Set)
import Data.Monoid
import Data.Aeson
import GHC.Generics
import System.Log.Logger (debugM)
import Language.Preprocessor.Cpphs (runCpphs, defaultCpphsOptions, CpphsOptions(..), defaultBoolOptions, BoolOptions(..))
-- exec query from dbname

-- getAllResults2 :: (MonadIO m, MonadBaseControl IO m, IResultRow row, Num (ElemType row), Ord (ElemType row)) => [AbstractDatabase row Formula] -> MSet Var -> Formula -> m [row]
-- getAllResults2 dbs rvars query = do
--     qp <- prepareQuery' dbs rvars query bottom
--     let (_, stream) = execQueryPlan ([], pure mempty) qp
--     getAllResultsInStream stream


-- queryPlan2 :: AbstractDBList row -> Set Var -> MSet Var -> Formula -> QueryPlan2
-- queryPlan2 dbs vars vars2  formula =
--     let qp = formulaToQueryPlan dbs formula
--         qp1 = simplifyQueryPlan qp
--         qp2 = calculateVars vars vars2 qp1 in
--         optimizeQueryPlan dbs qp2

-- rewriteQuery' :: (MonadIO m) => [InsertRewritingRule] -> [InsertRewritingRule] -> [InsertRewritingRule] -> MSet Var -> Formula -> Set Var -> m Formula
-- rewriteQuery' qr ir dr rvars qu0 vars = do
--     liftIO $ infoM "QA" ("original query: " ++ show qu0)
--     let qu = rewriteQuery  qr ir dr rvars qu0 vars
--     liftIO $ infoM "QA" ("rewritten query: " ++ show qu)
--     return qu
--

defaultRewritingLimit :: Int
defaultRewritingLimit = 100

type RewritingRuleSets = ([InsertRewritingRule],  [InsertRewritingRule], [InsertRewritingRule])

type RewritingRuleTSets = ([InsertRewritingRuleT],  [InsertRewritingRuleT], [InsertRewritingRuleT])

rewriteQuery :: [InsertRewritingRuleT] -> [InsertRewritingRuleT] -> [InsertRewritingRuleT] -> MSet Var -> FormulaT -> Set Var -> FormulaT
rewriteQuery  qr ir dr vars form ext = runNew (do
    registerVars (toAscList ((case vars of
        Include vs -> vs
        Exclude vs -> vs)  \/ freeVars form))
    rewrites defaultRewritingLimit ext   qr ir dr form)

data TransDB db = TransDB String db [Pred] RewritingRuleTSets

instance (IDatabaseUniformDBFormula FormulaT db) => IDatabase0 (TransDB db) where
    type DBFormulaType (TransDB db) = FormulaT
    getName (TransDB name _ _ _ ) = name
    getPreds (TransDB _ _ predmap _ ) = predmap
    supported _ _ _ _ = True

instance (IDatabaseUniformDBFormula FormulaT db) => IDatabase1 (TransDB db) where
    type DBQueryType (TransDB db) = DBQueryType db
    translateQuery (TransDB _ db _ (qr, ir, dr) ) vars2 qu vars =
                  let qu' = rewriteQuery qr ir dr (Include vars2) qu vars in do
                      qu' <- translateQuery db vars2 qu' vars
                      return qu' 


instance (IDatabase db) => IDatabase2 (TransDB db) where
    newtype ConnectionType (TransDB db) = TransDBConnection (ConnectionType db)
    dbOpen (TransDB _ db  _ _ ) = TransDBConnection <$> dbOpen db

instance (IDatabaseUniformDBFormula FormulaT db) => IDatabase (TransDB db)

instance (IDatabase db) => IDBConnection0 (ConnectionType (TransDB db)) where
    dbClose (TransDBConnection db ) = dbClose db
    dbBegin (TransDBConnection db) = dbBegin db
    dbCommit (TransDBConnection db) = dbCommit db
    dbPrepare (TransDBConnection db) = dbPrepare db
    dbRollback (TransDBConnection db) = dbRollback db

instance (IDatabase db) => IDBConnection (ConnectionType (TransDB db)) where
    type QueryType (ConnectionType (TransDB db)) = QueryType (ConnectionType db)
    type StatementType (ConnectionType (TransDB db)) = StatementType (ConnectionType db)
    prepareQuery (TransDBConnection db) = prepareQuery db
    -- exec (TransDB _ dbs _ _  _ _ _) qp vars stream = snd (execQueryPlan dbs (vars, stream ) qp)

getRewriting :: PredMap -> ICATTranslationConnInfo -> IO (RewritingRuleSets, PredMap, PredMap)
getRewriting predmap ps = do
    d0 <- toString <$> B.readFile (rewriting_file_path ps)
    d1 <- runCpphs defaultCpphsOptions{includes = include_file_path ps, boolopts = defaultBoolOptions {locations = False}}  (rewriting_file_path ps) d0
    case runParser rulesp () (rewriting_file_path ps) d1 of
        Left err -> error ("getRewriting: " ++ show err)
        Right actions ->
            return (processActions predmap actions)

typecheckRules :: PredTypeMap -> RewritingRuleSets -> Either String RewritingRuleTSets
typecheckRules ptm (qr, ir, dr) = do
  qr' <- mapM (\r ->
      case runNew (runReaderT (evalStateT (runExceptT (typecheck r)) (mempty, mempty)) ptm) of
          Right a -> return a
          Left err -> Left ("typecheckRules: rewrite rule " ++ show r ++ " type error\n" ++ err)) qr
  ir' <- mapM (\r ->
      case runNew (runReaderT (evalStateT (runExceptT (typecheck r)) (mempty, mempty)) ptm) of
          Right a -> return a
          Left err -> Left ("typecheckRules: insert rewrite rule " ++ show r ++ " type error\n" ++ err)) ir
  dr' <- mapM (\r ->
      case runNew (runReaderT (evalStateT (runExceptT (typecheck r)) (mempty, mempty)) ptm) of
          Right a -> return a
          Left err -> Left ("typecheckRules: delete rewrite rule " ++ show r ++ " type error\n" ++ err)) dr
  return (qr', ir', dr')

transDB :: (IDatabase db, DBFormulaType db ~ FormulaT, RowType (StatementType (ConnectionType db)) ~ MapResultRow) => String -> db -> ICATTranslationConnInfo -> IO (TransDB db)
transDB name sumdb transinfo = do
            let predmap0 = constructDBPredMap sumdb
            -- trace ("preds:\n" ++ intercalate "\n" (map show (elems predmap0))) $ return ()
            (rewriting, predmap, exports) <- getRewriting predmap0 transinfo
            let exportmap = allObjects exports
            let (rules0, exportedpreds) = foldrWithKey (\key pred1@(Pred pn predtype@(PredType _ paramTypes)) (rules0', exportedpreds') ->
                    if key /= pn
                        then
                            let pred0 = Pred key predtype
                                params = map (\i -> VarExpr (Var ("var" ++ show i))) [0..length paramTypes - 1]
                                atom0 = Atom key params
                                atom1 = Atom pn params in
                                ((if null (outputOnlyComponents predtype params)
                                  then ([InsertRewritingRule atom0 (FAtomic atom1)], [InsertRewritingRule atom0 (FInsert (Lit Pos atom1))], [InsertRewritingRule atom0 (FInsert (Lit Neg atom1))])
                                  else ([InsertRewritingRule atom0 (FAtomic atom1)], [], [])) <> rules0', pred0 : exportedpreds')
                        else
                            (rules0', pred1 : exportedpreds')) (([], [], []), []) exportmap
            -- trace (intercalate "\n" (map show (exports))) $ return ()
            -- trace (intercalate "\n" (map show (predmap1))) $ return ()
            let repeats = findRepeats exportedpreds
            unless (null repeats) $ error ("more than one export for predicates " ++ show repeats)
            let rules1@(qr, ir, dr) = rules0 <> rewriting
            let checkPatterns rules = do
                    let repeats1 = findRepeats (map (\(InsertRewritingRule (Atom p _) _) -> p) rules)
                    unless (null repeats1) $ error ("more than one definition for predicates " ++ show repeats1)
            checkPatterns qr
            checkPatterns ir
            checkPatterns dr
            mapM_ (debugM "QA" . show) qr
            mapM_ (debugM "QA" . show) ir
            mapM_ (debugM "QA" . show) dr
            let ptm = constructPredTypeMap (elems (allObjects predmap) ++ exportedpreds)
            let checkNoOutput (InsertRewritingRule (Atom p args) _) =
                    case lookup p ptm of
                      Nothing -> error "error"
                      Just pt ->
                          unless (null (outputOnlyComponents pt args)) $ error "rule pattern contains output parameters"
            mapM_ checkNoOutput ir
            mapM_ checkNoOutput dr
            case typecheckRules ptm rules1 of
              Left err -> error err
              Right rules1' ->
                return (TransDB name sumdb exportedpreds rules1')

data ICATTranslationConnInfo = ICATCacheConnInfo {
  rewriting_file_path :: String,
  include_file_path :: [String],
  trans_db_plugin :: ICATDBConnInfo
} deriving (Show, Generic)

instance ToJSON ICATTranslationConnInfo
instance FromJSON ICATTranslationConnInfo

data TransPlugin = TransPlugin

instance Plugin TransPlugin MapResultRow where
  getDB _ getDB0 ps = do
    let fsconf = getDBSpecificConfig ps
    db0 <- getDB0 (trans_db_plugin fsconf)
    case db0 of
        AbstractDatabase db -> AbstractDatabase <$> transDB (qap_name ps) db fsconf
