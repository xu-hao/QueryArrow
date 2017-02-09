{-# LANGUAGE TypeFamilies #-}

module QueryArrow.Binding.Binding where

import QueryArrow.FO.Data
import QueryArrow.Utils
import QueryArrow.DB.ResultStream
import QueryArrow.DB.DB
import QueryArrow.DB.NoConnection
import Data.Set (Set, member)
import Data.Map (fromList, singleton)
import Control.Monad.IO.Class (MonadIO(..))
import QueryArrow.FO.Utils

class Binding a where
  bindingPred :: a -> Pred
  dbName :: a -> String

class Binding a => UnaryBinding a where
  supportI :: a -> Bool
  supportO :: a -> Bool
  supportInsertI :: a -> Bool
  supportDeleteI :: a -> Bool

class UnaryBinding a => EffectFreeUnaryBinding a where
  execI :: a -> MapResultRow -> ResultValue -> [()]
  execO :: a -> MapResultRow -> Var -> [ResultValue]

class UnaryBinding a => EffectfulUnaryBinding a where
  execIE :: a -> MapResultRow -> ResultValue -> IO [()]
  execOE :: a -> MapResultRow -> Var -> IO [ResultValue]
  insertIE :: a -> MapResultRow -> ResultValue -> IO ()
  deleteIE :: a -> MapResultRow -> ResultValue -> IO ()

newtype EffectFreeUnaryBindingDatabase a = EffectFreeUnaryBindingDatabase a
newtype EffectfulUnaryBindingDatabase a = EffectfulUnaryBindingDatabase a

instance UnaryBinding a => IDatabase0 (EffectFreeUnaryBindingDatabase a) where
    type DBFormulaType (EffectFreeUnaryBindingDatabase a) = Formula
    getName (EffectFreeUnaryBindingDatabase db) = dbName db
    getPreds (EffectFreeUnaryBindingDatabase db) = [ bindingPred db ]
    supported (EffectFreeUnaryBindingDatabase db) _ (FAtomic (Atom _ [VarExpr var1])) env | not (var1 `member` env) = supportO db
    supported (EffectFreeUnaryBindingDatabase db) _ (FAtomic (Atom _ _)) _ = supportI db
    supported (EffectFreeUnaryBindingDatabase db) _ (FInsert (Lit Pos _)) _ = supportInsertI db
    supported (EffectFreeUnaryBindingDatabase db) _ (FInsert (Lit Neg _)) _ = supportDeleteI db
    supported _ _ _ _ = False
    checkQuery _ _ _ _ = return (Right ())

instance UnaryBinding a => IDatabase0 (EffectfulUnaryBindingDatabase a) where
    type DBFormulaType (EffectfulUnaryBindingDatabase a) = Formula
    getName (EffectfulUnaryBindingDatabase db) = dbName db
    getPreds (EffectfulUnaryBindingDatabase db) = [ bindingPred db ]
    supported (EffectfulUnaryBindingDatabase db) _ (FAtomic (Atom _ [VarExpr var1])) env | not (var1 `member` env) = supportO db
    supported (EffectfulUnaryBindingDatabase db) _ (FAtomic (Atom _ _)) _ = supportI db
    supported (EffectfulUnaryBindingDatabase db) _ (FInsert (Lit Pos _)) _ = supportInsertI db
    supported (EffectfulUnaryBindingDatabase db) _ (FInsert (Lit Neg _)) _ = supportDeleteI db
    supported _ _ _ _ = False
    checkQuery _ _ _ _ = return (Right ())

instance UnaryBinding a => IDatabase1 (EffectFreeUnaryBindingDatabase a) where
    type DBQueryType (EffectFreeUnaryBindingDatabase a) = ( Set Var, Formula,  Set Var)
    translateQuery _ vars qu vars2 = return (vars, qu, vars2)

instance UnaryBinding a => IDatabase1 (EffectfulUnaryBindingDatabase a) where
    type DBQueryType (EffectfulUnaryBindingDatabase a) = ( Set Var, Formula,  Set Var)
    translateQuery _ vars qu vars2 = return (vars, qu, vars2)

instance EffectFreeUnaryBinding a => INoConnectionDatabase2 (EffectFreeUnaryBindingDatabase a) where
    type NoConnectionQueryType (EffectFreeUnaryBindingDatabase a) = (Set Var, Formula, Set Var)
    type NoConnectionRowType (EffectFreeUnaryBindingDatabase a) = MapResultRow
    noConnectionDBStmtExec (EffectFreeUnaryBindingDatabase db) (_,  FAtomic (Atom _ [VarExpr a]), env) stream | not (a `member` env) = do
        row <- stream
        let rs = execO db row a
        listResultStream (map (\val1 -> singleton a val1) rs)
    noConnectionDBStmtExec (EffectFreeUnaryBindingDatabase db) (_, FAtomic (Atom _ [a]), _) stream = do
        row <- stream
        let aval = evalExpr row a
        let rs = execI db row aval
        listResultStream (map (\() -> mempty) rs)
    noConnectionDBStmtExec _ (_, FInsert (Lit Pos _), _) _ =
        return mempty
    noConnectionDBStmtExec _ (_, FInsert (Lit Neg _), _) _ =
        return mempty

    noConnectionDBStmtExec _ qu _ = error ("noConnectionDBStmtExec: unsupported Formula " ++ show qu)

instance EffectfulUnaryBinding a => INoConnectionDatabase2 (EffectfulUnaryBindingDatabase a) where
    type NoConnectionQueryType (EffectfulUnaryBindingDatabase a) = (Set Var, Formula, Set Var)
    type NoConnectionRowType (EffectfulUnaryBindingDatabase a) = MapResultRow
    noConnectionDBStmtExec (EffectfulUnaryBindingDatabase db) (_,  FAtomic (Atom _ [VarExpr a]), env) stream | not (a `member` env) = do
        row <- stream
        rs <- liftIO $ execOE db row a
        listResultStream (map (\val1 -> singleton a val1) rs)
    noConnectionDBStmtExec (EffectfulUnaryBindingDatabase db) (_, FAtomic (Atom _ [a]), _) stream = do
        row <- stream
        let aval = evalExpr row a
        rs <- liftIO $ execIE db row aval
        listResultStream (map (\() -> mempty) rs)
    noConnectionDBStmtExec (EffectfulUnaryBindingDatabase db) (_, FInsert (Lit Pos (Atom _ [a])), _) stream = do
        row <- stream
        let aval = evalExpr row a
        liftIO $ insertIE db row aval
        return mempty
    noConnectionDBStmtExec (EffectfulUnaryBindingDatabase db) (_, FInsert (Lit Neg (Atom _ [a])), _) stream = do
        row <- stream
        let aval = evalExpr row a
        liftIO $ deleteIE db row aval
        return mempty

    noConnectionDBStmtExec _ qu _ = error ("noConnectionDBStmtExec: unsupported Formula " ++ show qu)

class Binding a => BinaryBinding a where
  supportIO :: a -> Bool
  supportOI :: a -> Bool
  supportII :: a -> Bool
  supportOO :: a -> Bool
  supportInsertII :: a -> Bool
  supportDeleteII :: a -> Bool

class BinaryBinding a => EffectFreeBinaryBinding a where
  execIO :: a -> MapResultRow -> ResultValue -> Var -> [ResultValue]
  execOI :: a -> MapResultRow -> Var -> ResultValue -> [ResultValue]
  execOO :: a -> MapResultRow -> Var -> Var -> [(ResultValue, ResultValue)]
  execII :: a -> MapResultRow -> ResultValue -> ResultValue -> [()]

class BinaryBinding a => EffectfulBinaryBinding a where
  execIOE :: a -> MapResultRow -> ResultValue -> Var -> IO [ResultValue]
  execOIE :: a -> MapResultRow -> Var -> ResultValue -> IO [ResultValue]
  execOOE :: a -> MapResultRow -> Var -> Var -> IO [(ResultValue, ResultValue)]
  execIIE :: a -> MapResultRow -> ResultValue -> ResultValue -> IO [()]
  insertIIE :: a -> MapResultRow -> ResultValue -> ResultValue -> IO ()
  deleteIIE :: a -> MapResultRow -> ResultValue -> ResultValue -> IO ()

newtype EffectFreeBinaryBindingDatabase a = EffectFreeBinaryBindingDatabase a
newtype EffectfulBinaryBindingDatabase a = EffectfulBinaryBindingDatabase a

instance BinaryBinding a => IDatabase0 (EffectFreeBinaryBindingDatabase a) where
    type DBFormulaType (EffectFreeBinaryBindingDatabase a) = Formula
    getName (EffectFreeBinaryBindingDatabase db) = dbName db
    getPreds (EffectFreeBinaryBindingDatabase db) = [ bindingPred db ]
    supported (EffectFreeBinaryBindingDatabase db) _ (FAtomic (Atom _ [VarExpr var1, VarExpr var2])) env | not (var1 `member` env || var2 `member` env) = supportOO db
    supported (EffectFreeBinaryBindingDatabase db) _ (FAtomic (Atom _ [VarExpr var1, _])) env | not (var1 `member` env) = supportOI db
    supported (EffectFreeBinaryBindingDatabase db) _ (FAtomic (Atom _ [_, VarExpr var2])) env | not (var2 `member` env) = supportIO db
    supported (EffectFreeBinaryBindingDatabase db) _ (FAtomic (Atom _ _)) _ = supportII db
    supported (EffectFreeBinaryBindingDatabase db) _ (FInsert (Lit Pos _)) _ = supportInsertII db
    supported (EffectFreeBinaryBindingDatabase db) _ (FInsert (Lit Neg _)) _ = supportDeleteII db
    supported _ _ _ _ = False
    checkQuery _ _ _ _ = return (Right ())

instance BinaryBinding a => IDatabase0 (EffectfulBinaryBindingDatabase a) where
    type DBFormulaType (EffectfulBinaryBindingDatabase a) = Formula
    getName (EffectfulBinaryBindingDatabase db) = dbName db
    getPreds (EffectfulBinaryBindingDatabase db) = [ bindingPred db ]
    supported (EffectfulBinaryBindingDatabase db) _ (FAtomic (Atom _ [VarExpr var1, VarExpr var2])) env | not (var1 `member` env || var2 `member` env) = supportOO db
    supported (EffectfulBinaryBindingDatabase db) _ (FAtomic (Atom _ [VarExpr var1, _])) env | not (var1 `member` env) = supportOI db
    supported (EffectfulBinaryBindingDatabase db) _ (FAtomic (Atom _ [_, VarExpr var2])) env | not (var2 `member` env) = supportIO db
    supported (EffectfulBinaryBindingDatabase db) _ (FAtomic (Atom _ _)) _ = supportII db
    supported (EffectfulBinaryBindingDatabase db) _ (FInsert (Lit Pos _)) _ = supportInsertII db
    supported (EffectfulBinaryBindingDatabase db) _ (FInsert (Lit Neg _)) _ = supportDeleteII db
    supported _ _ _ _ = False
    checkQuery _ _ _ _ = return (Right ())

instance BinaryBinding a => IDatabase1 (EffectFreeBinaryBindingDatabase a) where
    type DBQueryType (EffectFreeBinaryBindingDatabase a) = ( Set Var, Formula,  Set Var)
    translateQuery _ vars qu vars2 = return (vars, qu, vars2)

instance BinaryBinding a => IDatabase1 (EffectfulBinaryBindingDatabase a) where
    type DBQueryType (EffectfulBinaryBindingDatabase a) = ( Set Var, Formula,  Set Var)
    translateQuery _ vars qu vars2 = return (vars, qu, vars2)

instance EffectFreeBinaryBinding a => INoConnectionDatabase2 (EffectFreeBinaryBindingDatabase a) where
    type NoConnectionQueryType (EffectFreeBinaryBindingDatabase a) = (Set Var, Formula, Set Var)
    type NoConnectionRowType (EffectFreeBinaryBindingDatabase a) = MapResultRow
    noConnectionDBStmtExec (EffectFreeBinaryBindingDatabase db) (_,  FAtomic (Atom _ [VarExpr a, VarExpr b]), env) stream | not (a `member` env || b `member` env) = do
        row <- stream
        let rs = execOO db row a b
        listResultStream (map (\(val1,val2) -> fromList [(a, val1),(b,val2)]) rs)
    noConnectionDBStmtExec (EffectFreeBinaryBindingDatabase db) (_, FAtomic (Atom _ [a, VarExpr b]), env) stream | not (b `member` env) = do
        row <- stream
        let aval = evalExpr row a
        let rs = execIO db row aval b
        listResultStream (map (\val2 -> singleton b val2) rs)
    noConnectionDBStmtExec (EffectFreeBinaryBindingDatabase db) (_, FAtomic (Atom _ [VarExpr a, b]), env) stream | not (a `member` env) = do
        row <- stream
        let bval = evalExpr row b
        let rs = execOI db row a bval
        listResultStream (map (\val1 -> singleton a val1) rs)
    noConnectionDBStmtExec (EffectFreeBinaryBindingDatabase db) (_, FAtomic (Atom _ [a, b]), _) stream = do
        row <- stream
        let aval = evalExpr row a
        let bval = evalExpr row b
        let rs = execII db row aval bval
        listResultStream (map (\() -> mempty) rs)
    noConnectionDBStmtExec _ (_, FInsert (Lit Pos _), _) _ =
        return mempty
    noConnectionDBStmtExec _ (_, FInsert (Lit Neg _), _) _ =
        return mempty

    noConnectionDBStmtExec _ qu _ = error ("noConnectionDBStmtExec: unsupported Formula " ++ show qu)

instance EffectfulBinaryBinding a => INoConnectionDatabase2 (EffectfulBinaryBindingDatabase a) where
    type NoConnectionQueryType (EffectfulBinaryBindingDatabase a) = (Set Var, Formula, Set Var)
    type NoConnectionRowType (EffectfulBinaryBindingDatabase a) = MapResultRow
    noConnectionDBStmtExec (EffectfulBinaryBindingDatabase db) (_,  FAtomic (Atom _ [VarExpr a, VarExpr b]), env) stream | not (a `member` env || b `member` env) = do
        row <- stream
        rs <- liftIO $ execOOE db row a b
        listResultStream (map (\(val1,val2) -> fromList [(a, val1),(b,val2)]) rs)
    noConnectionDBStmtExec (EffectfulBinaryBindingDatabase db) (_, FAtomic (Atom _ [a, VarExpr b]), env) stream | not (b `member` env) = do
        row <- stream
        let aval = evalExpr row a
        rs <- liftIO $ execIOE db row aval b
        listResultStream (map (\val2 -> singleton b val2) rs)
    noConnectionDBStmtExec (EffectfulBinaryBindingDatabase db) (_, FAtomic (Atom _ [VarExpr a, b]), env) stream | not (a `member` env) = do
        row <- stream
        let bval = evalExpr row b
        rs <- liftIO $ execOIE db row a bval
        listResultStream (map (\val1 -> singleton a val1) rs)
    noConnectionDBStmtExec (EffectfulBinaryBindingDatabase db) (_, FAtomic (Atom _ [a, b]), _) stream = do
        row <- stream
        let aval = evalExpr row a
        let bval = evalExpr row b
        rs <- liftIO $ execIIE db row aval bval
        listResultStream (map (\() -> mempty) rs)
    noConnectionDBStmtExec (EffectfulBinaryBindingDatabase db) (_, FInsert (Lit Pos (Atom _ [a, b])), _) stream = do
        row <- stream
        let aval = evalExpr row a
        let bval = evalExpr row b
        liftIO $ insertIIE db row aval bval
        return mempty
    noConnectionDBStmtExec (EffectfulBinaryBindingDatabase db) (_, FInsert (Lit Neg (Atom _ [a, b])), _) stream = do
        row <- stream
        let aval = evalExpr row a
        let bval = evalExpr row b
        liftIO $ deleteIIE db row aval bval
        return mempty

    noConnectionDBStmtExec _ qu _ = error ("noConnectionDBStmtExec: unsupported Formula " ++ show qu)

class Binding a => TernaryBinding a where
  supportIIO :: a -> Bool
  supportIOI :: a -> Bool
  supportIII :: a -> Bool
  supportIOO :: a -> Bool
  supportOIO :: a -> Bool
  supportOOI :: a -> Bool
  supportOII :: a -> Bool
  supportOOO :: a -> Bool
  supportInsertIII :: a -> Bool
  supportDeleteIII :: a -> Bool

class TernaryBinding a => EffectFreeTernaryBinding a where
  execIIO :: a -> MapResultRow -> ResultValue -> ResultValue -> Var -> [ResultValue]
  execIOI :: a -> MapResultRow -> ResultValue -> Var -> ResultValue -> [ResultValue]
  execIOO :: a -> MapResultRow -> ResultValue -> Var -> Var -> [(ResultValue, ResultValue)]
  execIII :: a -> MapResultRow -> ResultValue -> ResultValue -> ResultValue -> [()]
  execOIO :: a -> MapResultRow -> Var -> ResultValue -> Var -> [(ResultValue, ResultValue)]
  execOOI :: a -> MapResultRow -> Var -> Var -> ResultValue -> [(ResultValue, ResultValue)]
  execOOO :: a -> MapResultRow -> Var -> Var -> Var -> [(ResultValue, ResultValue, ResultValue)]
  execOII :: a -> MapResultRow -> Var -> ResultValue -> ResultValue -> [ResultValue]

class TernaryBinding a => EffectfulTernaryBinding a where
  execIIOE :: a -> MapResultRow -> ResultValue -> ResultValue -> Var -> IO [ResultValue]
  execIOIE :: a -> MapResultRow -> ResultValue -> Var -> ResultValue -> IO [ResultValue]
  execIOOE :: a -> MapResultRow -> ResultValue -> Var -> Var -> IO [(ResultValue, ResultValue)]
  execIIIE :: a -> MapResultRow -> ResultValue -> ResultValue -> ResultValue -> IO [()]
  execOIOE :: a -> MapResultRow -> Var -> ResultValue -> Var -> IO [(ResultValue, ResultValue)]
  execOOIE :: a -> MapResultRow -> Var -> Var -> ResultValue -> IO [(ResultValue, ResultValue)]
  execOOOE :: a -> MapResultRow -> Var -> Var -> Var -> IO [(ResultValue, ResultValue, ResultValue)]
  execOIIE :: a -> MapResultRow -> Var -> ResultValue -> ResultValue -> IO [ResultValue]
  insertIIIE :: a -> MapResultRow -> ResultValue -> ResultValue -> ResultValue -> IO ()
  deleteIIIE :: a -> MapResultRow -> ResultValue -> ResultValue -> ResultValue -> IO ()

newtype EffectFreeTernaryBindingDatabase a = EffectFreeTernaryBindingDatabase a
newtype EffectfulTernaryBindingDatabase a = EffectfulTernaryBindingDatabase a

instance TernaryBinding a => IDatabase0 (EffectFreeTernaryBindingDatabase a) where
    type DBFormulaType (EffectFreeTernaryBindingDatabase a) = Formula
    getName (EffectFreeTernaryBindingDatabase db) = dbName db
    getPreds (EffectFreeTernaryBindingDatabase db) = [ bindingPred db ]
    supported (EffectFreeTernaryBindingDatabase db) _ (FAtomic (Atom _ [VarExpr var1, VarExpr var2, VarExpr var3])) env | not (var1 `member` env || var2 `member` env || var3 `member` env) = supportOOO db
    supported (EffectFreeTernaryBindingDatabase db) _ (FAtomic (Atom _ [VarExpr var1, _, VarExpr var3])) env | not (var1 `member` env || var3 `member` env) = supportOIO db
    supported (EffectFreeTernaryBindingDatabase db) _ (FAtomic (Atom _ [_, VarExpr var2, VarExpr var3])) env | not (var2 `member` env || var3 `member` env) = supportIOO db
    supported (EffectFreeTernaryBindingDatabase db) _ (FAtomic (Atom _ [_, _, VarExpr var3])) env |  not (var3 `member` env) = supportIIO db
    supported (EffectFreeTernaryBindingDatabase db) _ (FAtomic (Atom _ [VarExpr var1, VarExpr var2, _])) env | not (var1 `member` env || var2 `member` env) = supportOOI db
    supported (EffectFreeTernaryBindingDatabase db) _ (FAtomic (Atom _ [VarExpr var1, _, _])) env | not (var1 `member` env) = supportOII db
    supported (EffectFreeTernaryBindingDatabase db) _ (FAtomic (Atom _ [_, VarExpr var2, _])) env | not (var2 `member` env) = supportIOI db
    supported (EffectFreeTernaryBindingDatabase db) _ (FAtomic (Atom _ _)) _ = supportIII db
    supported (EffectFreeTernaryBindingDatabase db) _ (FInsert (Lit Pos _)) _ = supportInsertIII db
    supported (EffectFreeTernaryBindingDatabase db) _ (FInsert (Lit Neg _)) _ = supportDeleteIII db
    supported _ _ _ _ = False
    checkQuery _ _ _ _ = return (Right ())

instance TernaryBinding a => IDatabase0 (EffectfulTernaryBindingDatabase a) where
    type DBFormulaType (EffectfulTernaryBindingDatabase a) = Formula
    getName (EffectfulTernaryBindingDatabase db) = dbName db
    getPreds (EffectfulTernaryBindingDatabase db) = [ bindingPred db ]
    supported (EffectfulTernaryBindingDatabase db) _ (FAtomic (Atom _ [VarExpr var1, VarExpr var2, VarExpr var3])) env | not (var1 `member` env || var2 `member` env || var3 `member` env) = supportOOO db
    supported (EffectfulTernaryBindingDatabase db) _ (FAtomic (Atom _ [VarExpr var1, _, VarExpr var3])) env | not (var1 `member` env || var3 `member` env) = supportOIO db
    supported (EffectfulTernaryBindingDatabase db) _ (FAtomic (Atom _ [_, VarExpr var2, VarExpr var3])) env | not (var2 `member` env || var3 `member` env) = supportIOO db
    supported (EffectfulTernaryBindingDatabase db) _ (FAtomic (Atom _ [_, _, VarExpr var3])) env | not (var3 `member` env) = supportIIO db
    supported (EffectfulTernaryBindingDatabase db) _ (FAtomic (Atom _ [VarExpr var1, VarExpr var2, _])) env | not (var1 `member` env || var2 `member` env) = supportOOI db
    supported (EffectfulTernaryBindingDatabase db) _ (FAtomic (Atom _ [VarExpr var1, _, _])) env | not (var1 `member` env) = supportOII db
    supported (EffectfulTernaryBindingDatabase db) _ (FAtomic (Atom _ [_, VarExpr var2, _])) env | not (var2 `member` env) = supportIOI db
    supported (EffectfulTernaryBindingDatabase db) _ (FAtomic (Atom _ _)) _ = supportIII db
    supported (EffectfulTernaryBindingDatabase db) _ (FInsert (Lit Pos _)) _ = supportInsertIII db
    supported (EffectfulTernaryBindingDatabase db) _ (FInsert (Lit Neg _)) _ = supportDeleteIII db
    supported _ _ _ _ = False
    checkQuery _ _ _ _ = return (Right ())

instance TernaryBinding a => IDatabase1 (EffectFreeTernaryBindingDatabase a) where
    type DBQueryType (EffectFreeTernaryBindingDatabase a) = ( Set Var, Formula,  Set Var)
    translateQuery _ vars qu vars2 = return (vars, qu, vars2)

instance TernaryBinding a => IDatabase1 (EffectfulTernaryBindingDatabase a) where
    type DBQueryType (EffectfulTernaryBindingDatabase a) = ( Set Var, Formula,  Set Var)
    translateQuery _ vars qu vars2 = return (vars, qu, vars2)

instance EffectFreeTernaryBinding a => INoConnectionDatabase2 (EffectFreeTernaryBindingDatabase a) where
    type NoConnectionQueryType (EffectFreeTernaryBindingDatabase a) = (Set Var, Formula, Set Var)
    type NoConnectionRowType (EffectFreeTernaryBindingDatabase a) = MapResultRow
    noConnectionDBStmtExec (EffectFreeTernaryBindingDatabase db) (_,  FAtomic (Atom _ [VarExpr a, VarExpr b, VarExpr c]), env) stream | not (a `member` env || b `member` env || c `member` env) = do
        row <- stream
        let rs = execOOO db row a b c
        listResultStream (map (\(val1,val2,val3) -> fromList [(a, val1),(b,val2),(c,val3)]) rs)
    noConnectionDBStmtExec (EffectFreeTernaryBindingDatabase db) (_, FAtomic (Atom _ [a, VarExpr b, VarExpr c]), env) stream | not (b `member` env || c `member` env) = do
        row <- stream
        let aval = evalExpr row a
        let rs = execIOO db row aval b c
        listResultStream (map (\(val2,val3) -> fromList [(b,val2),(c,val3)]) rs)
    noConnectionDBStmtExec (EffectFreeTernaryBindingDatabase db) (_, FAtomic (Atom _ [VarExpr a, b, VarExpr c]), env) stream | not (a `member` env || c `member` env) = do
        row <- stream
        let bval = evalExpr row b
        let rs = execOIO db row a bval c
        listResultStream (map (\(val1,val3) -> fromList [(a, val1),(c,val3)]) rs)
    noConnectionDBStmtExec (EffectFreeTernaryBindingDatabase db) (_, FAtomic (Atom _ [a, b, VarExpr c]), env) stream | not (c `member` env) = do
        row <- stream
        let aval = evalExpr row a
        let bval = evalExpr row b
        let rs = execIIO db row aval bval c
        listResultStream (map (\val3 -> singleton c val3) rs)
    noConnectionDBStmtExec (EffectFreeTernaryBindingDatabase db) (_,  FAtomic (Atom _ [VarExpr a, VarExpr b, c]), env) stream | not (a `member` env || b `member` env) = do
        row <- stream
        let cval = evalExpr row c
        let rs = execOOI db row a b cval
        listResultStream (map (\(val1,val2) -> fromList [(a, val1),(b,val2)]) rs)
    noConnectionDBStmtExec (EffectFreeTernaryBindingDatabase db) (_, FAtomic (Atom _ [a, VarExpr b, c]), env) stream | not (b `member` env) = do
        row <- stream
        let aval = evalExpr row a
        let cval = evalExpr row c
        let rs = execIOI db row aval b cval
        listResultStream (map (\val2 -> singleton b val2) rs)
    noConnectionDBStmtExec (EffectFreeTernaryBindingDatabase db) (_, FAtomic (Atom _ [VarExpr a, b, c]), env) stream | not (a `member` env) = do
        row <- stream
        let bval = evalExpr row b
        let cval = evalExpr row c
        let rs = execOII db row a bval cval
        listResultStream (map (\val1 -> singleton a val1) rs)
    noConnectionDBStmtExec (EffectFreeTernaryBindingDatabase db) (_, FAtomic (Atom _ [a, b, c]), _) stream = do
        row <- stream
        let aval = evalExpr row a
        let bval = evalExpr row b
        let cval = evalExpr row c
        let rs = execIII db row aval bval cval
        listResultStream (map (\() -> mempty) rs)
    noConnectionDBStmtExec _ (_, FInsert (Lit Pos _), _) _ =
        return mempty
    noConnectionDBStmtExec _ (_, FInsert (Lit Neg _), _) _ =
        return mempty

    noConnectionDBStmtExec _ qu _ = error ("noConnectionDBStmtExec: unsupported Formula " ++ show qu)

instance EffectfulTernaryBinding a => INoConnectionDatabase2 (EffectfulTernaryBindingDatabase a) where
    type NoConnectionQueryType (EffectfulTernaryBindingDatabase a) = (Set Var, Formula, Set Var)
    type NoConnectionRowType (EffectfulTernaryBindingDatabase a) = MapResultRow
    noConnectionDBStmtExec (EffectfulTernaryBindingDatabase db) (_,  FAtomic (Atom _ [VarExpr a, VarExpr b, VarExpr c]), env) stream | not (a `member` env || b `member` env || c `member` env) = do
        row <- stream
        rs <- liftIO $ execOOOE db row a b c
        listResultStream (map (\(val1,val2,val3) -> fromList [(a, val1),(b,val2),(c,val3)]) rs)
    noConnectionDBStmtExec (EffectfulTernaryBindingDatabase db) (_, FAtomic (Atom _ [a, VarExpr b, VarExpr c]), env) stream | not (b `member` env || c `member` env) = do
        row <- stream
        let aval = evalExpr row a
        rs <- liftIO $ execIOOE db row aval b c
        listResultStream (map (\(val2, val3) -> fromList[( b, val2), (c, val3)]) rs)
    noConnectionDBStmtExec (EffectfulTernaryBindingDatabase db) (_, FAtomic (Atom _ [VarExpr a, b, VarExpr c]), env) stream | not (a `member` env || c `member` env) = do
        row <- stream
        let bval = evalExpr row b
        rs <- liftIO $ execOIOE db row a bval c
        listResultStream (map (\(val1, val3) -> fromList[(a,val1), (c,val3)]) rs)
    noConnectionDBStmtExec (EffectfulTernaryBindingDatabase db) (_, FAtomic (Atom _ [a, b, VarExpr c]), env) stream | not (c `member` env) = do
        row <- stream
        let aval = evalExpr row a
        let bval = evalExpr row b
        rs <- liftIO $ execIIOE db row aval bval c
        listResultStream (map (\val3 -> singleton c val3) rs)
    noConnectionDBStmtExec (EffectfulTernaryBindingDatabase db) (_,  FAtomic (Atom _ [VarExpr a, VarExpr b, c]), env) stream | not (a `member` env || b `member` env) = do
        row <- stream
        let cval = evalExpr row c
        rs <- liftIO $ execOOIE db row a b cval
        listResultStream (map (\(val1,val2) -> fromList [(a, val1),(b,val2)]) rs)
    noConnectionDBStmtExec (EffectfulTernaryBindingDatabase db) (_, FAtomic (Atom _ [a, VarExpr b, c]), env) stream | not (b `member` env) = do
        row <- stream
        let aval = evalExpr row a
        let cval = evalExpr row c
        rs <- liftIO $ execIOIE db row aval b cval
        listResultStream (map (\val2 -> singleton b val2) rs)
    noConnectionDBStmtExec (EffectfulTernaryBindingDatabase db) (_, FAtomic (Atom _ [VarExpr a, b, c]), env) stream | not (a `member` env) = do
        row <- stream
        let bval = evalExpr row b
        let cval = evalExpr row c
        rs <- liftIO $ execOIIE db row a bval cval
        listResultStream (map (\val1 -> singleton a val1) rs)
    noConnectionDBStmtExec (EffectfulTernaryBindingDatabase db) (_, FAtomic (Atom _ [a, b, c]), _) stream = do
        row <- stream
        let aval = evalExpr row a
        let bval = evalExpr row b
        let cval = evalExpr row c
        rs <- liftIO $ execIIIE db row aval bval cval
        listResultStream (map (\() -> mempty) rs)
    noConnectionDBStmtExec (EffectfulTernaryBindingDatabase db) (_, FInsert (Lit Pos (Atom _ [a, b, c])), _) stream = do
        row <- stream
        let aval = evalExpr row a
        let bval = evalExpr row b
        let cval = evalExpr row c
        liftIO $ insertIIIE db row aval bval cval
        return mempty
    noConnectionDBStmtExec (EffectfulTernaryBindingDatabase db) (_, FInsert (Lit Neg (Atom _ [a, b, c])), _) stream = do
        row <- stream
        let aval = evalExpr row a
        let bval = evalExpr row b
        let cval = evalExpr row c
        liftIO $ deleteIIIE db row aval bval cval
        return mempty

    noConnectionDBStmtExec _ qu _ = error ("noConnectionDBStmtExec: unsupported Formula " ++ show qu)


data BinaryBoolean = BinaryBoolean String String String CastType CastType (ResultValue -> ResultValue -> Bool)

instance Binding BinaryBoolean where
    dbName (BinaryBoolean n _ _ _ _ _) = n
    bindingPred (BinaryBoolean _ ns n t1 t2 _) = Pred (PredName [ns] n) (PredType ObjectPred [PTKeyI t1, PTKeyI t2])

instance BinaryBinding BinaryBoolean where
    supportII _ = True
    supportIO _ = False
    supportOO _ = False
    supportOI _ = False
    supportInsertII _ = False
    supportDeleteII _ = False

instance EffectFreeBinaryBinding BinaryBoolean where
    execII (BinaryBoolean _ _ _ _ _ func) _ val1 val2 =
      if func val1 val2
        then [mempty]
        else []


data UnaryFunction = UnaryFunction String String String CastType CastType (ResultValue -> ResultValue)

instance Binding UnaryFunction where
    dbName (UnaryFunction n _ _ _ _ _) = n
    bindingPred (UnaryFunction _ ns n t1 t2 _) = Pred (PredName [ns] n) (PredType PropertyPred [PTKeyI t1, PTPropIO t2])

instance BinaryBinding UnaryFunction where
    supportII _ = True
    supportIO _ = True
    supportOO _ = False
    supportOI _ = False
    supportInsertII _ = False
    supportDeleteII _ = False

instance EffectFreeBinaryBinding UnaryFunction where
    execII (UnaryFunction _ _ _ _ _ func) _ val1 val2 =
      if func val1 == val2
        then [mempty]
        else []
    execIO (UnaryFunction _ _ _ _ _ func) _ val1 _ =
      [ func val1 ]

data UnaryIso = UnaryIso String String String CastType CastType (ResultValue -> ResultValue) (ResultValue -> ResultValue)

instance Binding UnaryIso where
    dbName (UnaryIso n _ _ _ _ _ _) = n
    bindingPred (UnaryIso _ ns n t1 t2 _ _) = Pred (PredName [ns] n) (PredType PropertyPred [PTKeyIO t1, PTPropIO t2])

instance BinaryBinding UnaryIso where
    supportII _ = True
    supportIO _ = True
    supportOO _ = False
    supportOI _ = True
    supportInsertII _ = False
    supportDeleteII _ = False

instance EffectFreeBinaryBinding UnaryIso where
    execII (UnaryIso _ _ _ _ _ func _) _ val1 val2 =
      if func val1 == val2
        then [mempty]
        else []
    execIO (UnaryIso _ _ _ _ _ func _) _ val1 _ =
      [ func val1 ]
    execOI (UnaryIso _ _ _ _ _ _ g) _ _ val2 =
      [ g val2 ]

data BinaryFunction = BinaryFunction String String String CastType CastType CastType (ResultValue -> ResultValue -> ResultValue)

instance Binding BinaryFunction where
    dbName (BinaryFunction n _ _ _ _ _ _) = n
    bindingPred (BinaryFunction _ ns n t1 t2 t3 _) = Pred (PredName [ns] n) (PredType PropertyPred [PTKeyI t1, PTKeyI t2, PTPropIO t3])

instance TernaryBinding BinaryFunction where
    supportIII _ = True
    supportIIO _ = True
    supportIOO _ = False
    supportIOI _ = False
    supportOII _ = False
    supportOIO _ = False
    supportOOO _ = False
    supportOOI _ = False
    supportInsertIII _ = False
    supportDeleteIII _ = False

instance EffectFreeTernaryBinding BinaryFunction where
    execIII (BinaryFunction _ _ _ _ _ _ func) _ val1 val2 val3 =
      if func val1 val2 == val3
        then [mempty]
        else []
    execIIO (BinaryFunction _ _ _ _ _ _ func) _ val1 val2 _ =
      [ func val1 val2 ]

data UnaryProcedure = UnaryProcedure String String String CastType (ResultValue -> IO ())

instance Binding UnaryProcedure where
    dbName (UnaryProcedure n _ _ _ _) = n
    bindingPred (UnaryProcedure _ ns n t1 _) = Pred (PredName [ns] n) (PredType ObjectPred [PTKeyI t1])

instance UnaryBinding UnaryProcedure where
    supportI _ = True
    supportO _ = False
    supportInsertI _ = False
    supportDeleteI _ = False

instance EffectfulUnaryBinding UnaryProcedure where
    execIE (UnaryProcedure _ _ _ _ func) _ val1 = do
      func val1
      return [mempty]

data BinaryParamIso = BinaryParamIso String String String CastType CastType CastType (ResultValue -> ResultValue -> ResultValue) (ResultValue -> ResultValue -> ResultValue)

instance Binding BinaryParamIso where
  dbName (BinaryParamIso n _ _ _ _ _ _ _) = n
  bindingPred (BinaryParamIso _ ns n t1 t2 t3 _ _) = Pred (PredName [ns] n) (PredType PropertyPred [PTKeyIO t1, PTKeyI t2, PTPropIO t3])

instance TernaryBinding BinaryParamIso where
    supportIII _ = True
    supportIIO _ = True
    supportIOO _ = False
    supportIOI _ = False
    supportOII _ = True
    supportOIO _ = False
    supportOOO _ = False
    supportOOI _ = False
    supportInsertIII _ = False
    supportDeleteIII _ = False


instance EffectFreeTernaryBinding BinaryParamIso where
    execIII (BinaryParamIso _ _ _ _ _ _ func _) _ a b c =
        if func a b == c
            then [mempty]
            else []
    execIIO (BinaryParamIso _ _ _ _ _ _ func _) _ a b _ =
        [func a b]
    execOII (BinaryParamIso _ _ _ _ _ _ _ g) _ _ b c =
        [g c b]

data BinaryIso = BinaryIso String String String CastType CastType CastType (ResultValue -> ResultValue -> ResultValue) (ResultValue -> ResultValue -> ResultValue) (ResultValue -> ResultValue -> ResultValue)

instance Binding BinaryIso where
  dbName (BinaryIso n _ _ _ _ _ _ _ _) = n
  bindingPred (BinaryIso _ ns n t1 t2 t3 _ _ _) = Pred (PredName [ns] n) (PredType PropertyPred [PTKeyIO t1, PTKeyIO t2, PTPropIO t3])

instance TernaryBinding BinaryIso where
    supportIII _ = True
    supportIIO _ = True
    supportIOO _ = False
    supportIOI _ = True
    supportOII _ = True
    supportOIO _ = False
    supportOOO _ = False
    supportOOI _ = False
    supportInsertIII _ = False
    supportDeleteIII _ = False


instance EffectFreeTernaryBinding BinaryIso where
    execIII (BinaryIso _ _ _ _ _ _ func _ _) _ a b c =
        if func a b == c
            then [mempty]
            else []
    execIIO (BinaryIso _ _ _ _ _ _ func _ _) _ a b _ =
        [func a b]
    execIOI (BinaryIso _ _ _ _ _ _ _ _ g) _ a _ c =
        [g a c]
    execOII (BinaryIso _ _ _ _ _ _ _ _ h) _ _ b c =
        [h b c]

data BinaryMono = BinaryMono String String String CastType CastType CastType (ResultValue -> ResultValue -> ResultValue) (ResultValue -> ResultValue -> Maybe ResultValue) (ResultValue -> ResultValue -> Maybe ResultValue)

instance Binding BinaryMono where
  dbName (BinaryMono n _ _ _ _ _ _ _ _) = n
  bindingPred (BinaryMono _ ns n t1 t2 t3 _ _ _) = Pred (PredName [ns] n) (PredType PropertyPred [PTKeyIO t1, PTKeyIO t2, PTPropIO t3])

instance TernaryBinding BinaryMono where
    supportIII _ = True
    supportIIO _ = True
    supportIOO _ = False
    supportIOI _ = True
    supportOII _ = True
    supportOIO _ = False
    supportOOO _ = False
    supportOOI _ = False
    supportInsertIII _ = False
    supportDeleteIII _ = False


instance EffectFreeTernaryBinding BinaryMono where
    execIII (BinaryMono _ _ _ _ _ _ func _ _) _ a b c =
        if func a b == c
            then [mempty]
            else []
    execIIO (BinaryMono _ _ _ _ _ _ func _ _) _ a b _ =
      [func a b]
    execIOI (BinaryMono _ _ _ _ _ _ _ _ g) _ a _ c =
      case g a c of
        Just b -> [b]
        Nothing -> []
    execOII (BinaryMono _ _ _ _ _ _ _ _ h) _ _ b c =
      case h b c of
        Just a -> [a]
        Nothing -> []
