{-# LANGUAGE MonadComprehensions, ForeignFunctionInterface #-}
module FFI where
import FO
import FO.Data
import QueryPlan
import FO.Parser
import FO.Config
import Parser
import DBQuery
import ResultStream
-- import Plugins
import FO.E
import SQL.HDBC.PostgreSQL


import Prelude hiding (lookup)
import Data.Map.Strict (empty, lookup, fromList)
import Control.Monad (when)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.State.Strict
import Control.Monad.Trans.Class (lift)
import Text.ParserCombinators.Parsec (runParser)
import Data.Aeson
import Control.Applicative ((<$>))
import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as B8
import Data.Either.Utils (fromEither)
import Data.Maybe (fromMaybe)
import Foreign
import Foreign.C

foreign export ccall run3 :: Ptr () -> CString -> Ptr() -> Ptr () -> CInt -> FunPtr SuccessFun -> FunPtr ErrorFun -> IO ()
foreign export ccall init3 :: CString -> CString -> IO (Ptr (Ptr ()))
foreign import ccall "dynamic" mkFunSuccess :: FunPtr SuccessFun -> SuccessFun
foreign import ccall "dynamic" mkFunError :: FunPtr ErrorFun -> ErrorFun


type DB3 = (Database DBAdapterMonad MapResultRow, Query -> String, Insert -> String, Completion, Completion, TheoremProver, [Input])

init3 :: CString -> CString -> IO (Ptr (Ptr ()))
init3 ps0c ps1c = do
    ps0 <- peekCString ps0c
    case eitherDecode (B8.pack ps0) of
        Left err ->
            castPtr <$> toCStringArray ["Error", show err]
        Right ps0 -> do
            ps1 <- peekCString ps1c
            case eitherDecode (B8.pack ps1) of
                Left err ->
                    castPtr <$> toCStringArray ["Error", show err]
                Right ps1 -> do
                    (db,p1,p2) <- getDB ps0
                    let predmap = constructPredMap [db]
                    v <- getVerifier ps1
                    d0 <- B8.unpack <$> B.readFile (rule_file_path ps1)
                    let rules = parseTPTP predmap d0
                    cadd <- loadCompletion predmap (add_set ps1)
                    cdel <- loadCompletion predmap (delete_set ps1)
                    ptr <- newStablePtr (db,p1,p2,cadd, cdel, v::TheoremProver,rules::[Input])
                    flac <- newCString "Success"
                    arr <- mallocArray0 2
                    pokeArray arr [castPtr flac, castStablePtrToPtr ptr]
                    return arr


-- DB Handle, Query, State, Num, Callback, Error
-- Callback :: State -> Num -> Rows -> Continue
-- Continue :: >0 batch 0 = stop
type SuccessFun = Ptr () -> Ptr () -> CInt -> Ptr CString -> IO CInt
type ErrorFun = Ptr () -> Ptr () -> Ptr CString -> IO ()
run3 :: Ptr () -> CString -> Ptr() -> Ptr () -> CInt -> FunPtr SuccessFun -> FunPtr ErrorFun -> IO ()
run3 dbhandle queryc handler state (CInt n) succFun errFun = do
            (db3, printFunc, a, cadd, cdel, verifier, rules) <- deRefStablePtr (castPtrToStablePtr dbhandle) :: IO DB3
            let succFunh = mkFunSuccess succFun
            let errFunh = mkFunError errFun
            query <- peekCString queryc
            evalStateT (dbWithSession [db3] $ do
                        let predmap = constructPredMap [db3]
                        case runParser progp predmap "" query of
                            Left err -> liftIO $ do
                                errc <- toCStringArray [show err]
                                errFunh handler state errc
                            Right (Q qu@(Query vars _), _) -> do
                                liftIO $ print (printFunc qu)
                                rs <- execQuery qu
                                res <- lift $ lift $ runResultStream rs (\row (state, n, batch) -> liftIO $ do
                                    let fla = map (\v -> fromMaybe "null" (show <$> lookup v row)) vars
                                    let batch' = batch ++ [fla]
                                    if fromIntegral (length batch') /= n
                                        then return (Right (state, n, batch'))
                                        else do
                                            batchc <- toCStringArray (concat batch')
                                            CInt i <- succFunh handler state (CInt n) batchc
                                            freeCStringArray batchc
                                            return (if i == 0
                                                then Left (state, i, [])
                                                else Right (state, i, []))) (state, n, [])
                                let (state, i, batch) = fromEither res
                                when (length batch /= 0) $ liftIO $ do
                                    batchc <- toCStringArray (concat batch)
                                    succFunh handler state (CInt (fromIntegral (length batch))) batchc
                                    freeCStringArray batchc
                            Right (D atoms cond, _) -> do
                                let qu = transformDeletion cdel atoms cond
                                insert handler state  verifier rules succFunh qu
                            Right (I qu, _) ->
                                insert handler state  verifier rules succFunh qu) (DBAdapterState empty)

defaultInsertMap :: Database m row -> InsertMap
defaultInsertMap (Database db) = 
    let preds = getPreds db in
        fromList (zip preds (replicate (length preds) ([0],[0])))

insert handler state verifier rules succFunh qu = do
    [db3] <- getDBsFromDBSession
    rs <- execInsert verifier rules (defaultInsertMap db3) qu
    rows <- lift $ lift $ getAllResultsInStream rs
    rowc <- liftIO $ toCStringArray []
    liftIO $ succFunh handler state (CInt 1) rowc
    liftIO $ freeCStringArray rowc
    return ()

freeCStringArray :: Ptr CString -> IO ()
freeCStringArray arr = do
    strsc <- peekArray0 nullPtr arr
    mapM_ free strsc
    free arr

toCStringArray :: [String] -> IO (Ptr CString)
toCStringArray fla = do
            flac <- mapM newCString fla
            arr <- mallocArray0 (length fla + 1)
            pokeArray0 nullPtr arr flac
            return arr

loadCompletion :: PredMap -> String -> IO Completion
loadCompletion pmap path = do
    a <- readFile path
    let inputs = parseTPTP pmap a
    let formulas = map (\(_,_,a)->a) inputs
    return (map formulaToCompletion formulas)

formulaToCompletion (Disjunction (Not (Atomic head) : tails)) | all (\f-> case f of
    Atomic _->True
    _ -> False) tails = (head, map (\f-> case f of
        Atomic a->a) tails)
formulaToCompletion _ = error "malformatted completion rule"
