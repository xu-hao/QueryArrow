module FO.CVC4 where

import FO.Data
import FO.Parser
import FO.Config

import System.IO.Temp
import GHC.IO.Handle
import System.Process
import Data.List (isInfixOf, intercalate)

data CVC4 = CVC4 String Int Int Int

getVerifier :: VerificationInfo -> IO TheoremProver
getVerifier (VerificationInfo  _ path _ cpu memory _ _)= return (TheoremProver (CVC4 path (round (cpu * 1000)) (round (cpu * 1000)) (round memory)))

instance TheoremProver_ CVC4 where
    prove (CVC4 proverpath cpulimit cpulimit2 _) rules formula = do
        let tptp3 = toTPTP3 rules formula
        withTempFile "/tmp" "test.tptp" $ \filepath handle -> do
            hClose handle
            writeFile filepath tptp3
            (code, out, err) <- readProcessWithExitCode proverpath (["--lang", "tptp", "--tlimit=" ++ show cpulimit, filepath]) ""
            putStrLn (show code)
            putStrLn out
            putStrLn err
            -- let out = "SZS status Theorem" :: String -- putStrLn out
            if "SZS status Theorem" `isInfixOf` out
                then return (Just True)
                else if "SZS status CounterSatisfiable" `isInfixOf` out
                    then return (Just False)
                    else do -- try to find a finite model
                        putStrLn "trying to find a finite model"
                        (code, out, err) <- readProcessWithExitCode proverpath (["--lang", "tptp", "--tlimit=" ++ show cpulimit2, filepath, "--finite-model-find", "--dump-models"]) ""
                        putStrLn (show code)
                        putStrLn out
                        putStrLn err
                        return (if "SZS status CounterSatisfiable" `isInfixOf` out
                            then Just False
                            else Nothing)
