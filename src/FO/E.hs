module FO.E where

import FO.Data
import FO.Parser
import FO.Config

import System.IO.Temp
import GHC.IO.Handle
import System.Process
import Data.List (isInfixOf, intercalate)

data E = E String Int Int

getVerifier :: VerificationInfo -> IO TheoremProver
getVerifier (VerificationInfo _ path _ cpu memory)= return ( TheoremProver (E path (round cpu) (round memory)))

instance TheoremProver_ E where
    prove (E proverpath cpulimit memorylimit) rules formula = do
        let tptp3 = toTPTP3 rules formula
        withTempFile "/tmp" "test.tptp" $ \filepath handle -> do
            hClose handle
            writeFile filepath tptp3
            (code, out, err) <- readProcessWithExitCode proverpath (["--tptp3-in", "--tptp3-out", "--cpu-limit="++ show cpulimit, "--memory-limit="++show memorylimit, filepath]) ""
            putStrLn (show code)
            putStrLn out
            putStrLn err
            -- let out = "SZS status Theorem" :: String -- putStrLn out
            return (if "SZS status Theorem" `isInfixOf` out
                then Just True
                else if "SZS status CounterSatisfiable" `isInfixOf` out
                    then Just False
                    else Nothing)
