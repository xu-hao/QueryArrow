module Control.Monad.Logger.HSLogger where

import Control.Monad.Logger
import System.Log.FastLogger
import System.Log.Logger
import qualified Data.ByteString.Char8 as CS


instance MonadLogger IO where
  monadLoggerLog loc logsource loglevel msg = do
      let priority = case loglevel of
                          LevelDebug -> DEBUG
                          LevelInfo -> INFO
                          LevelWarn -> WARNING
                          LevelError -> ERROR
                          LevelOther _ -> NOTICE
      logM (show logsource) priority (CS.unpack (fromLogStr (toLogStr msg)))

instance MonadLoggerIO IO where
  askLoggerIO = return monadLoggerLog
