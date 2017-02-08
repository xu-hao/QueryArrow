module QueryArrow.Control.Monad.Logger.HSLogger where

import Control.Monad.Logger
import System.Log.FastLogger
import System.Log.Logger
import qualified Data.Text as T
import Data.Text.Encoding


instance MonadLogger IO where
  monadLoggerLog loc logsource loglevel msg = do
      let priority = case loglevel of
                          LevelDebug -> DEBUG
                          LevelInfo -> INFO
                          LevelWarn -> WARNING
                          LevelError -> ERROR
                          LevelOther _ -> NOTICE
      logM (show logsource) priority (T.unpack (decodeUtf8 (fromLogStr (toLogStr msg))))

instance MonadLoggerIO IO where
  askLoggerIO = return monadLoggerLog
