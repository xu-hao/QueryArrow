
import Distribution.Simple
import Distribution.PackageDescription
import System.Directory
import System.Process
import System.IO

main = defaultMainWithHooks simpleUserHooks {
            preConf = \_ _ -> do
                createDirectoryIfMissing True "gen"
                createDirectoryIfMissing True "gen/SQL"
                copyFile "schema.sql" "gen/schema.sql"
                add <- readFile "phatom.sql"
                withFile "gen/schema.sql" AppendMode $ \handle ->
                    hPutStrLn handle add
                return emptyHookedBuildInfo,
            postBuild = \_ _ _ _ ->
                callCommand "stack exec schema_parser_main",
            postClean = \_ _ _ _ ->
                removeDirectoryRecursive "gen"
      }
