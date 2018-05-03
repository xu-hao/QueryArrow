
import Distribution.Simple
import Distribution.PackageDescription
import System.Directory
import System.Process
import System.IO

replace :: Eq a => [a] -> [a] -> [a] -> [a]
replace [] _ _ = []
replace s@(h : t) find repl =
    if take (length find) s == find
        then repl ++ replace (drop (length find) s) find repl
        else h : replace t find repl

main = defaultMainWithHooks simpleUserHooks {
            preBuild = \_ _ -> do
                createDirectoryIfMissing True "gen"
                createDirectoryIfMissing True "gen/SQL"
                copyFile "schema.sql" "gen/schema.sql"
                add <- readFile "phatom.sql"
                withFile "gen/schema.sql" AppendMode $ \handle ->
                    hPutStrLn handle add
                return emptyHookedBuildInfo,
            postBuild = \_ _ _ _ -> do
                callCommand ".stack-work/dist/x86_64-linux/Cabal-1.24.2.0/build/schema_parser_main/schema_parser_main"
                createDirectoryIfMissing True "etc"
                createDirectoryIfMissing True "etc/QueryArrow"
                createDirectoryIfMissing True "etc/QueryArrow/gen"
                createDirectoryIfMissing True "etc/QueryArrow/gen/SQL"
                copyFile "gen/ICATGen.yaml" "etc/QueryArrow/gen/ICATGen.yaml"
                copyFile "gen/SQL/ICATGen.yaml" "etc/QueryArrow/gen/SQL/ICATGen.yaml"
                copyFile "rewriting-plugin-common.rules" "etc/QueryArrow/rewriting-plugin-common.rules"
                copyFile "rewriting-plugin-gen.rules" "etc/QueryArrow/rewriting-plugin-gen.rules"
                conf <- readFile "tdb-plugin-gen.yaml"
                writeFile "etc/QueryArrow/tdb-plugin-gen-abs.yaml" (replace conf "../QueryArrow-gen" "/etc/QueryArrow")
      }
