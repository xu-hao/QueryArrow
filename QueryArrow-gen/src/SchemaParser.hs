{-# LANGUAGE TemplateHaskell #-}

module SchemaParser where

import qualified Text.Parsec.Token as T
import Text.ParserCombinators.Parsec
import Control.Applicative ((*>), (<*), (<$>), (<*>))
import QueryArrow.FO.Data
import QueryArrow.FO.Utils
import QueryArrow.SQL.SQL
import System.IO.Unsafe
import Data.List (partition)
import Data.Char
import QueryArrow.ListUtils

lexer = T.makeTokenParser T.LanguageDef {
    T.commentStart = "/*",
    T.commentEnd = "*/",
    T.commentLine = "--",
    T.nestedComments = False,
    T.identStart = letter <|> char '_',
    T.identLetter = alphaNum <|> char '_',
    T.opStart = oneOf "#\'",
    T.opLetter = oneOf "",
    T.reservedNames = ["create", "table", "sequence", "not", "null", "varchar", "DEFAULT","int", "INTEGER", "INT64TYPE"],
    T.reservedOpNames = [],
    T.caseSensitive = True
}

identifier = T.identifier lexer
reserved = T.reserved lexer
reservedOp = T.reservedOp lexer
parens = T.parens lexer
integer = T.integer lexer
stringp = char '\'' *> many (noneOf "\'") <* char '\'' <* whiteSpace
comma = T.comma lexer
semi = T.semi lexer
symbol = T.symbol lexer
whiteSpace = T.whiteSpace lexer

data Stmt2 = Stmt String [ColDef] deriving Show
data ColDef = ColDef String ColType [Constraint] deriving Show
data ColType = Text
             | Number deriving Show
data Constraint = NotNull
                | DI Integer
                | DS String deriving Show


prog :: Parser [Stmt2]
prog = do
        whiteSpace
        s <- stmts
        eof
        return s

ifdef :: Parser ()
ifdef = do
    reservedOp "#"
    choice [symbol "if", symbol "ifdef"]
    manyTill anyChar (try (reservedOp "#" *> symbol "endif" *> return ()))
    return ()

index :: Parser ()
index = do
    try $ do
        (reserved "create" <|> reserved "CREATE")
        optional (reserved "unique" <|> reserved "UNIQUE")
        (reserved "index" <|> reserved "INDEX")
    manyTill anyChar (try semi)
    return ()

stmts :: Parser [Stmt2]
stmts = many (pre *> stmt <* pre) where
    pre = many (choice [ifdef, index])

stmt :: Parser Stmt2
stmt =
      create_stmt -- <|> create_sequence

{- create_sequence :: Parser Stmt2
create_sequence = do
      try $ do
          reserved "create"
          reserved "sequence"
      id <- identifier
      let coldefs = [ColDef "nextval()" Number []]
      identifier
      identifier
      integer
      identifier
      identifier
      integer
      semi
      return (Stmt id coldefs)
-}
create_stmt :: Parser Stmt2
create_stmt = do
      try $ do
          (reserved "create" <|> reserved "CREATE")
          (reserved "table" <|> reserved "TABLE")
      id <- identifier
      coldefs <- parens col_def_list
      semi
      return (Stmt id coldefs)

col_def_list :: Parser [ColDef]
col_def_list =
      sepBy col_def comma

col_def :: Parser ColDef
col_def =
    ColDef <$> identifier <*> typep <*> constraints

typep :: Parser ColType
typep = reserved "integer" *> return Number
    <|> reserved "INTEGER" *> return Number
    <|> reserved "INT64TYPE" *> return Number
    <|> reserved "bigint" *> return Number
    <|> reserved "int" *> return Number
    <|> reserved "varchar" *> parens integer *> return Text
    <|> reserved "character" *> reserved "varying" *> parens integer *> return Text

constraints :: Parser [Constraint]
constraints = many constraint

constraint :: Parser Constraint
constraint = (reserved "not" <|> reserved "NOT") *> (reserved "null" <|> reserved "NULL") *> return NotNull
         <|> try(reserved "DEFAULT" *> (DI <$> integer))
         <|> reserved "DEFAULT" *> (DS <$> stringp)

findAllKeys :: String -> [ColDef] -> ([ColDef], [ColDef])
findAllKeys prefix coldefs =
    case prefix of
        "SERVER_LOAD" -> (coldefs, []) -- special case for server load
        "SERVER_LOAD_DIGEST" -> (coldefs, []) -- special case for server load digest
        "QUOTA_USAGE" -> partition (\(ColDef key0 _ _) ->
                                    let key1 = map toUpper key0 in
                                          "RESC_ID" == key1 || "USER_ID" == key1)  coldefs  -- specical case for quota usage
        "DATA" -> partition (\(ColDef key0 _ _) ->
                                    let key1 = map toUpper key0 in
                                          "DATA_ID" == key1 || "RESC_ID" == key1)  coldefs  -- specical case for data
        "USER_PASSWORD" -> partition (\(ColDef key0 _ _) ->
                                    let key1 = map toUpper key0 in
                                          "USER_ID" == key1 || "RCAT_PASSWORD" == key1)  coldefs  -- special case for user password
        "USER_AUTH" -> partition (\(ColDef key0 _ _) ->
                                    let key1 = map toUpper key0 in
                                          "USER_ID" == key1 || "USER_AUTH_NAME" == key1)  coldefs  -- specical case for user auth
        "TICKET_ALLOWED_HOSTS" -> partition (\(ColDef key0 _ _) ->
                                    let key1 = map toUpper key0 in
                                          "TICKET_ID" == key1 || "HOST" == key1)  coldefs  -- specical case for user auth
        "TICKET_ALLOWED_GROUPS" -> partition (\(ColDef key0 _ _) ->
                                    let key1 = map toUpper key0 in
                                          "TICKET_ID" == key1 || "GROUP_NAME" == key1)  coldefs  -- specical case for user auth
        "TICKET_ALLOWED_USERS" -> partition (\(ColDef key0 _ _) ->
                                    let key1 = map toUpper key0 in
                                          "TICKET_ID" == key1 || "USER_NAME" == key1)  coldefs  -- specical case for user auth
        "GRID_CONFIGURATION" -> (coldefs, [])  -- specical case for user auth
        _ ->
            let par@(key, _) = partition (\(ColDef key0 _ _) -> map toUpper key0 == prefix ++ "_ID")  coldefs in
                if null key
                    then partition (\(ColDef key0 _ _) -> let key1 = (map toUpper key0) in
                                                                              (endswith "_ID" key1)) coldefs
                    else par

findAllNotNulls :: [ColDef] -> [ColDef]
findAllNotNulls = filter (\(ColDef _ _ cs) -> any (\c -> case c of NotNull -> True ; _ -> False) cs)

extractPrefix tablename =
    let predname0 = drop 2 (map toUpper tablename) in
        replace  "_MAIN" "" predname0

prefixToPredName prefix =
    let predname1 = prefix ++ "_OBJ" in
        map toUpper predname1

colNameToPredName prefix colname =
  let predname0 = map toUpper colname
      predname = if startswith "R_" predname0 then drop 2 predname0 else predname0 in
      (if startswith prefix predname then "" else prefix ++ "_") ++ predname

colTypeToQExp :: ColType -> CastType
colTypeToQExp keytype = case keytype of
                                    Number -> Int64Type
                                    Text -> TextType
generateICATDef :: Stmt2 -> [Pred]
generateICATDef (Stmt tablename coldefs) = do
    let prefix = extractPrefix tablename
    if prefix `elem` []
        then
            []
        else do
            let predname = prefixToPredName prefix
            -- find all keys
            let (keys, props) = findAllKeys prefix coldefs
            let keysq = map (\(ColDef _ keytype _) -> PTKeyIO (colTypeToQExp keytype)) keys
            let q1 = Pred (UQPredName predname) (PredType ObjectPred keysq)
            let propPred (ColDef key2 keytype2 _) = Pred (UQPredName (colNameToPredName prefix key2)) (PredType PropertyPred (keysq ++  [PTPropIO (colTypeToQExp keytype2)]))
            let propPreds = map propPred props
            q1 : propPreds

generateICATDefs :: [Stmt2] -> [Pred]
generateICATDefs = concatMap generateICATDef

generateICATMapping :: Stmt2 -> [SQLMapping]
generateICATMapping (Stmt tablename coldefs) = do
    let prefix = extractPrefix tablename
    if prefix `elem` []
        then
            []
        else do
            let predname = prefixToPredName prefix
            -- find all keys
            let (keys, props) = findAllKeys prefix coldefs
            let tn = map toLower tablename
            let v = SQLVar "1"
            let t = OneTable tn v
            let keysq = map (\(ColDef key _ _) -> SQLQualifiedCol v key) keys
            let q1 = SQLMapping predname t keysq
            let propPred (ColDef key2 _ _) = SQLMapping (colNameToPredName prefix key2) t (keysq ++ [SQLQualifiedCol v key2])
            let propPreds = map propPred props
            q1 : propPreds

generateICATMappings :: [Stmt2] -> [SQLMapping]
generateICATMappings = concatMap generateICATMapping

{- generateICATSchema :: Stmt2 -> Q Exp
generateICATSchema (Stmt tablename coldefs) = do
    let prefix = extractPrefix tablename
    let (keys, _) = findAllKeys prefix coldefs
    let tn = map toLower tablename
    let keysq = foldr (\(ColDef key _ _) q -> [| $(stringQ key) : $q |]) [| [] |] keys
    let colsq = foldr (\(ColDef key _ _) q -> [| $(stringQ key) : $q |]) [| [] |] coldefs
    [| ($(stringQ tn), ($colsq, $keysq)) |]

generateICATSchemas :: [Stmt2] -> Q Exp
generateICATSchemas = foldr (\stmt q2 -> [| $(generateICATSchema stmt) : $q2 |] ) [|[]|] -}

schema :: ([Pred], [SQLMapping])
schema =
    let path = "gen/schema.sql"
        file = unsafePerformIO $ readFile path in
        case runParser prog () path file of
            Left err -> error (show err)
            Right ast -> (generateICATDefs ast, generateICATMappings ast)
