module QueryArrow.FFI.GenQuery.Data where

data Cond = Cond String Cond2 deriving (Show)
data Cond2 = EqString String |
            EqInteger Integer |
            NotEqString String |
            NotEqInteger Integer |
            LikeCond String |
            ParentOfCond String |
            OrCond Cond2 Cond2 |
            AndCond Cond2 Cond2 deriving (Show)

data Sel = Sel String Sel2 deriving (Show)
data Sel2 = None | GQOrderDesc | GQOrderAsc | GQCount | GQSum | GQMax | GQMin deriving (Show)

data GenQuery = GenQuery [Sel] [Cond] deriving (Show)
