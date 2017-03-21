{-# LANGUAGE StaticPointers, ExistentialQuantification, ScopedTypeVariables, GADTs, ConstraintKinds, FlexibleInstances, TemplateHaskell #-}

module QueryArrow.Distributed.Serializable.TH where

import QueryArrow.Distributed.Serializable
import Language.Haskell.TH
import Language.Haskell.TH.Syntax
import QueryArrow.Data.Some
import Data.Typeable
import Data.Constraint
import GHC.StaticPtr

-- instance DistributedSerializable Unconstrained Int64 where
--   getStaticPtr _ = static getSomeUnconstrainedInt64
--
-- getSomeUnconstrainedInt64 :: Get (Some (DistributedSerializable Unconstrained))
-- getSomeUnconstrainedInt64 = do
--   i64 <- sGet
--   return (Some (i64 :: Int64))
-- instance DistributedSerializable Show Identity (ExceptT String IO) Int64 where
--   getStaticPtr _ = static getSomeShowInt64
--
-- getSomeShowInt64 :: BSL.ByteString -> ExceptT String IO (Some (DistributedSerializable Show Identity (ExceptT String IO)), BSL.ByteString )
-- getSomeShowInt64 bs = do
--   (i64, b1) <- sGet bs
--   return (Some (i64 :: Int64), b1)

deriveDistributedSerializable :: Name -> Name -> Name -> Name -> Name -> Q [Dec]
deriveDistributedSerializable constrname putname getname bname tyname = do
  let toIdentifier = map (\ ch -> case ch of
                            '.' -> '_'
                            _ -> ch)
  let constr = conT constrname
  let ty = conT tyname
  let get = conT getname
  let put = conT putname
  let b = conT bname
  let tybase = toIdentifier (showName tyname)
  let constrbase = toIdentifier (showName constrname)
  let putbase = toIdentifier (showName putname)
  let getbase = toIdentifier (showName getname)
  let bbase = toIdentifier (showName bname)
  let getfuncname = mkName ("getSome" ++ constrbase ++ putbase ++ getbase ++ bbase ++ tybase)
  let bs = mkName ("bs")
  t <- [t|GetTarget $(get) -> $(get) (Some (DistributedSerializable $(constr) $(put) $(get) $(b)), GetTarget $(get))|]
  let sig = SigD getfuncname t
  e <- [|do
          (a, bs1) <- sGet $(varE bs)
          return (Some (a :: $(ty)), bs1)|]
  let fd = FunD getfuncname [Clause [VarP bs] (NormalB e) []]
  instd <- [d|
        instance DistributedSerializable $(constr) $(put) $(get) $(b) $(ty) where
          getStaticPtr _ = static $(varE getfuncname)|]
  return (sig : fd : instd)
