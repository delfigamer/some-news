{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TemplateHaskell #-}

module HEq1
    ( HEq1(..)
    , (:~:)(..)
    , declareHEq1Instance
    ) where

import Language.Haskell.TH
import Type.Reflection ((:~:)(..))

class HEq1 f where
    heq :: f a -> f b -> Maybe (a :~: b)

declareHEq1Instance typeName = [d|
        instance HEq1 $(conT typeName) where
            heq = $(heqTemplate typeName)
    |]

heqTemplate :: Name -> Q Exp
heqTemplate typeName = do
    DataD _ _ _ _ cons _ <- reify typeName >>= \case
        TyConI typeDecl@DataD {} -> return typeDecl
        _ -> fail "heqTemplate: type name expected"
    nx <- newName "x"
    ny <- newName "y"
    yMatches <- mapM (heqTemplateConMatch $ VarE ny) cons
    return $
        LamE [VarP nx, VarP ny] $
            CaseE (VarE nx) yMatches

heqTemplateConMatch :: Exp -> Con -> Q Match
heqTemplateConMatch bsource (GadtC [conName] [] _) = do
    return $
        Match (ConP conName [])
            (NormalB $ CaseE bsource
                [ Match (ConP conName [])
                    (NormalB $ AppE (ConE 'Just) (ConE 'Refl))
                    []
                , Match WildP
                    (NormalB $ ConE 'Nothing)
                    []
                ])
        []
heqTemplateConMatch bsource (GadtC [conName] fields _) = do
    varsA <- mapM (const $ newName "a") fields
    varsB <- mapM (const $ newName "b") fields
    let abEquality =
            foldr1
                (\e1 e2 -> InfixE (Just $ e1) (VarE '(&&)) (Just $ e2))
                (zipWith
                    (\va vb -> InfixE (Just $ VarE va) (VarE '(==)) (Just $ VarE vb))
                    varsA varsB)
    return $
        Match (ConP conName (map VarP varsA))
            (NormalB $ CaseE bsource
                [ Match (ConP conName (map VarP varsB))
                    (GuardedB [(
                        NormalG abEquality,
                        AppE (ConE 'Just) (ConE 'Refl))])
                    []
                , Match WildP
                    (NormalB $ ConE 'Nothing)
                    []
                ])
        []
heqTemplateConMatch bsource (ForallC _ _ con) = heqTemplateConMatch bsource con
heqTemplateConMatch _ con = fail $ "unsupported constructor syntax: " ++ show con
