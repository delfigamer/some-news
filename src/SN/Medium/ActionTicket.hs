module SN.Medium.ActionTicket
    ( ActionTicket(..)
    ) where

import Data.Maybe
import SN.Data.HEq1
import SN.Ground.Interface

data ActionTicket
    = forall a. ActionTicket (Action a)
    | UploadTicket (Reference Article) (Reference User)

instance Eq ActionTicket where
    ActionTicket a == ActionTicket b = isJust $ heq a b
    _ == _ = False
