module JsonInterface.ActionTicket
    ( ActionTicket(..)
    ) where

import Data.Maybe
import HEq1
import Storage

data ActionTicket
    = forall a. ActionTicket (Action a)
    | UploadTicket (Reference Article) (Reference User)

instance Eq ActionTicket where
    ActionTicket a == ActionTicket b = isJust $ heq a b
    _ == _ = False
