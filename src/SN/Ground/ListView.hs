{-# LANGUAGE EmptyCase #-}
{-# LANGUAGE EmptyDataDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module SN.Ground.ListView
    ( ListView(..)
    , OrderDirection(..)
    , ViewFilter(..)
    , ViewOrder(..)
    , withView
    ) where

import Control.Monad.IO.Class
import Data.Int
import Data.List
import Data.Semigroup
import Data.Time.Clock
import SN.Data.HList
import SN.Ground.Types
import SN.Sql.Query

data ListView a = ListView
    { viewOffset :: Int64
    , viewLimit :: Int64
    , viewFilter :: [ViewFilter a]
    , viewOrder :: [(ViewOrder a, OrderDirection)]
    }
deriving instance (Show (ViewFilter a), Show (ViewOrder a)) => Show (ListView a)
deriving instance (Eq (ViewFilter a), Eq (ViewOrder a)) => Eq (ListView a)

data OrderDirection
    = Ascending
    | Descending
    deriving (Show, Eq)

data family ViewFilter a
data family ViewOrder a

withView
    :: (ListableObject a)
    => UTCTime
    -> ListView a
    -> ([RowSource] -> [Condition] -> (FieldName -> [RowOrder]) -> RowRange -> UTCTime -> r)
    -> r
withView time view cont = do
    let demand = foldMap applyFilter (viewFilter view) <> foldMap (uncurry applyOrder) (viewOrder view)
    withQueryDemand demand time $ \joinEndo filterEndo orderEndo -> do
        let joins = nub (appEndo joinEndo [])
        let (joinSources, joinFilters) = unzip $ map (\(JoinDemand source filter) -> (source, filter)) joins
        cont
            joinSources
            (appEndo (mconcat joinFilters <> filterEndo) [])
            (\fieldName -> appEndo orderEndo [Asc fieldName])
            (RowRange (viewOffset view) (viewLimit view))
            time

class ListableObject a where
    applyFilter :: ViewFilter a -> QueryDemand
    applyOrder :: ViewOrder a -> OrderDirection -> QueryDemand

data JoinDemand = JoinDemand RowSource (Endo [Condition])

instance Eq JoinDemand where
    JoinDemand (TableSource t1) _ == JoinDemand (TableSource t2) _ = t1 == t2
    JoinDemand (OuterJoinSource t1 _) _ == JoinDemand (OuterJoinSource t2 _) _ = t1 == t2
    JoinDemand (RecursiveSource t1 _ _ _) _ == JoinDemand (RecursiveSource t2 _ _ _) _ = t1 == t2
    _ == _ = False

newtype QueryDemand = QueryDemand
    { withQueryDemand :: forall r. UTCTime
        -> (Endo [JoinDemand] -> Endo [Condition] -> Endo [RowOrder] -> r)
        -> r
    }

instance Semigroup QueryDemand where
    QueryDemand b1 <> QueryDemand b2 = QueryDemand $ \time cont ->
        b1 time $ \j1 c1 o1 ->
            b2 time $ \j2 c2 o2 ->
                cont (j1 <> j2) (c1 <> c2) (o1 <> o2)

instance Monoid QueryDemand where
    mempty = QueryDemand $ \_ cont -> cont mempty mempty mempty

withCurrentTime :: (UTCTime -> QueryDemand) -> QueryDemand
withCurrentTime inner = QueryDemand $ \time cont -> do
    withQueryDemand (inner time) time cont

demandJoin :: RowSource -> [Condition] -> QueryDemand
demandJoin source cond = QueryDemand $ \_ cont -> do
    let demand = JoinDemand source (Endo (cond ++))
    cont (Endo (demand :)) mempty mempty

demandCondition :: Condition -> QueryDemand
demandCondition cond = QueryDemand $ \_ cont ->
    cont mempty (Endo (cond :)) mempty

demandOrder :: FieldName -> OrderDirection -> QueryDemand
demandOrder field dir = QueryDemand $ \_ cont ->
    cont mempty mempty (Endo (order :))
  where
    order = case dir of
        Ascending -> Asc field
        Descending -> Desc field

inverseDirection :: OrderDirection -> OrderDirection
inverseDirection Ascending = Descending
inverseDirection Descending = Ascending

data instance ViewFilter User
    = FilterUserId (Reference User)
    | FilterUserIsAdmin Bool
    | FilterUserAuthorId (Reference Author)
    | FilterUserAccessKey AccessKey
    deriving (Show, Eq)

data instance ViewOrder User
    = OrderUserName
    | OrderUserSurname
    | OrderUserJoinDate
    | OrderUserIsAdmin
    deriving (Show, Eq)

instance ListableObject User where
    applyFilter (FilterUserId ref) = demandCondition (WhereIs ref "user_id")
    applyFilter (FilterUserIsAdmin True) = demandCondition (Where "user_is_admin")
    applyFilter (FilterUserIsAdmin False) = demandCondition (Where "NOT user_is_admin")
    applyFilter (FilterUserAuthorId ref) = demandCondition (WhereIs ref "a2u_author_id")
        <> demandJoin "sn_author2user" [Where "a2u_user_id = user_id"]
    applyFilter (FilterUserAccessKey key@(AccessKey keyFront _)) =
        demandCondition (WhereIs keyFront "access_key_id")
            <> demandCondition (WhereIs (hashAccessKey key) "access_key_hash")
            <> demandJoin "sn_access_keys" [Where "access_key_user_id = user_id"]
    applyOrder OrderUserName = demandOrder "user_name"
    applyOrder OrderUserSurname = demandOrder "user_surname"
    applyOrder OrderUserJoinDate = demandOrder "user_join_date" . inverseDirection
    applyOrder OrderUserIsAdmin = demandOrder "user_is_admin" . inverseDirection

data instance ViewFilter AccessKey
    deriving (Show, Eq)

data instance ViewOrder AccessKey
    deriving (Show, Eq)

instance ListableObject AccessKey where
    applyFilter = \case {}
    applyOrder = \case {}

data instance ViewFilter Author
    = FilterAuthorId (Reference Author)
    | FilterAuthorUserId (Reference User)
    deriving (Show, Eq)

data instance ViewOrder Author
    = OrderAuthorName
    deriving (Show, Eq)

instance ListableObject Author where
    applyFilter (FilterAuthorId ref) = demandCondition (WhereIs ref "author_id")
    applyFilter (FilterAuthorUserId ref) = demandCondition (WhereIs ref "a2u_user_id")
        <> demandJoin "sn_author2user" [Where "a2u_author_id = author_id"]
    applyOrder OrderAuthorName = demandOrder "author_name"

data instance ViewFilter Category
    = FilterCategoryId (Reference Category)
    | FilterCategoryParentId (Reference Category)
    | FilterCategoryTransitiveParentId (Reference Category)
    deriving (Show, Eq)

data instance ViewOrder Category
    = OrderCategoryName
    deriving (Show, Eq)

instance ListableObject Category where
    applyFilter (FilterCategoryId ref) = demandCondition (WhereIs ref "category_id")
    applyFilter (FilterCategoryParentId ref) = demandCondition (WhereIs ref "category_parent_id")
    applyFilter (FilterCategoryTransitiveParentId ref) = demandJoin (subcategoriesSource ref) [Where "category_id = subcategory_id"]
    applyOrder OrderCategoryName = demandOrder "category_name"

data instance ViewFilter Article
    = FilterArticleId (Reference Article)
    | FilterArticleAuthorId (Reference Author)
    | FilterArticleUserId (Reference User)
    | FilterArticleCategoryId (Reference Category)
    | FilterArticleTransitiveCategoryId (Reference Category)
    | FilterArticlePublishedCurrently
    | FilterArticlePublishedBefore UTCTime
    | FilterArticlePublishedAfter UTCTime
    | FilterArticleTagIds [Reference Tag]
    deriving (Show, Eq)

data instance ViewOrder Article
    = OrderArticleName
    | OrderArticleDate
    | OrderArticleAuthorName
    | OrderArticleCategoryName
    deriving (Show, Eq)

instance ListableObject Article where
    applyFilter (FilterArticleId ref) = demandCondition (WhereIs ref "article_id")
    applyFilter (FilterArticleAuthorId ref) = demandCondition (WhereIs ref "article_author_id")
    applyFilter (FilterArticleUserId ref) = demandCondition (WhereIs ref "a2u_user_id")
        <> demandJoin (OuterJoinSource "sn_author2user" (WhereFieldIs "a2u_author_id" "article_author_id")) []
    applyFilter (FilterArticleCategoryId ref) = demandCondition (WhereIs ref "article_category_id")
    applyFilter (FilterArticleTransitiveCategoryId (Reference "")) = demandCondition (Where "article_category_id IS NULL")
    applyFilter (FilterArticleTransitiveCategoryId ref) = demandJoin (subcategoriesSource ref) [Where "article_category_id = subcategory_id"]
    applyFilter FilterArticlePublishedCurrently = withCurrentTime $ \time -> demandCondition (WhereWith time "article_publication_date <= ?")
    applyFilter (FilterArticlePublishedBefore end) = demandCondition (WhereWith end "article_publication_date < ?")
    applyFilter (FilterArticlePublishedAfter begin) = demandCondition (WhereWith begin "article_publication_date >= ?")
    applyFilter (FilterArticleTagIds []) = mempty
    applyFilter (FilterArticleTagIds tagRefs) = demandCondition (WhereWithList "EXISTS (SELECT * FROM sn_article2tag WHERE a2t_article_id = article_id AND a2t_tag_id IN " tagRefs ")")
    applyOrder OrderArticleName = demandOrder "article_name"
    applyOrder OrderArticleDate = demandOrder "article_publication_date" . inverseDirection
    applyOrder OrderArticleAuthorName = demandOrder "COALESCE(author_name, '')"
        <> pure (demandJoin (OuterJoinSource "sn_authors" (WhereFieldIs "author_id" "article_author_id")) [])
    applyOrder OrderArticleCategoryName = demandOrder "COALESCE(category_name, '')"
        <> pure (demandJoin (OuterJoinSource "sn_categories" (WhereFieldIs "category_id" "article_category_id")) [])

data instance ViewFilter Tag
    = FilterTagId (Reference Tag)
    | FilterTagArticleId (Reference Article)
    deriving (Show, Eq)

data instance ViewOrder Tag
    = OrderTagName
    deriving (Show, Eq)

instance ListableObject Tag where
    applyFilter (FilterTagId ref) = demandCondition (WhereIs ref "tag_id")
    applyFilter (FilterTagArticleId ref) = demandCondition (WhereIs ref "a2t_article_id")
        <> demandJoin "sn_article2tag" [Where "a2t_tag_id = tag_id"]
    applyOrder OrderTagName = demandOrder "tag_name"

data instance ViewFilter Comment
    = FilterCommentId (Reference Comment)
    | FilterCommentArticleId (Reference Article)
    | FilterCommentUserId (Reference User)
    deriving (Show, Eq)

data instance ViewOrder Comment
    = OrderCommentDate
    deriving (Show, Eq)

instance ListableObject Comment where
    applyFilter (FilterCommentId ref) = demandCondition (WhereIs ref "comment_id")
    applyFilter (FilterCommentArticleId ref) = demandCondition (WhereIs ref "comment_article_id")
    applyFilter (FilterCommentUserId ref) = demandCondition (WhereIs ref "comment_user_id")
    applyOrder OrderCommentDate = demandOrder "comment_date" . inverseDirection

data instance ViewFilter FileInfo
    = FilterFileId (Reference FileInfo)
    | FilterFileArticleId (Reference Article)
    | FilterFileUserId (Reference User)
    deriving (Show, Eq)

data instance ViewOrder FileInfo
    = OrderFileName
    | OrderFileMimeType
    | OrderFileUploadDate
    | OrderFileIndex
    deriving (Show, Eq)

instance ListableObject FileInfo where
    applyFilter (FilterFileId ref) = demandCondition (WhereIs ref "file_id")
    applyFilter (FilterFileArticleId ref) = demandCondition (WhereIs ref "file_article_id")
    applyFilter (FilterFileUserId ref) = demandCondition (WhereIs ref "file_user_id")
    applyOrder OrderFileName = demandOrder "file_name"
    applyOrder OrderFileMimeType = demandOrder "file_mimetype"
    applyOrder OrderFileUploadDate = demandOrder "file_upload_date" . inverseDirection
    applyOrder OrderFileIndex = demandOrder "file_index"

subcategoriesSource :: Reference Category -> RowSource
subcategoriesSource ref = RecursiveSource "subcategories"
    (fReference "subcategory_id" :/ E)
    (Select ["sn_categories"] (fReference "category_id" :/ E) [WhereIs ref "category_id"] [] (RowRange 0 maxBound))
    (Select ["sn_categories", "subcategories"] (fReference "category_id" :/ E) [Where "category_parent_id = subcategory_id"] [] (RowRange 0 maxBound))
