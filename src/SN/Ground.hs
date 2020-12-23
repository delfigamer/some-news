{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeFamilies #-}

module SN.Ground
    ( Reference(..)
    , Version(..)
    , User(..)
    , Password(..)
    , AccessKey(..)
    , Author(..)
    , Category(..)
    , PublicationStatus(..)
    , Article(..)
    , Tag(..)
    , Comment(..)
    , FileInfo(..)
    , Upload(..)
    , InitFailure(..)
    , Ground(..)
    , withSqlGround
    , currentSchema
    , upgradeSchema
    , Action(..)
    , GroundError(..)
    , ListView(..)
    , OrderDirection(..)
    , ViewFilter(..)
    , ViewOrder(..)
    , module SN.Ground.Config
    ) where

import Control.Monad.Reader
import Data.IORef
import Data.Int
import Data.Maybe
import Data.Time.Clock
import qualified Crypto.Random as CRand
import qualified Data.ByteString as BS
import qualified Data.Text as Text
import SN.Data.HList
import SN.Ground.Config
import SN.Ground.Interface
import SN.Ground.ListView
import SN.Ground.Schema
import SN.Ground.Types
import SN.Logger
import SN.Sql.Query
import qualified SN.Sql.Database as Db

data SqlGround = SqlGround
    { groundConfig :: GroundConfig
    , groundLogger :: Logger
    , groundDb :: Db.Database
    , groundGen :: IORef CRand.ChaChaDRG
    }

withSqlGround :: GroundConfig -> Logger -> Db.Database -> (InitFailure -> IO r) -> (Ground -> IO r) -> IO r
withSqlGround config logger db onFail onSuccess = do
    matchCurrentSchema logger db onFail $ do
        pgen <- newIORef =<< CRand.drgNew
        let ground = SqlGround config logger db pgen
        onSuccess $ Ground
            { groundPerform = \action -> do
                logDebug logger $ "SqlGround: Request: " <<| action
                sqlGroundPerform action ground $ \result -> do
                    {- use CPS to bring (Show r) into scope -}
                    logSend logger (resultLogLevel result) $
                        "SqlGround: Request finished: " <<| action << " -> " <<| result
                    return result
            , groundGenerateBytes = \count -> do
                atomicModifyIORef' pgen $ \gen1 ->
                    let (value, gen2) = CRand.randomBytesGenerate count gen1
                    in (gen2, value)
            , groundUpload = sqlGroundUpload ground
            , groundDownload = sqlGroundDownload ground
            }
  where
    resultLogLevel (Left InternalError) = LevelWarn
    resultLogLevel _ = LevelDebug

sqlGroundPerform :: Action a -> SqlGround -> (Show a => Either GroundError a -> IO r) -> IO r
sqlGroundPerform (UserCreate name surname password isAdmin) = runTransaction Db.ReadCommited $ do
    ref <- Reference <$> generateBytes groundConfigUserIdLength
    joinDate <- currentTime
    salt <- generateBytes (const 16)
    doQuery
        (Insert "sn_users"
            (fReference "user_id"
                :/ fText "user_name"
                :/ fText "user_surname"
                :/ fTime "user_join_date"
                :/ fBool "user_is_admin"
                :/ fBlob "user_password_salt"
                :/ fBlob "user_password_hash"
                :/ E)
            (Value ref
                :/ Value name
                :/ Value surname
                :/ Value joinDate
                :/ Value isAdmin
                :/ Value salt
                :/ Value (hashPassword salt password)
                :/ E))
        (parseInsert $ User ref name surname joinDate isAdmin)
sqlGroundPerform (UserCheckPassword userRef password) = runTransaction Db.ReadCommited $ do
    (salt, hash1) <- doQuery
        (Select ["sn_users"]
            (fBlob "user_password_salt" :/ fBlob "user_password_hash" :/ E)
            [WhereIs userRef "user_id"]
            []
            (RowRange 0 1))
        (\case
            [Just salt :/ Just hash :/ E] -> Right (salt, hash)
            [] -> Left NotFoundError
            _ -> Left InternalError)
    let hash2 = hashPassword salt password
    if hash1 == hash2
        then return ()
        else lift $ Db.rollback $ Left NotFoundError
sqlGroundPerform (UserSetName userRef name surname) = runTransaction Db.ReadCommited $ do
    doQuery
        (Update "sn_users"
            (fText "user_name" :/ fText "user_surname" :/ E)
            (Value name :/ Value surname :/ E)
            [WhereIs userRef "user_id"])
        parseCount
sqlGroundPerform (UserSetIsAdmin userRef isAdmin) = runTransaction Db.ReadCommited $ do
    doQuery
        (Update "sn_users"
            (fBool "user_is_admin" :/ E)
            (Value isAdmin :/ E)
            [WhereIs userRef "user_id"])
        parseCount
sqlGroundPerform (UserSetPassword userRef password) = runTransaction Db.ReadCommited $ do
    salt <- generateBytes (const 16)
    doQuery
        (Update "sn_users"
            (fBlob "user_password_salt" :/ fBlob "user_password_hash" :/ E)
            (Value salt :/ Value (hashPassword salt password) :/ E)
            [WhereIs userRef "user_id"])
        parseCount
sqlGroundPerform (UserDelete userRef) = runTransaction Db.ReadCommited $ do
    doQuery
        (Delete "sn_users"
            [WhereIs userRef "user_id"])
        parseCount
sqlGroundPerform (UserList view) = runTransaction Db.ReadCommited $ do
    time <- currentTime
    withView time view $ \tables filter order range _ -> do
        doQuery
            (Select ("sn_users" : tables)
                (fReference "user_id" :/ fText "user_name" :/ fText "user_surname" :/ fTime "user_join_date" :/ fBool "user_is_admin" :/ E)
                filter
                (order "user_id")
                range)
            (parseList $ parseOne User)

sqlGroundPerform (AccessKeyCreate userRef) = runTransaction Db.ReadCommited $ do
    keyBack <- generateBytes groundConfigAccessKeyTokenLength
    keyFront <- Reference <$> generateBytes groundConfigAccessKeyIdLength
    let key = AccessKey keyFront keyBack
    let keyHash = hashAccessKey key
    doQuery
        (Insert "sn_access_keys"
            (fReference "access_key_id" :/ fBlob "access_key_hash" :/ fReference "access_key_user_id" :/ E)
            (Value keyFront :/ Value keyHash :/ Value userRef :/ E))
        (parseInsert key)
sqlGroundPerform (AccessKeyClear userRef) = runTransaction Db.ReadCommited $ do
    doQuery_
        (Delete "sn_access_keys"
            [WhereIs userRef "access_key_user_id"])
sqlGroundPerform (AccessKeyList userRef view) = runTransaction Db.ReadCommited $ do
    time <- currentTime
    withView time view $ \tables filter order range _ -> do
        doQuery
            (Select ("sn_access_keys" : tables)
                (fReference "access_key_id" :/ E)
                (WhereIs userRef "access_key_user_id" : filter)
                (order "access_key_id")
                range)
            (parseList $ parseOne id)

sqlGroundPerform (AuthorCreate name description) = runTransaction Db.ReadCommited $ do
    ref <- Reference <$> generateBytes groundConfigAuthorIdLength
    doQuery
        (Insert "sn_authors"
            (fReference "author_id" :/ fText "author_name" :/ fText "author_description" :/ E)
            (Value ref :/ Value name :/ Value description :/ E))
        (parseInsert $ Author ref name description)
sqlGroundPerform (AuthorSetName authorRef name) = runTransaction Db.ReadCommited $ do
    doQuery
        (Update "sn_authors"
            (fText "author_name" :/ E)
            (Value name :/ E)
            [WhereIs authorRef "author_id"])
        parseCount
sqlGroundPerform (AuthorSetDescription authorRef description) = runTransaction Db.ReadCommited $ do
    doQuery
        (Update "sn_authors"
            (fText "author_description" :/ E)
            (Value description :/ E)
            [WhereIs authorRef "author_id"])
        parseCount
sqlGroundPerform (AuthorDelete authorRef) = runTransaction Db.ReadCommited $ do
    doQuery_
        (Delete "sn_articles"
            [WhereIs authorRef "article_author_id", WhereWith NonPublished "article_publication_date = ?"])
    doQuery
        (Delete "sn_authors"
            [WhereIs authorRef "author_id"])
        parseCount
sqlGroundPerform (AuthorList view) = runTransaction Db.ReadCommited $ do
    time <- currentTime
    withView time view $ \tables filter order range _ -> do
        doQuery
            (Select ("sn_authors" : tables)
                (fReference "author_id" :/ fText "author_name" :/ fText "author_description" :/ E)
                filter
                (order "author_id")
                range)
            (parseList $ parseOne Author)
sqlGroundPerform (AuthorSetOwnership authorRef userRef True) = runTransaction Db.ReadCommited $ do
    doQuery_
        (Insert "sn_author2user"
            (fReference "a2u_user_id" :/ fReference "a2u_author_id" :/ E)
            (Value userRef :/ Value authorRef :/ E))
sqlGroundPerform (AuthorSetOwnership authorRef userRef False) = runTransaction Db.ReadCommited $ do
    doQuery_
        (Delete "sn_author2user"
            [WhereIs userRef "a2u_user_id", WhereIs authorRef "a2u_author_id"])

sqlGroundPerform (CategoryCreate name parentRef) = runTransaction Db.ReadCommited $ do
    ref <- Reference <$> generateBytes groundConfigCategoryIdLength
    parentName <- case parentRef of
        "" -> return ""
        _ -> doQuery
            (Select ["sn_categories"]
                (fText "category_name" :/ E)
                [WhereIs parentRef "category_id"]
                []
                (RowRange 0 1))
            (parseHead $ parseOne id)
    doQuery
        (Insert "sn_categories"
            (fReference "category_id" :/ fText "category_name" :/ fReference "category_parent_id" :/ E)
            (Value ref :/ Value name :/ Value parentRef :/ E))
        (parseInsert $ Category ref name parentRef parentName)
sqlGroundPerform (CategorySetName categoryRef name) = runTransaction Db.ReadCommited $ do
    doQuery
        (Update "sn_categories"
            (fText "category_name" :/ E)
            (Value name :/ E)
            [WhereIs categoryRef "category_id"])
        parseCount
sqlGroundPerform (CategorySetParent categoryRef parentRef) = runTransaction Db.Serializable $ do
    doQuery
        (Select
            [ RecursiveSource "ancestors"
                (fReference "ancestor_category_id" :/ E)
                (Select ["sn_categories"]
                    (fReference "category_id" :/ E)
                    [WhereIs parentRef "category_id"]
                    []
                    (RowRange 0 maxBound))
                (Select ["sn_categories", "ancestors"]
                    (fReference "category_parent_id" :/ E)
                    [Where "category_id = ancestor_category_id"]
                    []
                    (RowRange 0 maxBound))
            ]
            (fReference "ancestor_category_id" :/ E)
            [WhereIs categoryRef "ancestor_category_id", Where "ancestor_category_id IS NOT NULL"]
            []
            (RowRange 0 1))
        (\case
            [] -> Right ()
            _ -> Left CyclicReferenceError)
    doQuery
        (Update "sn_categories"
            (fReference "category_parent_id" :/ E)
            (Value parentRef :/ E)
            [WhereIs categoryRef "category_id"])
        parseCount
sqlGroundPerform (CategoryDelete categoryRef) = runTransaction Db.Serializable $ do
    parent <- doQuery
        (Select ["sn_categories"]
            (fReference "category_parent_id" :/ E)
            [WhereIs categoryRef "category_id"]
            []
            (RowRange 0 1))
        (parseHead $ parseOne id)
    doQuery_
        (Update "sn_articles"
            (fReference "article_category_id" :/ E)
            (Value parent :/ E)
            [WhereIs categoryRef "article_category_id"])
    doQuery_
        (Update "sn_categories"
            (fReference "category_parent_id" :/ E)
            (Value parent :/ E)
            [WhereIs categoryRef "category_parent_id"])
    doQuery_
        (Delete "sn_categories"
            [WhereIs categoryRef "category_id"])
sqlGroundPerform (CategoryList view) = runTransaction Db.ReadCommited $ do
    time <- currentTime
    withView time view $ \tables filter order range _ -> do
        doQuery
            (Select
                ("sn_categories AS cat1"
                    : OuterJoinSource "sn_categories AS cat2" (WhereFieldIs "cat1.category_parent_id" "cat2.category_id")
                    : tables)
                (fReference "cat1.category_id"
                    :/ fText "cat1.category_name"
                    :/ fReference "cat1.category_parent_id"
                    :/ fText "COALESCE(cat2.category_name, '')"
                    :/ E)
                filter
                (order "cat1.category_id")
                range)
            (parseList $ parseOne Category)
sqlGroundPerform (CategoryAncestry categoryRef) = runTransaction Db.ReadCommited $ do
    ancestryLimit <- fromIntegral . groundConfigCategoryAncestryLimit . groundConfig <$> ask
    tups <- doQuery
        (Select
            [RecursiveSource "anc"
                (fReference "anc_id" :/ fText "anc_name" :/ fReference "anc_parent_id" :/ fInt "anc_level" :/ E)
                (Select
                    ["sn_categories"]
                    (fReference "category_id" :/ fText "category_name" :/ fReference "category_parent_id" :/ fInt "0" :/ E)
                    [WhereIs categoryRef "category_id"]
                    []
                    (RowRange 0 maxBound))
                (Select
                    ["sn_categories", "anc"]
                    (fReference "category_id" :/ fText "category_name" :/ fReference "category_parent_id" :/ fInt "anc_level + 1" :/ E)
                    [WhereFieldIs "category_id" "anc_parent_id"]
                    []
                    (RowRange 0 maxBound))]
            (fReference "anc_id" :/ fText "anc_name" :/ E)
            []
            [Asc "anc_level"]
            (RowRange 0 (ancestryLimit + 1)))
        (parseList $ parseOne (,))
    return $ buildAncestry ancestryLimit tups
  where
    buildAncestry _ [] = []
    buildAncestry limit ((ref, name) : rest)
        | limit <= 0 = []
        | (parentRef, parentName) : _ <- rest = Category ref name parentRef parentName : buildAncestry (limit - 1) rest
        | otherwise = [Category ref name "" ""]

sqlGroundPerform (ArticleCreate authorRef) = runTransaction Db.ReadCommited $ do
    articleRef <- Reference <$> generateBytes groundConfigArticleIdLength
    articleVersion <- Version <$> generateBytes groundConfigArticleVersionLength
    doQuery
        (Insert "sn_articles"
            (fReference "article_id"
                :/ fVersion "article_version"
                :/ fReference "article_author_id"
                :/ fText "article_name"
                :/ fText "article_text"
                :/ fPublicationStatus "article_publication_date"
                :/ fReference "article_category_id"
                :/ E)
            (Value articleRef
                :/ Value articleVersion
                :/ Value authorRef
                :/ Value ""
                :/ Value ""
                :/ Value NonPublished
                :/ Value (Reference "")
                :/ E))
        (parseInsert $ Article articleRef articleVersion authorRef "" NonPublished (Reference ""))
sqlGroundPerform (ArticleGetText articleRef) = runTransaction Db.ReadCommited $ do
    doQuery
        (Select ["sn_articles"]
            (fText "article_text" :/ E)
            [WhereIs articleRef "article_id"]
            []
            (RowRange 0 1))
        (parseHead $ parseOne id)
sqlGroundPerform (ArticleSetAuthor articleRef authorRef) = runTransaction Db.ReadCommited $ do
    doQuery
        (Update "sn_articles"
            (fReference "article_author_id" :/ E)
            (Value authorRef :/ E)
            [WhereIs articleRef "article_id"])
        parseCount
sqlGroundPerform (ArticleSetName articleRef name) = runTransaction Db.ReadCommited $ do
    doQuery
        (Update "sn_articles"
            (fText "article_name" :/ E)
            (Value name :/ E)
            [WhereIs articleRef "article_id"])
        parseCount
sqlGroundPerform (ArticleSetText articleRef oldVersion text) = runTransaction Db.Serializable $ do
    doQuery
        (Select ["sn_articles"]
            (fVersion "article_version" :/ E)
            [WhereIs articleRef "article_id"]
            []
            (RowRange 0 1))
        (\case
            [Just version :/ E]
                | version == oldVersion -> Right ()
                | otherwise -> Left VersionError
            [] -> Left NotFoundError
            _ -> Left InternalError)
    newVersion <- Version <$> generateBytes groundConfigArticleVersionLength
    doQuery_
        (Update "sn_articles"
            (fVersion "article_version" :/ fText "article_text" :/ E)
            (Value newVersion :/ Value text :/ E)
            [WhereIs articleRef "article_id"])
    return newVersion
sqlGroundPerform (ArticleSetCategory articleRef categoryRef) = runTransaction Db.ReadCommited $ do
    doQuery
        (Update "sn_articles"
            (fReference "article_category_id" :/ E)
            (Value categoryRef :/ E)
            [WhereIs articleRef "article_id"])
        parseCount
sqlGroundPerform (ArticleSetPublicationStatus articleRef pubStatus) = runTransaction Db.ReadCommited $ do
    doQuery
        (Update "sn_articles"
            (fPublicationStatus "article_publication_date" :/ E)
            (Value pubStatus :/ E)
            [WhereIs articleRef "article_id"])
        parseCount
sqlGroundPerform (ArticleDelete articleRef) = runTransaction Db.ReadCommited $ do
    doQuery
        (Delete "sn_articles"
            [WhereIs articleRef "article_id"])
        parseCount
sqlGroundPerform (ArticleList view) = runTransaction Db.ReadCommited $ do
    time <- currentTime
    withView time view $ \tables filter order range time -> do
        doQuery
            (Select ("sn_articles" : tables)
                (fReference "article_id"
                    :/ fVersion "article_version"
                    :/ fReference "article_author_id"
                    :/ fText "article_name"
                    :/ fPublicationStatus "article_publication_date"
                    :/ fReference "article_category_id"
                    :/ E)
                filter
                (order "article_id")
                range)
            (parseList $ parseOne Article)
sqlGroundPerform (ArticleSetTag articleRef tagRef True) = runTransaction Db.ReadCommited $ do
    doQuery_
        (Insert "sn_article2tag"
            (fReference "a2t_article_id" :/ fReference "a2t_tag_id" :/ E)
            (Value articleRef :/ Value tagRef :/ E))
sqlGroundPerform (ArticleSetTag articleRef tagRef False) = runTransaction Db.ReadCommited $ do
    doQuery_
        (Delete "sn_article2tag"
            [WhereIs articleRef "a2t_article_id", WhereIs tagRef "a2t_tag_id"])

sqlGroundPerform (TagCreate name) = runTransaction Db.ReadCommited $ do
    tagRef <- Reference <$> generateBytes groundConfigTagIdLength
    doQuery
        (Insert "sn_tags"
            (fReference "tag_id" :/ fText "tag_name" :/ E)
            (Value tagRef :/ Value name :/ E))
        (parseInsert $ Tag tagRef name)
sqlGroundPerform (TagSetName tagRef name) = runTransaction Db.ReadCommited $ do
    doQuery
        (Update "sn_tags"
            (fText "tag_name" :/ E)
            (Value name :/ E)
            [WhereIs tagRef "tag_id"])
        parseCount
sqlGroundPerform (TagDelete tagRef) = runTransaction Db.ReadCommited $ do
    doQuery
        (Delete "sn_tags"
            [WhereIs tagRef "tag_id"])
        parseCount
sqlGroundPerform (TagList view) = runTransaction Db.ReadCommited $ do
    time <- currentTime
    withView time view $ \tables filter order range time -> do
        doQuery
            (Select ("sn_tags" : tables)
                (fReference "tag_id" :/ fText "tag_name" :/ E)
                filter
                (order "tag_id")
                range)
            (parseList $ parseOne Tag)

sqlGroundPerform (CommentCreate articleRef userRef text) = runTransaction Db.ReadCommited $ do
    commentRef <- Reference <$> generateBytes groundConfigCommentIdLength
    commentDate <- currentTime
    doQuery
        (Insert "sn_comments"
            (fReference "comment_id"
                :/ fReference "comment_article_id"
                :/ fReference "comment_user_id"
                :/ fText "comment_text"
                :/ fTime "comment_date"
                :/ E)
            (Value commentRef
                :/ Value articleRef
                :/ Value userRef
                :/ Value text
                :/ Value commentDate
                :/ E))
        (parseInsert $ Comment commentRef articleRef userRef text commentDate Nothing)
sqlGroundPerform (CommentSetText commentRef text) = runTransaction Db.ReadCommited $ do
    editDate <- currentTime
    doQuery
        (Update "sn_comments"
            (fText "comment_text" :/ fTime "comment_edit_date" :/ E)
            (Value text :/ Value editDate :/ E)
            [WhereIs commentRef "comment_id"])
        parseCount
sqlGroundPerform (CommentDelete commentRef) = runTransaction Db.ReadCommited $ do
    doQuery
        (Delete "sn_comments"
            [WhereIs commentRef "comment_id"])
        parseCount
sqlGroundPerform (CommentList view) = runTransaction Db.ReadCommited $ do
    time <- currentTime
    withView time view $ \tables filter order range time -> do
        doQuery
            (Select ("sn_comments" : tables)
                (fReference "comment_id"
                    :/ fReference "comment_article_id"
                    :/ fReference "comment_user_id"
                    :/ fText "comment_text"
                    :/ fTime "comment_date"
                    :/ fTime "comment_edit_date"
                    :/ E)
                filter
                (order "comment_id")
                range)
            (parseList $ \case
                Just commentRef :/ Just articleRef :/ Just userRef :/ Just text :/ Just commentDate :/ maybeEditDate :/ E ->
                    Just $ Comment commentRef articleRef userRef text commentDate maybeEditDate
                _ -> Nothing)

sqlGroundPerform (FileSetName fileRef name) = runTransaction Db.ReadCommited $ do
    doQuery
        (Update "sn_files"
            (fText "file_name" :/ E)
            (Value name :/ E)
            [WhereIs fileRef "file_id"])
        parseCount
sqlGroundPerform (FileSetIndex fileRef index) = runTransaction Db.ReadCommited $ do
    doQuery
        (Update "sn_files"
            (fInt "file_index" :/ E)
            (Value index :/ E)
            [WhereIs fileRef "file_id"])
        parseCount
sqlGroundPerform (FileDelete fileRef) = runTransaction Db.ReadCommited $ do
    doQuery
        (Delete "sn_files"
            [WhereIs fileRef "file_id"])
        parseCount
sqlGroundPerform (FileList view) = runTransaction Db.ReadCommited $ do
    time <- currentTime
    withView time view $ \tables filter order range time -> do
        doQuery
            (Select ("sn_files" : tables)
                (fReference "file_id"
                    :/ fText "file_name"
                    :/ fText "file_mimetype"
                    :/ fTime "file_upload_date"
                    :/ fReference "file_article_id"
                    :/ fInt "file_index"
                    :/ fReference "file_user_id"
                    :/ E)
                filter
                (order "file_id")
                range)
            (parseList $ parseOne FileInfo)

sqlGroundUpload
    :: SqlGround
    -> Text.Text
    -> Text.Text
    -> Reference Article
    -> Reference User
    -> (FileInfo -> IO (Upload r))
    -> IO (Either GroundError r)
sqlGroundUpload ground name mimeType articleRef userRef uploader = go (groundConfigTransactionRetryCount $ groundConfig ground)
  where
    go n
        | n <= 0 = return $ Left InternalError
        | otherwise = do
            pSourceConsumed <- newIORef False
            tret <- Db.execute (groundDb ground) Db.ReadCommited $ do
                uploadTime <- currentTime
                fileRef <- (Reference <$> generateBytes groundConfigFileIdLength) `runReaderT` ground
                maxIndexQret <- Db.query
                    (Select ["sn_files"]
                        (fInt "COALESCE(MAX(file_index), 0)" :/ E)
                        [WhereIs articleRef "file_article_id"]
                        []
                        (RowRange 0 1))
                maxIndex <- case maxIndexQret of
                    [Just i :/ E] -> return i
                    _ -> Db.abort
                {- since this transaction is relaxed, it's possible for multiple files to end up with the same (`article_id`, `index`) pair -}
                {- this is fine -}
                {- for identification, we use an independent `id` field, which is under a primary key constraint -}
                {- for ordering, we always append an `id ASC` to the end of the "order by" clause, so it's still always deterministic -}
                {- if the user isn't happy with whatever order the files turned out to be, they can always rearrange the indices later -}
                let myIndex = maxIndex + 1
                insertQret <- Db.query
                    (Insert "sn_files"
                        (fReference "file_id"
                            :/ fText "file_name"
                            :/ fText "file_mimetype"
                            :/ fTime "file_upload_date"
                            :/ fReference "file_article_id"
                            :/ fInt "file_index"
                            :/ fReference "file_user_id"
                            :/ E)
                        (Value fileRef
                            :/ Value name
                            :/ Value mimeType
                            :/ Value uploadTime
                            :/ Value articleRef
                            :/ Value myIndex
                            :/ Value userRef
                            :/ E))
                unless insertQret $ Db.abort
                liftIO $ writeIORef pSourceConsumed True
                driveUpload fileRef 0 $ uploader $ FileInfo fileRef name mimeType uploadTime articleRef myIndex userRef
            case tret of
                Right bret -> return bret
                Left Db.QueryError -> return $ Left InternalError
                Left Db.SerializationError -> do
                    sourceConsumed <- readIORef pSourceConsumed
                    if sourceConsumed
                        then return $ Left InternalError
                        else go $ n - 1
    driveUpload fileRef chunkIndex action = do
        aret <- liftIO action
        case aret of
            UploadAbort result -> do
                Db.rollback $ Right result
            UploadFinish result -> do
                return $ Right result
            UploadChunk chunkData next -> do
                qret <- Db.query
                    (Insert "sn_file_chunks"
                        (fReference "chunk_file_id" :/ fInt "chunk_index" :/ fBlob "chunk_data" :/ E)
                        (Value fileRef :/ Value chunkIndex :/ Value chunkData :/ E))
                unless qret $ Db.abort
                driveUpload fileRef (chunkIndex + 1) next

sqlGroundDownload
    :: SqlGround
    -> Reference FileInfo
    -> (GroundError -> IO r)
    -> (Int64 -> Text.Text -> ((BS.ByteString -> IO ()) -> IO ()) -> IO r)
    -> IO r
sqlGroundDownload ground fileRef onError onStream = do
    pmResult <- newIORef Nothing
    tret <- Db.execute (groundDb ground) Db.RepeatableRead $ do
        filetypeQret <- Db.query
            (Select
                ["sn_files"]
                (fText "file_mimetype" :/ E)
                [WhereIs fileRef "file_id"]
                []
                (RowRange 0 1))
        filetype <- case filetypeQret of
            [Just filetype :/ E] -> return filetype
            [] -> Db.rollback $ Left NotFoundError
            _ -> Db.abort
        filesizeQret <- Db.query
            (Select
                ["sn_file_chunks"]
                (fInt "sum(length(chunk_data))" :/ E)
                [WhereIs fileRef "chunk_file_id"]
                []
                (RowRange 0 1))
        filesize <- case filesizeQret of
            [Just filesize :/ E] -> return filesize
            [Nothing :/ E] -> Db.rollback $ Left NotFoundError
            _ -> Db.abort
        sink <- Db.mshift $ \cont -> do
            pOut <- newIORef $ error "invalid onStream callback"
            result <- onStream filesize filetype $ \sink -> do
                cont sink >>= writeIORef pOut
            writeIORef pmResult $ Just result
            readIORef pOut
        driveDownloader 0 sink
    mResult <- readIORef pmResult
    case mResult of
        Just result -> return result
        Nothing -> case tret of
            Right (Left err) -> onError err
            _ -> onError InternalError
  where
    driveDownloader index sink = do
        chunkQret <- Db.query
            (Select
                ["sn_file_chunks"]
                (fBlob "chunk_data" :/ E)
                [WhereIs fileRef "chunk_file_id"]
                [Asc "chunk_index"]
                (RowRange index 1))
        case chunkQret of
            [] -> return $ Right ()
            [Just chunkData :/ E] -> do
                liftIO $ sink chunkData
                driveDownloader (index + 1) sink
            _ -> Db.abort

type Transaction a = ReaderT SqlGround (Db.Transaction (Either GroundError a))

generateBytes :: (MonadReader SqlGround m, MonadIO m) => (GroundConfig -> Int) -> m BS.ByteString
generateBytes getter = do
    SqlGround
        { groundConfig = config
        , groundGen = tgen
        } <- ask
    liftIO $ atomicModifyIORef' tgen $ \gen1 ->
        let (value, gen2) = CRand.randomBytesGenerate (getter config) gen1
        in (gen2, value)

doQuery :: Query qr -> (qr -> Either GroundError r) -> Transaction a r
doQuery query parser = ReaderT $ \_ -> do
    qr <- Db.query query
    case parser qr of
        Left ste -> Db.rollback $ Left ste
        Right r -> return r

doQuery_ :: Query qr -> Transaction a ()
doQuery_ query = doQuery query (const $ Right ())

runTransaction :: Show a => Db.TransactionLevel -> Transaction a a -> SqlGround -> (Show a => Either GroundError a -> IO r) -> IO r
runTransaction level body ground cont = do
    tret <- Db.executeRepeat (groundDb ground) (groundConfigTransactionRetryCount $ groundConfig ground) level $ do
        br <- body `runReaderT` ground
        return $ Right br
    case tret of
        Right bret -> cont bret
        Left _ -> cont $ Left InternalError

parseInsert :: a -> Bool -> Either GroundError a
parseInsert x True = Right x
parseInsert _ False = Left InternalError

parseCount :: Int64 -> Either GroundError ()
parseCount 1 = Right ()
parseCount 0 = Left NotFoundError
parseCount _ = Left InternalError

parseList :: (HList Maybe ts -> Maybe b) -> [HList Maybe ts] -> Either GroundError [b]
parseList func rs = Right $ mapMaybe func rs

parseHead :: (HList Maybe ts -> Maybe b) -> [HList Maybe ts] -> Either GroundError b
parseHead func [r] = case func r of
    Just x -> Right x
    Nothing -> Left InternalError
parseHead _ [] = Left NotFoundError
parseHead _ _ = Left InternalError

currentTime :: MonadIO m => m UTCTime
currentTime = do
    UTCTime uDay uTime <- liftIO getCurrentTime
    let tpsec1 = diffTimeToPicoseconds uTime
    let tpsec2 = (tpsec1 `div` rounding) * rounding
    return $ UTCTime uDay (picosecondsToDiffTime tpsec2)
  where
    rounding = 1000000000 -- 1 millisecond

type family Returning r ts where
    Returning r '[] = r
    Returning r (t ': ts) = t -> Returning r ts

parseOne :: Returning r ts -> HList Maybe ts -> Maybe r
parseOne f (Just x :/ xs) = parseOne (f x) xs
parseOne _ (Nothing :/ xs) = Nothing
parseOne x E = Just x
