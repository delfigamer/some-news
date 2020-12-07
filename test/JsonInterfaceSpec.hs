{-# LANGUAGE StandaloneDeriving #-}

module JsonInterfaceSpec
    ( spec
    ) where

import Control.Exception
import Control.Monad
import Data.Aeson
import Data.Aeson.Text
import Data.IORef
import Data.List
import Data.Maybe
import Data.String
import Test.Hspec
import qualified Data.ByteString as BS
import qualified Data.HashMap.Strict as Map
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import qualified Data.Text.Lazy as TextLazy
import qualified Data.Time.Clock as TClock
import qualified Data.Time.Format.ISO8601 as TFormat
import qualified Data.Vector as Vector
import Hex
import Logger
import Storage
import Storage.Fake
import JsonInterface
import qualified JsonInterface.Config as Config

mkTime :: String -> TClock.UTCTime
mkTime = fromJust . TFormat.iso8601ParseM

testConfig :: Config.Config
testConfig = Config.Config
    { Config.defaultPageLimit = 10
    , Config.maxPageLimit = 100
    , Config.minPasswordLength = 4
    , Config.maxAccessKeyCount = 3
    , Config.ticketLength = 4
    , Config.ticketLifetime = 10
    , Config.fileChunkSize = 10
    , Config.maxFileSize = 50
    }

expectResponse :: HasCallStack => Response -> Accept ()
expectResponse expected = Accept
    (\given -> given `shouldBe` expected)
    (\_ _ _ -> expectationFailure "expected a simple response, got a stream")

expectStream :: HasCallStack => Text.Text -> [BS.ByteString] -> Accept ()
expectStream expectedType expectedChunks = Accept
    (\given -> expectationFailure $ "expected a stream, got a " ++ show given)
    (\givenSize givenType inner -> do
        givenSize `shouldBe` expectedSize
        givenType `shouldBe` expectedType
        pbuf <- newIORef expectedChunks
        inner $ \chunk -> do
            buf <- readIORef pbuf
            case buf of
                [] -> expectationFailure $ "unexpected chunk" ++ show chunk
                c : cs -> do
                    chunk `shouldBe` c
                    writeIORef pbuf cs
        readIORef pbuf `shouldReturn` [])
  where
    expectedSize = foldl' (+) 0 $ map (fromIntegral . BS.length) expectedChunks

rqdata :: [(BS.ByteString, BS.ByteString)] -> RequestData
rqdata params = RequestData (Map.fromList params) "host"

testSendFile
    :: (BS.ByteString -> BS.ByteString -> BS.ByteString -> IO BS.ByteString -> IO ())
    -> BS.ByteString -> BS.ByteString -> BS.ByteString -> [BS.ByteString] -> IO ()
testSendFile sendFile paramName fileName fileType chunkList = do
    pbuf <- newIORef chunkList
    let reader = do
            buf <- readIORef pbuf
            case buf of
                c : cs -> do
                    writeIORef pbuf cs
                    return c
                [] -> do
                    return ""
    sendFile paramName fileName fileType reader
    readIORef pbuf `shouldReturn` []

data TestEnv = TestEnv FakeStorage JsonInterface

withTestEnv :: (TestEnv -> IO r) -> IO r
withTestEnv inner = do
    withTestLogger $ \logger -> do
        withFakeStorage $ \fake storage -> do
            withJsonInterface testConfig logger storage $ \jint -> do
                inner $ TestEnv fake jint

data MethodEnv = MethodEnv FakeStorage (Accept () -> Request -> IO ())

specifyMethod :: String -> (MethodEnv -> IO ()) -> SpecWith TestEnv
specifyMethod uri inner = do
    let uriParts = Text.splitOn "/" $ Text.pack $ takeWhile (/= ';') uri
    specify uri $ \(TestEnv fake jint) -> do
        inner $ MethodEnv fake (executeRequest jint uriParts)

spec :: Spec
spec = around withTestEnv $ do
    describe "JsonInterface" $ do
        let userLookup = UserList (ListView 0 1 [FilterUserAccessKey $ AccessKey (Reference "\x01") "\x02"] [])
        let tuser = User "uid" "foo" "bar" (mkTime "2020-01-02T03:04:05.06Z") False
        let adminLookup = UserList (ListView 0 1 [FilterUserAccessKey $ AccessKey (Reference "\x01") "\x02", FilterUserIsAdmin True] [])
        let tadmin = User "uida" "fooa" "bara" (mkTime "2020-01-01T02:02:02.33Z") True
        let tauthor = Author "author" "authorname" "authordesc"
        let tcategory1 = Category "cat1" "cat name 1" "cat2"
        let tcategory2 = Category "cat2" "cat name 2" ""
        let tarticle = Article "aid" "aver" "author" "aname" (PublishAt $ mkTime "2020-02-01T01:01:01Z") "cat1"
        let ttag1 = Tag "tag1" "tag name 1"
        let ttag2 = Tag "tag2" "tag name 2"
        let tarticleEx = ExArticle "aid" "aver"
                (Just $ ExAuthor tauthor)
                "aname"
                (PublishAt $ mkTime "2020-02-01T01:01:01Z")
                (Just $ ExCategory "cat1" "cat name 1" $ Just $ ExCategory "cat2" "cat name 2" Nothing)
                [ExTag ttag1, ExTag ttag2]
        specifyMethod "unknown URI" $ \(MethodEnv fake exec) -> do
            exec
                (expectResponse $ Response StatusNotFound $ ResponseBodyError ErrUnknownRequest)
                (SimpleRequest $ rqdata [])
        specifyMethod "users/create" $ \(MethodEnv fake exec) -> do
            {- name :: required non-empty text -}
            {- surname :: required non-empty text -}
            {- newPassword :: required bytestring (at least Config.minPasswordLength bytes) -}
            {- normally, 'newPassword' would be a utf8-encoded string, but if a user wants to have it binary, so be it -}
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "name")
                (SimpleRequest $ rqdata [("surname", "bar"), ("password", "1234")])
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "name")
                (SimpleRequest $ rqdata [("name", ""), ("surname", "bar"), ("password", "1234")])
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "surname")
                (SimpleRequest $ rqdata [("name", "foo"), ("password", "1234")])
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "surname")
                (SimpleRequest $ rqdata [("name", "foo"), ("surname", ""), ("password", "1234")])
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "newPassword")
                (SimpleRequest $ rqdata [("name", "foo"), ("surname", "bar"), ("newPassword", "123")])
            checkpoint fake
                [ UserCreate "foo" "bar" "1234" |>> Right tuser
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkUser $ ExUser tuser)
                (SimpleRequest $ rqdata [("name", "foo"), ("surname", "bar"), ("newPassword", "1234")])
            checkpoint fake
                [ UserCreate "foo" "bar" "abc\255" |>> Right tuser
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkUser $ ExUser tuser)
                (SimpleRequest $ rqdata [("name", "foo"), ("surname", "bar"), ("newPassword", "abc\255")])
        specifyMethod "users/info" $ \(MethodEnv fake exec) -> do
            {- user :: required user id -}
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "user")
                (SimpleRequest $ rqdata [])
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "user")
                (SimpleRequest $ rqdata [("user", "not a reference")])
            checkpoint fake
                [ UserList (ListView 0 1 [FilterUserId "uid1"] []) |>> Right []
                ]
            exec
                (expectResponse $ Response StatusNotFound $ ResponseBodyError ErrNotFound)
                (SimpleRequest $ rqdata [("user", hex "uid1")])
            checkpoint fake
                [ UserList (ListView 0 1 [FilterUserId "uid1"] []) |>> Right [tuser]
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkUser $ ExUser tuser)
                (SimpleRequest $ rqdata [("user", hex "uid1")])
        specifyMethod "users/me; also user authorization" $ \(MethodEnv fake exec) -> do
            {- akey :: required user access key -}
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "akey")
                (SimpleRequest $ rqdata [])
            let invalidAkeys =
                    [ ""
                    , "0123"
                    , "0123:45:67"
                    , "0123:def"
                    , "012:cdef"
                    , "0123:defg"
                    , "FE:3210"
                    ]
            forM_ invalidAkeys $ \akey -> do
                exec
                    (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "akey")
                    (SimpleRequest $ rqdata [("akey", akey)])
            let validAkeys =
                    [ ("01234567:89abcdef", "\x01\x23\x45\x67", "\x89\xab\xcd\xef")
                    , ("fe:3210", "\xfe", "\x32\x10")
                    ]
            forM_ validAkeys $ \(akey, keyref, keytoken) -> do
                checkpoint fake
                    [ UserList (ListView 0 1 [FilterUserAccessKey $ AccessKey (Reference keyref) keytoken] []) |>> Right []
                    ]
                exec
                    (expectResponse $ Response StatusForbidden $ ResponseBodyError ErrInvalidAccessKey)
                    (SimpleRequest $ rqdata [("akey", akey)])
                checkpoint fake
                    [ UserList (ListView 0 1 [FilterUserAccessKey $ AccessKey (Reference keyref) keytoken] []) |>> Right [tuser]
                    ]
                exec
                    (expectResponse $ Response StatusOk $ ResponseBodyOkUser $ ExUser tuser)
                    (SimpleRequest $ rqdata [("akey", akey)])
        specifyMethod "users/setName" $ \(MethodEnv fake exec) -> do
            {- akey :: required user access key -}
            {- name :: required non-empty text -}
            {- surname :: required non-empty text -}
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "akey")
                (SimpleRequest $ rqdata [("name", "foo"), ("surname", "bar")])
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "surname")
                (SimpleRequest $ rqdata [("akey", "01:23"), ("name", "foo")])
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "surname")
                (SimpleRequest $ rqdata [("akey", "01:23"), ("name", "foo"), ("surname", "")])
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "name")
                (SimpleRequest $ rqdata [("akey", "01:23"), ("surname", "bar")])
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "name")
                (SimpleRequest $ rqdata [("akey", "01:23"), ("name", ""), ("surname", "bar")])
            checkpoint fake
                [ UserList (ListView 0 1 [FilterUserAccessKey $ AccessKey (Reference "\x01\x02") "\x03\x04"] []) |>> Right []
                ]
            exec
                (expectResponse $ Response StatusForbidden $ ResponseBodyError ErrInvalidAccessKey)
                (SimpleRequest $ rqdata [("akey", "0102:0304"), ("name", "foo"), ("surname", "bar")])
            checkpoint fake
                [ UserList (ListView 0 1 [FilterUserAccessKey $ AccessKey (Reference "\x01\x02") "\x03\x04"] []) |>> Right [tuser]
                , UserSetName "uid" "foo" "bar" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "0102:0304"), ("name", "foo"), ("surname", "bar")])
        specifyMethod "users/setPassword; also password authorization" $ \(MethodEnv fake exec) -> do
            {- user :: required user id -}
            {- password :: required current password -}
            {- newPassword :: required bytestring (at least Config.minPasswordLength bytes) -}
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "user")
                (SimpleRequest $ rqdata [("password", "1234"), ("newPassword", "abcd")])
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "user")
                (SimpleRequest $ rqdata [("user", "not a reference"), ("password", "1234"), ("newPassword", "abcd")])
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "password")
                (SimpleRequest $ rqdata [("user", hex "uid1"), ("newPassword", "abcd")])
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "newPassword")
                (SimpleRequest $ rqdata [("user", hex "uid1"), ("password", "1234")])
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "newPassword")
                (SimpleRequest $ rqdata [("user", hex "uid1"), ("password", "1234"), ("newPassword", "a")])
            checkpoint fake
                [ UserCheckPassword "uid1" "1234" |>> Left NotFoundError
                ]
            exec
                (expectResponse $ Response StatusForbidden $ ResponseBodyError ErrInvalidAccessKey)
                (SimpleRequest $ rqdata [("user", hex "uid1"), ("password", "1234"), ("newPassword", "abcd")])
            checkpoint fake
                [ UserCheckPassword "uid1" "1234" |>> Right ()
                , UserSetPassword "uid1" "abcd" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("user", hex "uid1"), ("password", "1234"), ("newPassword", "abcd")])
        specifyMethod "users/delete; also confirmation tickets" $ \(MethodEnv fake exec) -> do
            {- akey :: required user access key -}
            {- confirm :: confirmation ticket -}
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "akey")
                (SimpleRequest $ rqdata [])
            let tuser1 = User "uid1" "foo1" "bar1" (mkTime "2020-01-02T03:04:05.06Z") False
            let tuser2 = User "uid2" "foo2" "bar2" (mkTime "2020-01-02T03:04:05.06Z") False
            let badTickets =
                    [ []
                    , [("confirm", "")]
                    , [("confirm", "-")]
                    , [("confirm", "not a hex string")]
                    , [("confirm", "01020304")]
                    ]
            forM_ badTickets $ \ticket -> do
                checkpoint fake
                    [ UserList (ListView 0 1 [FilterUserAccessKey $ AccessKey (Reference "\x01") "\x02"] []) |>> Right [tuser1]
                    , StorageGenerateBytes 4 |>> "tikt"
                    ]
                exec
                    (expectResponse $ Response StatusOk $ ResponseBodyConfirm "tikt")
                    (SimpleRequest $ rqdata $ ticket ++ [("akey", "01:02")])
            checkpoint fake
                [ UserList (ListView 0 1 [FilterUserAccessKey $ AccessKey (Reference "\x03") "\x04"] []) |>> Right [tuser2]
                , StorageGenerateBytes 4 |>> "2222"
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyConfirm "2222")
                (SimpleRequest $ rqdata [("akey", "03:04"), ("confirm", hex "tikt")])
            checkpoint fake
                [ UserList (ListView 0 1 [FilterUserAccessKey $ AccessKey (Reference "\x01") "\x02"] []) |>> Right [tuser1]
                , UserDelete "uid1" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "01:02"), ("confirm", hex "tikt")])
            checkpoint fake
                [ UserList (ListView 0 1 [FilterUserAccessKey $ AccessKey (Reference "\x03") "\x04"] []) |>> Right [tuser2]
                , UserDelete "uid2" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "03:04"), ("confirm", hex "2222")])
        specifyMethod "users/createAccessKey" $ \(MethodEnv fake exec) -> do
            {- user :: required user id -}
            {- password :: required current password -}
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "user")
                (SimpleRequest $ rqdata [("password", "1234")])
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "user")
                (SimpleRequest $ rqdata [("user", "not a reference"), ("password", "1234")])
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "password")
                (SimpleRequest $ rqdata [("user", hex "uid1")])
            checkpoint fake
                [ UserCheckPassword "uid1" "1234" |>> Left NotFoundError
                ]
            exec
                (expectResponse $ Response StatusForbidden $ ResponseBodyError ErrInvalidAccessKey)
                (SimpleRequest $ rqdata [("user", hex "uid1"), ("password", "1234")])
            checkpoint fake
                [ UserCheckPassword "uid1" "1234" |>> Right ()
                , AccessKeyList "uid1" (ListView 0 maxBound [] []) |>> Right ["a1", "a2", "a3"]
                ]
            exec
                (expectResponse $ Response StatusForbidden $ ResponseBodyError ErrLimitExceeded)
                (SimpleRequest $ rqdata [("user", hex "uid1"), ("password", "1234")])
            checkpoint fake
                [ UserCheckPassword "uid1" "1234" |>> Right ()
                , AccessKeyList "uid1" (ListView 0 maxBound [] []) |>> Right ["a1", "a2"]
                , AccessKeyCreate "uid1" |>> Right (AccessKey "akid" "aktok")
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkAccessKey (AccessKey "akid" "aktok"))
                (SimpleRequest $ rqdata [("user", hex "uid1"), ("password", "1234")])
        specifyMethod "users/listAccessKeys" $ \(MethodEnv fake exec) -> do
            {- user :: required user id -}
            {- password :: required current password -}
            checkpoint fake
                [ UserCheckPassword "uid1" "1234" |>> Right ()
                , AccessKeyList "uid1" (ListView 0 maxBound [] []) |>> Right ["a1", "a2", "a3"]
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkAccessKeyList ["a1", "a2", "a3"])
                (SimpleRequest $ rqdata [("user", hex "uid1"), ("password", "1234")])
        specifyMethod "users/clearAccessKeys" $ \(MethodEnv fake exec) -> do
            {- user :: required user id -}
            {- password :: required current password -}
            checkpoint fake
                [ UserCheckPassword "uid1" "1234" |>> Right ()
                , AccessKeyClear "uid1" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("user", hex "uid1"), ("password", "1234")])
        specifyMethod "users/setName_; also admin authorization" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- name :: required non-empty text -}
            {- surname :: required non-empty text -}
            exec
                (expectResponse $ Response StatusNotFound $ ResponseBodyError ErrUnknownRequest)
                (SimpleRequest $ rqdata [])
            exec
                (expectResponse $ Response StatusNotFound $ ResponseBodyError ErrUnknownRequest)
                (SimpleRequest $ rqdata [("akey", "not an access key")])
            checkpoint fake
                [ adminLookup |>> Right []
                ]
            exec
                (expectResponse $ Response StatusNotFound $ ResponseBodyError ErrUnknownRequest)
                (SimpleRequest $ rqdata [("akey", "01:02")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "user")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("name", "foo"), ("surname", "bar")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "user")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("name", "foo"), ("surname", "bar"), ("user", "not a hex string")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "name")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("name", ""), ("surname", "bar"), ("user", hex "uid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "surname")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("name", "foo"), ("surname", ""), ("user", hex "uid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , UserSetName "uid" "foo" "bar" |>> Left NotFoundError
                ]
            exec
                (expectResponse $ Response StatusNotFound $ ResponseBodyError $ ErrNotFound)
                (SimpleRequest $ rqdata [("akey", "01:02"), ("name", "foo"), ("surname", "bar"), ("user", hex "uid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , UserSetName "uid" "foo" "bar" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "01:02"), ("name", "foo"), ("surname", "bar"), ("user", hex "uid")])
        specifyMethod "users/grantAdmin_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- user :: required user id -}
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "user")
                (SimpleRequest $ rqdata [("akey", "01:02")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "user")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("user", "not an uid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , UserSetIsAdmin "uid" True |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "01:02"), ("user", hex "uid")])
        specifyMethod "users/revokeAdmin_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- user :: required user id -}
            {- confirm :: confirmation ticket (conditionally) -}
            {- confirmation is required when an admin tries to revoke the rights from oneself, that is, when 'user' is the owner of 'akey' -}
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "user")
                (SimpleRequest $ rqdata [("akey", "01:02")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "user")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("user", "not an uid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , UserSetIsAdmin "uid" False |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "01:02"), ("user", hex "uid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , StorageGenerateBytes 4 |>> "tikt"
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyConfirm "tikt")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("user", hex "uida")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , UserSetIsAdmin "uida" False |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "01:02"), ("user", hex "uida"), ("confirm", hex "tikt")])
        specifyMethod "users/delete_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- user :: required user id -}
            {- confirm :: confirmation ticket -}
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "user")
                (SimpleRequest $ rqdata [("akey", "01:02")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , StorageGenerateBytes 4 |>> "tikt"
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyConfirm "tikt")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("user", hex "uida")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , UserDelete "uida" |>> Left NotFoundError
                ]
            exec
                (expectResponse $ Response StatusNotFound $ ResponseBodyError ErrNotFound)
                (SimpleRequest $ rqdata [("akey", "01:02"), ("user", hex "uida"), ("confirm", hex "tikt")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , UserDelete "uida" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "01:02"), ("user", hex "uida"), ("confirm", hex "tikt")])
        specifyMethod "users/list_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- offset :: optional integer, 0 <= offset, default -> 0 -}
            {- limit :: optional integer, 0 < limit <= Config.maxPageLimit, default -> Config.defaultPageLimit -}
            {- order :: optional sorting order spec, columns: "name", "surname" (utf8 lexicographical order), "joinDate" (newest to oldest), "isAdmin" (admins first) -}
            {- author :: optional author id -}
            let users =
                    [ User "uid1" "name1" "surname1" (mkTime "2020-01-01T01:01:01Z") False
                    , User "uid2" "name2" "surname2" (mkTime "2020-01-02T03:04:05Z") True
                    , User "uid3" "name3" "surname3" (mkTime "2020-01-06T12:18:25Z") True
                    ]
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , UserList (ListView 0 10 [] []) |>> Right users
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkUserList $ map ExUser users)
                (SimpleRequest $ rqdata [("akey", "01:02")])
            do
                forM_
                    [ ("0", 0)
                    , ("123456", 123456)
                    , ("1234567890123456789", 1234567890123456789) -- 64-bit integer
                    ] $ \(offsetParam, offsetValue) -> do
                        checkpoint fake
                            [ adminLookup |>> Right [tadmin]
                            , UserList (ListView offsetValue 10 [] []) |>> Left InternalError
                            ]
                        exec
                            (expectResponse $ Response StatusInternalError $ ResponseBodyError ErrInternal)
                            (SimpleRequest $ rqdata [("akey", "01:02"), ("offset", offsetParam)])
                forM_
                    [ ""
                    , "-1"
                    , "not a number"
                    , "4.5"
                    , "123456789012345678901234567890" -- not a 64-bit integer
                    ] $ \offsetParam -> do
                        checkpoint fake
                            [ adminLookup |>> Right [tadmin]
                            ]
                        exec
                            (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "offset")
                            (SimpleRequest $ rqdata [("akey", "01:02"), ("offset", offsetParam)])
            do
                forM_
                    [ ("1", 1)
                    , ("12", 12)
                    , ("100", 100) -- maxPageLimit
                    ] $ \(limitParam, limitValue) -> do
                        checkpoint fake
                            [ adminLookup |>> Right [tadmin]
                            , UserList (ListView 0 limitValue [] []) |>> Left InternalError
                            ]
                        exec
                            (expectResponse $ Response StatusInternalError $ ResponseBodyError ErrInternal)
                            (SimpleRequest $ rqdata [("akey", "01:02"), ("limit", limitParam)])
                forM_
                    [ ""
                    , "0"
                    , "-1"
                    , "101" -- maxPageLimit + 1
                    , "not a number"
                    , "4.5"
                    , "123456789012345678901234567890" -- not a 64-bit integer
                    ] $ \limitParam -> do
                        checkpoint fake
                            [ adminLookup |>> Right [tadmin]
                            ]
                        exec
                            (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "limit")
                            (SimpleRequest $ rqdata [("akey", "01:02"), ("limit", limitParam)])
            do
                forM_
                    [ ("", [])
                    , ("name", [(OrderUserName, Ascending)])
                    , ("surname", [(OrderUserSurname, Ascending)])
                    , ("joinDate", [(OrderUserJoinDate, Ascending)])
                    , ("isAdmin", [(OrderUserIsAdmin, Ascending)])
                    , ("nameAsc", [(OrderUserName, Ascending)])
                    , ("nameDesc", [(OrderUserName, Descending)])
                    , ("nameAsc+joinDateDesc", [(OrderUserName, Ascending), (OrderUserJoinDate, Descending)])
                    ] $ \(orderParam, orderValue) -> do
                        checkpoint fake
                            [ adminLookup |>> Right [tadmin]
                            , UserList (ListView 0 10 [] orderValue) |>> Left InternalError
                            ]
                        exec
                            (expectResponse $ Response StatusInternalError $ ResponseBodyError ErrInternal)
                            (SimpleRequest $ rqdata [("akey", "01:02"), ("order", orderParam)])
                forM_
                    [ "not an order"
                    , "+"
                    , "+nameDesc"
                    , "nameDesc+"
                    , "nameAscsurnameAsc"
                    ] $ \orderParam -> do
                        checkpoint fake
                            [ adminLookup |>> Right [tadmin]
                            ]
                        exec
                            (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "order")
                            (SimpleRequest $ rqdata [("akey", "01:02"), ("order", orderParam)])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "author")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("author", "not a ref")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , UserList (ListView 0 10 [FilterUserAuthorId "aid"] []) |>> Left InternalError
                ]
            exec
                (expectResponse $ Response StatusInternalError $ ResponseBodyError ErrInternal)
                (SimpleRequest $ rqdata [("akey", "01:02"), ("author", hex "aid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , UserList (ListView 1 2 [FilterUserAuthorId "aid"] [(OrderUserName, Descending), (OrderUserJoinDate, Ascending)]) |>> Left InternalError
                ]
            exec
                (expectResponse $ Response StatusInternalError $ ResponseBodyError ErrInternal)
                (SimpleRequest $ rqdata [("akey", "01:02"), ("author", hex "aid"), ("offset", "1"), ("limit", "2"), ("order", "nameDesc+joinDate")])
        specifyMethod "authors/info" $ \(MethodEnv fake exec) -> do
            {- author :: required author id -}
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "author")
                (SimpleRequest $ rqdata [])
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "author")
                (SimpleRequest $ rqdata [("author", "not a reference")])
            checkpoint fake
                [ AuthorList (ListView 0 1 [FilterAuthorId "author"] []) |>> Right []
                ]
            exec
                (expectResponse $ Response StatusNotFound $ ResponseBodyError ErrNotFound)
                (SimpleRequest $ rqdata [("author", hex "author")])
            checkpoint fake
                [ AuthorList (ListView 0 1 [FilterAuthorId "author"] []) |>> Right [tauthor]
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkAuthor $ ExAuthor tauthor)
                (SimpleRequest $ rqdata [("author", hex "author")])
        specifyMethod "authors/create_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- name :: required non-empty text -}
            {- description :: required non-empty text -}
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "name")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("description", "bar")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "name")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("name", "")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "description")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("name", "foo")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "description")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("name", "foo"), ("description", "")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , AuthorCreate "foo" "bar" |>> Right tauthor
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkAuthor $ ExAuthor tauthor)
                (SimpleRequest $ rqdata [("akey", "01:02"), ("name", "foo"), ("description", "bar")])
        specifyMethod "authors/setName_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- author :: required author id -}
            {- name :: required non-empty text -}
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "author")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("name", "foo")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "name")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("author", hex "aid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "name")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("author", hex "aid"), ("name", "")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , AuthorSetName "aid" "foo" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "01:02"), ("author", hex "aid"), ("name", "foo")])
        specifyMethod "authors/setDescription_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- author :: required author id -}
            {- description :: required non-empty text -}
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "author")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("description", "foo")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "description")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("author", hex "aid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "description")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("author", hex "aid"), ("description", "")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , AuthorSetDescription "aid" "foo" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "01:02"), ("author", hex "aid"), ("description", "foo")])
        specifyMethod "authors/delete_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- author :: required author id -}
            {- confirm :: confirmation ticket -}
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "author")
                (SimpleRequest $ rqdata [("akey", "01:02")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , StorageGenerateBytes 4 |>> "tikt"
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyConfirm "tikt")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("author", hex "aida")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , AuthorDelete "aida" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "01:02"), ("author", hex "aida"), ("confirm", hex "tikt")])
        specifyMethod "authors/list_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- offset :: optional integer, 0 <= offset, default -> 0 -}
            {- limit :: optional integer, 0 < limit <= Config.maxPageLimit, default -> Config.defaultPageLimit -}
            {- order :: optional sorting order spec, columns: "name" (utf8 lexicographical order) -}
            {- user :: optional user id -}
            let authors =
                    [ Author "aid1" "name1" "description1"
                    , Author "aid2" "name2" "description2"
                    , Author "aid3" "name3" "description3"
                    ]
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , AuthorList (ListView 0 10 [] []) |>> Right authors
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkAuthorList $ map ExAuthor authors)
                (SimpleRequest $ rqdata [("akey", "01:02")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , AuthorList (ListView 50 7 [] []) |>> Right authors
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkAuthorList $ map ExAuthor authors)
                (SimpleRequest $ rqdata [("akey", "01:02"), ("offset", "50"), ("limit", "7")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , AuthorList (ListView 0 10 [] [(OrderAuthorName, Ascending)]) |>> Left InternalError
                ]
            exec
                (expectResponse $ Response StatusInternalError $ ResponseBodyError ErrInternal)
                (SimpleRequest $ rqdata [("akey", "01:02"), ("order", "name")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "user")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("user", "not a ref")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , AuthorList (ListView 0 10 [FilterAuthorUserId "uid"] []) |>> Left InternalError
                ]
            exec
                (expectResponse $ Response StatusInternalError $ ResponseBodyError ErrInternal)
                (SimpleRequest $ rqdata [("akey", "01:02"), ("user", hex "uid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , AuthorList (ListView 1 2 [FilterAuthorUserId "uid"] [(OrderAuthorName, Descending)]) |>> Left InternalError
                ]
            exec
                (expectResponse $ Response StatusInternalError $ ResponseBodyError ErrInternal)
                (SimpleRequest $ rqdata [("akey", "01:02"), ("user", hex "uid"), ("offset", "1"), ("limit", "2"), ("order", "nameDesc")])
        specifyMethod "authors/addOwner_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- author :: required author id -}
            {- user :: required user id -}
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "author")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("user", hex "uid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "user")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("author", hex "aid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , AuthorSetOwnership "aid" "uid" True |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "01:02"), ("author", hex "aid"), ("user", hex "uid")])
        specifyMethod "authors/removeOwner_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- author :: required author id -}
            {- user :: required user id -}
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "author")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("user", hex "uid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "user")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("author", hex "aid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , AuthorSetOwnership "aid" "uid" False |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "01:02"), ("author", hex "aid"), ("user", hex "uid")])
        specifyMethod "files/upload" $ \(MethodEnv fake exec) -> do
            {- akey :: required user access key -}
            {- article :: required article id -}
            {- user must possess the article -}
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "article")
                (SimpleRequest $ rqdata [("akey", "01:02")])
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "article")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("article", "not an id")])
            checkpoint fake
                [ userLookup |>> Right [tuser]
                , ArticleList (ListView 0 1 [FilterArticleId "aid", FilterArticleUserId "uid"] []) |>> Right []
                ]
            exec
                (expectResponse $ Response StatusForbidden $ ResponseBodyError ErrArticleNotEditable)
                (SimpleRequest $ rqdata [("akey", "01:02"), ("article", hex "aid")])
            checkpoint fake
                [ userLookup |>> Right [tuser]
                , ArticleList (ListView 0 1 [FilterArticleId "aid", FilterArticleUserId "uid"] []) |>> Right [tarticle]
                , StorageGenerateBytes 4 |>> "uptk"
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyFollow $ "host/upload/" <> hex "uptk")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("article", hex "aid")])
        specify "upload/..." $ \(TestEnv fake jint) -> do
            checkpoint fake
                [ userLookup |>> Right [tuser]
                , ArticleList (ListView 0 1 [FilterArticleId "aid", FilterArticleUserId "uid"] []) |>> Right [tarticle]
                , StorageGenerateBytes 4 |>> "uptk"
                ]
            executeRequest jint ["files", "upload"]
                (expectResponse $ Response StatusOk $ ResponseBodyFollow $ "host/upload/" <> hex "uptk")
                (SimpleRequest $ rqdata [("akey", "01:02"), ("article", hex "aid")])
            executeRequest jint ["upload", hex "uptk"]
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError ErrInvalidRequest)
                (SimpleRequest $ rqdata [("akey", "01:02"), ("article", hex "aid")])
            checkpoint fake
                [ StorageUpload "file1.txt" "text/plain" "aid" "uid"
                    |>> Right (UploadProcess
                        (FileInfo "fid1" "file1.txt" "text/plain" (mkTime "2020-01-01T01:01:01Z") "aid" 0 "uid")
                        ["1234567890", "1234567890", "1234567890", "1234567890", "1234567890"]
                        (Right (True, Nothing)))
                , StorageUpload "file2.txt" "text/plain" "aid" "uid"
                    |>> Right (UploadProcess
                        (FileInfo "fid2" "file2.txt" "text/plain" (mkTime "2020-01-01T01:01:02Z") "aid" 1 "uid")
                        ["1234567890", "1234567"]
                        (Right (True, Nothing)))
                , StorageUpload "file6.txt" "text/plain" "aid" "uid"
                    |>> Right (UploadProcess
                        (FileInfo "fid6" "file6.txt" "text/plain" (mkTime "2020-01-01T01:01:06Z") "aid" 3 "uid")
                        ["1234567890", "1234567890", "1234567890", "1234567890"]
                        (Right (False, Nothing)))
                , StorageUpload "file7.txt" "text/plain" "aid" "uid"
                    |>> Right (UploadProcess
                        (FileInfo "fid7" "file7.txt" "text/plain" (mkTime "2020-01-01T01:01:07Z") "aid" 3 "uid")
                        ["1234567890"]
                        (Left InternalError))
                , ArticleList (ListView 0 1 [FilterArticleId "aid"] []) |>> Right [tarticle]
                , AuthorList (ListView 0 1 [FilterAuthorId "author"] []) |>> Right [tauthor]
                , CategoryList (ListView 0 1 [FilterCategoryId "cat1"] []) |>> Right [tcategory1]
                , CategoryList (ListView 0 1 [FilterCategoryId "cat2"] []) |>> Right [tcategory2]
                , TagList (ListView 0 maxBound [FilterTagArticleId "aid"] [(OrderTagName, Ascending)]) |>> Right [ttag1, ttag2]
                , UserList (ListView 0 1 [FilterUserId "uid"] []) |>> Right [tuser]
                ]
            executeRequest jint ["upload", hex "uptk"]
                (expectResponse $ Response StatusOk $ ResponseBodyUploadStatusList
                    [ ExUploadStatus "param1" $ Right $ ExFileInfo
                        "fid1" "file1.txt" "text/plain" (mkTime "2020-01-01T01:01:01Z")
                        (Just tarticleEx) 0 (Just $ ExUser tuser) ("host/get/" <> hex "fid1" <> "/file1.txt")
                    , ExUploadStatus "param2" $ Right $ ExFileInfo
                        "fid2" "file2.txt" "text/plain" (mkTime "2020-01-01T01:01:02Z")
                        (Just tarticleEx) 1 (Just $ ExUser tuser) ("host/get/" <> hex "fid2" <> "/file2.txt")
                    , ExUploadStatus "param3\xfffd" $ Left ErrInvalidRequest
                    , ExUploadStatus "param4" $ Left $ ErrInvalidParameter "fileName"
                    , ExUploadStatus "param5" $ Left $ ErrInvalidParameter "mimeType"
                    , ExUploadStatus "param6" $ Left ErrFileTooLarge
                    , ExUploadStatus "param7" $ Left ErrInternal
                    ])
                (UploadRequest $ \uploadFile -> do
                    testSendFile uploadFile "param1" "file1.txt" "text/plain"
                        ["12345678901234567890123456789012345678901234567890"]
                    testSendFile uploadFile "param2" "file2.txt" "text/plain" -- chunk reshaping
                        ["123456", "7890123", "4567"]
                    testSendFile uploadFile "param3\255" "file3.txt" "text/plain" -- parameter name is not valid utf8
                        ["1"]
                    testSendFile uploadFile "param4" "\255file4.txt" "text/plain" -- filename is invalid
                        ["1"]
                    testSendFile uploadFile "param5" "file5.txt" "text/\255" -- mime type is invalid
                        ["1"]
                    testSendFile uploadFile "param6" "file6.txt" "text/plain" -- file is too large
                        ["12345678901234567890123456789012345678901234", "567890123456", "7", "8", "9"] -- trailing chunks must be drained nevertheless
                    testSendFile uploadFile "param7" "file7.txt" "text/plain" -- server-side error
                        ["123456", "7890123", "456"]
                    return $ rqdata [])
        specify "get/..." $ \(TestEnv fake jint) -> do
            checkpoint fake
                [ StorageDownload "fid1" |>>
                    Right (DownloadProcess "text/plain" ["1234567890", "1234567890", "123"])
                ]
            executeRequest jint ["get", hex "fid1", "filename that should be ignored"]
                (expectStream "text/plain" ["1234567890", "1234567890", "123"])
                (SimpleRequest $ rqdata [])
            checkpoint fake
                [ StorageDownload "fid2" |>> Left NotFoundError
                ]
            executeRequest jint ["get", hex "fid2", "filename that should be ignored"]
                (expectResponse $ Response StatusNotFound $ ResponseBodyError ErrNotFound)
                (SimpleRequest $ rqdata [])
