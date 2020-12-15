{-# LANGUAGE StandaloneDeriving #-}

module SN.MediumSpec
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
import SN.Data.Base64
import SN.Ground.Interface
import SN.Ground.Fake
import SN.Logger
import SN.Medium

mkTime :: String -> TClock.UTCTime
mkTime = fromJust . TFormat.iso8601ParseM

testConfig :: MediumConfig
testConfig = MediumConfig
    { mediumConfigDefaultPageLimit = 10
    , mediumConfigMaxPageLimit = 100
    , mediumConfigMinPasswordLength = 4
    , mediumConfigMaxAccessKeyCount = 3
    , mediumConfigTicketLength = 4
    , mediumConfigTicketLifetime = 10
    , mediumConfigFileChunkSize = 10
    , mediumConfigMaxFileSize = 50
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

data TestEnv = TestEnv FakeGround Medium

withTestEnv :: (TestEnv -> IO r) -> IO r
withTestEnv inner = do
    withTestLogger $ \logger -> do
        withFakeGround $ \fake ground -> do
            withMedium testConfig logger ground $ \medium -> do
                inner $ TestEnv fake medium

data MethodEnv = MethodEnv FakeGround (Accept () -> Request -> IO ())

specifyMethod :: String -> (MethodEnv -> IO ()) -> SpecWith TestEnv
specifyMethod uri inner = do
    let uriParts = Text.splitOn "/" $ Text.pack $ takeWhile (/= ';') uri
    specify uri $ \(TestEnv fake medium) -> do
        inner $ MethodEnv fake (executeRequest medium uriParts)

spec :: Spec
spec = around withTestEnv $ do
    describe "Medium" $ do
        let userLookup = UserList (ListView 0 1 [FilterUserAccessKey $ AccessKey (Reference "\x01") "\x02"] [])
        let tuser = User "uid" "foo" "bar" (mkTime "2020-01-02T03:04:05.06Z") False
        let adminLookup = UserList (ListView 0 1 [FilterUserAccessKey $ AccessKey (Reference "\x01") "\x02", FilterUserIsAdmin True] [])
        let tadmin = User "uida" "fooa" "bara" (mkTime "2020-01-01T02:02:02.33Z") True
        let tauthor = Author "author" "authorname" "authordesc"
        let tcategory1 = Category "cat1" "cat name 1" "cat2" "cat name 2"
        let tcategory2 = Category "cat2" "cat name 2" "" ""
        let tarticle = Article "aid" "aver" "author" "aname" (PublishAt $ mkTime "2020-02-01T01:01:01Z") "cat1"
        let ttag1 = Tag "tag1" "tag name 1"
        let ttag2 = Tag "tag2" "tag name 2"
        let tarticleEx = ExArticle "aid" "aver"
                (Just $ ExAuthor tauthor)
                "aname"
                (PublishAt $ mkTime "2020-02-01T01:01:01Z")
                [ExCategory tcategory1, ExCategory tcategory2]
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
                [ UserCreate "foo" "bar" "1234" False |>> Right tuser
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkUser $ ExUser tuser)
                (SimpleRequest $ rqdata [("name", "foo"), ("surname", "bar"), ("newPassword", "1234")])
            checkpoint fake
                [ UserCreate "foo" "bar" "abc\255" False |>> Right tuser
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
                (SimpleRequest $ rqdata [("user", toBase64 "uid1")])
            checkpoint fake
                [ UserList (ListView 0 1 [FilterUserId "uid1"] []) |>> Right [tuser]
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkUser $ ExUser tuser)
                (SimpleRequest $ rqdata [("user", toBase64 "uid1")])
        specifyMethod "users/me; also user authorization" $ \(MethodEnv fake exec) -> do
            {- akey :: required user access key -}
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "akey")
                (SimpleRequest $ rqdata [])
            let invalidAkeys =
                    [ ""
                    , "0001"
                    , "0002:56:78"
                    , "0003:AAA="
                    , "0004:A"
                    , "00056:AAAA"
                    , "+=AA:3210"
                    ]
            forM_ invalidAkeys $ \akey -> do
                exec
                    (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "akey")
                    (SimpleRequest $ rqdata [("akey", akey)])
            let validAkeys =
                    [ ("ASNFZw:iavN7w", "\x01\x23\x45\x67", "\x89\xab\xcd\xef")
                    , ("_g:MhA", "\xfe", "\x32\x10")
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
                (SimpleRequest $ rqdata [("akey", "AQI:AwQ"), ("name", "foo"), ("surname", "")])
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "name")
                (SimpleRequest $ rqdata [("akey", "AQI:AwQ"), ("surname", "bar")])
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "name")
                (SimpleRequest $ rqdata [("akey", "AQI:AwQ"), ("name", ""), ("surname", "bar")])
            checkpoint fake
                [ UserList (ListView 0 1 [FilterUserAccessKey $ AccessKey (Reference "\x01\x02") "\x03\x04"] []) |>> Right []
                ]
            exec
                (expectResponse $ Response StatusForbidden $ ResponseBodyError ErrInvalidAccessKey)
                (SimpleRequest $ rqdata [("akey", "AQI:AwQ"), ("name", "foo"), ("surname", "bar")])
            checkpoint fake
                [ UserList (ListView 0 1 [FilterUserAccessKey $ AccessKey (Reference "\x01\x02") "\x03\x04"] []) |>> Right [tuser]
                , UserSetName "uid" "foo" "bar" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "AQI:AwQ"), ("name", "foo"), ("surname", "bar")])
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
                (SimpleRequest $ rqdata [("user", toBase64 "uid1"), ("newPassword", "abcd")])
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "newPassword")
                (SimpleRequest $ rqdata [("user", toBase64 "uid1"), ("password", "1234")])
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "newPassword")
                (SimpleRequest $ rqdata [("user", toBase64 "uid1"), ("password", "1234"), ("newPassword", "a")])
            checkpoint fake
                [ UserCheckPassword "uid1" "1234" |>> Left NotFoundError
                ]
            exec
                (expectResponse $ Response StatusForbidden $ ResponseBodyError ErrInvalidAccessKey)
                (SimpleRequest $ rqdata [("user", toBase64 "uid1"), ("password", "1234"), ("newPassword", "abcd")])
            checkpoint fake
                [ UserCheckPassword "uid1" "1234" |>> Right ()
                , UserSetPassword "uid1" "abcd" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("user", toBase64 "uid1"), ("password", "1234"), ("newPassword", "abcd")])
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
                    , GroundGenerateBytes 4 |>> "tikt"
                    ]
                exec
                    (expectResponse $ Response StatusOk $ ResponseBodyConfirm "tikt")
                    (SimpleRequest $ rqdata $ ticket ++ [("akey", "AQ:Ag")])
            checkpoint fake
                [ UserList (ListView 0 1 [FilterUserAccessKey $ AccessKey (Reference "\x03") "\x04"] []) |>> Right [tuser2]
                , GroundGenerateBytes 4 |>> "2222"
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyConfirm "2222")
                (SimpleRequest $ rqdata [("akey", "Aw:BA"), ("confirm", toBase64 "tikt")])
            checkpoint fake
                [ UserList (ListView 0 1 [FilterUserAccessKey $ AccessKey (Reference "\x01") "\x02"] []) |>> Right [tuser1]
                , UserDelete "uid1" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("confirm", toBase64 "tikt")])
            checkpoint fake
                [ UserList (ListView 0 1 [FilterUserAccessKey $ AccessKey (Reference "\x03") "\x04"] []) |>> Right [tuser2]
                , UserDelete "uid2" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "Aw:BA"), ("confirm", toBase64 "2222")])
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
                (SimpleRequest $ rqdata [("user", toBase64 "uid1")])
            checkpoint fake
                [ UserCheckPassword "uid1" "1234" |>> Left NotFoundError
                ]
            exec
                (expectResponse $ Response StatusForbidden $ ResponseBodyError ErrInvalidAccessKey)
                (SimpleRequest $ rqdata [("user", toBase64 "uid1"), ("password", "1234")])
            checkpoint fake
                [ UserCheckPassword "uid1" "1234" |>> Right ()
                , AccessKeyList "uid1" (ListView 0 maxBound [] []) |>> Right ["a1", "a2", "a3"]
                ]
            exec
                (expectResponse $ Response StatusForbidden $ ResponseBodyError ErrLimitExceeded)
                (SimpleRequest $ rqdata [("user", toBase64 "uid1"), ("password", "1234")])
            checkpoint fake
                [ UserCheckPassword "uid1" "1234" |>> Right ()
                , AccessKeyList "uid1" (ListView 0 maxBound [] []) |>> Right ["a1", "a2"]
                , AccessKeyCreate "uid1" |>> Right (AccessKey "akid" "aktok")
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkAccessKey (AccessKey "akid" "aktok"))
                (SimpleRequest $ rqdata [("user", toBase64 "uid1"), ("password", "1234")])
        specifyMethod "users/listAccessKeys" $ \(MethodEnv fake exec) -> do
            {- user :: required user id -}
            {- password :: required current password -}
            checkpoint fake
                [ UserCheckPassword "uid1" "1234" |>> Right ()
                , AccessKeyList "uid1" (ListView 0 maxBound [] []) |>> Right ["a1", "a2", "a3"]
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkAccessKeyList ["a1", "a2", "a3"])
                (SimpleRequest $ rqdata [("user", toBase64 "uid1"), ("password", "1234")])
        specifyMethod "users/clearAccessKeys" $ \(MethodEnv fake exec) -> do
            {- user :: required user id -}
            {- password :: required current password -}
            checkpoint fake
                [ UserCheckPassword "uid1" "1234" |>> Right ()
                , AccessKeyClear "uid1" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("user", toBase64 "uid1"), ("password", "1234")])
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
                (SimpleRequest $ rqdata [("akey", "AQ:Ag")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "user")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("name", "foo"), ("surname", "bar")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "user")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("name", "foo"), ("surname", "bar"), ("user", "not a hex string")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "name")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("name", ""), ("surname", "bar"), ("user", toBase64 "uid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "surname")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("name", "foo"), ("surname", ""), ("user", toBase64 "uid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , UserSetName "uid" "foo" "bar" |>> Left NotFoundError
                ]
            exec
                (expectResponse $ Response StatusNotFound $ ResponseBodyError $ ErrNotFound)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("name", "foo"), ("surname", "bar"), ("user", toBase64 "uid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , UserSetName "uid" "foo" "bar" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("name", "foo"), ("surname", "bar"), ("user", toBase64 "uid")])
        specifyMethod "users/grantAdmin_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- user :: required user id -}
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "user")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "user")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("user", "not an uid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , UserSetIsAdmin "uid" True |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("user", toBase64 "uid")])
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
                (SimpleRequest $ rqdata [("akey", "AQ:Ag")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "user")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("user", "not an uid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , UserSetIsAdmin "uid" False |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("user", toBase64 "uid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , GroundGenerateBytes 4 |>> "tikt"
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyConfirm "tikt")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("user", toBase64 "uida")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , UserSetIsAdmin "uida" False |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("user", toBase64 "uida"), ("confirm", toBase64 "tikt")])
        specifyMethod "users/delete_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- user :: required user id -}
            {- confirm :: confirmation ticket -}
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "user")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , GroundGenerateBytes 4 |>> "tikt"
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyConfirm "tikt")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("user", toBase64 "uida")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , UserDelete "uida" |>> Left NotFoundError
                ]
            exec
                (expectResponse $ Response StatusNotFound $ ResponseBodyError ErrNotFound)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("user", toBase64 "uida"), ("confirm", toBase64 "tikt")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , UserDelete "uida" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("user", toBase64 "uida"), ("confirm", toBase64 "tikt")])
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
                (SimpleRequest $ rqdata [("akey", "AQ:Ag")])
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
                            (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("offset", offsetParam)])
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
                            (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("offset", offsetParam)])
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
                            (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("limit", limitParam)])
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
                            (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("limit", limitParam)])
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
                            (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("order", orderParam)])
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
                            (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("order", orderParam)])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "author")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("author", "not a ref")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , UserList (ListView 0 10 [FilterUserAuthorId "aid"] []) |>> Left InternalError
                ]
            exec
                (expectResponse $ Response StatusInternalError $ ResponseBodyError ErrInternal)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("author", toBase64 "aid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , UserList (ListView 1 2 [FilterUserAuthorId "aid"] [(OrderUserName, Descending), (OrderUserJoinDate, Ascending)]) |>> Left InternalError
                ]
            exec
                (expectResponse $ Response StatusInternalError $ ResponseBodyError ErrInternal)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("author", toBase64 "aid"), ("offset", "1"), ("limit", "2"), ("order", "nameDesc+joinDate")])
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
                (SimpleRequest $ rqdata [("author", toBase64 "author")])
            checkpoint fake
                [ AuthorList (ListView 0 1 [FilterAuthorId "author"] []) |>> Right [tauthor]
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkAuthor $ ExAuthor tauthor)
                (SimpleRequest $ rqdata [("author", toBase64 "author")])
        specifyMethod "authors/mine" $ \(MethodEnv fake exec) -> do
            {- akey :: required user access key -}
            {- offset :: optional integer, 0 <= offset, default -> 0 -}
            {- limit :: optional integer, 0 < limit <= Config.maxPageLimit, default -> Config.defaultPageLimit -}
            {- order :: optional sorting order spec, columns: "name" (utf8 lexicographical order) -}
            let authors =
                    [ Author "aid1" "name1" "description1"
                    , Author "aid2" "name2" "description2"
                    , Author "aid3" "name3" "description3"
                    ]
            checkpoint fake
                [ userLookup |>> Right [tuser]
                , AuthorList (ListView 0 10 [FilterAuthorUserId "uid"] []) |>> Right authors
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkAuthorList $ map ExAuthor authors)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag")])
            checkpoint fake
                [ userLookup |>> Right [tuser]
                , AuthorList (ListView 50 7 [FilterAuthorUserId "uid"] []) |>> Right authors
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkAuthorList $ map ExAuthor authors)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("offset", "50"), ("limit", "7")])
            checkpoint fake
                [ userLookup |>> Right [tuser]
                , AuthorList (ListView 1 2 [FilterAuthorUserId "uid"] [(OrderAuthorName, Descending)]) |>> Right []
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkAuthorList [])
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("offset", "1"), ("limit", "2"), ("order", "nameDesc")])
        specifyMethod "authors/owners" $ \(MethodEnv fake exec) -> do
            {- akey :: required user access key -}
            {- author :: required author id -}
            {- offset :: optional integer, 0 <= offset, default -> 0 -}
            {- limit :: optional integer, 0 < limit <= Config.maxPageLimit, default -> Config.defaultPageLimit -}
            {- order :: optional sorting order spec, columns: "name", "surname" (utf8 lexicographical order), "joinDate" (newest to oldest), "isAdmin" (admins first) -}
            let users =
                    [ User "uid1" "name1" "surname1" (mkTime "2020-01-01T01:01:01Z") False
                    , User "uid2" "name2" "surname2" (mkTime "2020-01-02T03:04:05Z") True
                    , User "uid3" "name3" "surname3" (mkTime "2020-01-06T12:18:25Z") True
                    ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "author")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag")])
            checkpoint fake
                [ userLookup |>> Right [tuser]
                , AuthorList (ListView 0 1 [FilterAuthorId "aid", FilterAuthorUserId "uid"] []) |>> Right []
                ]
            exec
                (expectResponse $ Response StatusForbidden $ ResponseBodyError ErrAuthorNotOwned)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("author", toBase64 "aid")])
            checkpoint fake
                [ userLookup |>> Right [tuser]
                , AuthorList (ListView 0 1 [FilterAuthorId "aid", FilterAuthorUserId "uid"] []) |>> Right [tauthor]
                , UserList (ListView 0 10 [FilterUserAuthorId "aid"] []) |>> Right [tuser]
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkUserList [ExUser tuser])
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("author", toBase64 "aid")])
            checkpoint fake
                [ userLookup |>> Right [tuser]
                , AuthorList (ListView 0 1 [FilterAuthorId "aid", FilterAuthorUserId "uid"] []) |>> Right [tauthor]
                , UserList (ListView 1 2 [FilterUserAuthorId "aid"] [(OrderUserName, Ascending), (OrderUserJoinDate, Descending)]) |>> Right [tuser]
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkUserList [ExUser tuser])
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("author", toBase64 "aid"), ("offset", "1"), ("limit", "2"), ("order", "name+joinDateDesc")])
        specifyMethod "authors/create_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- name :: required non-empty text -}
            {- description :: required non-empty text -}
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "name")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("description", "bar")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "name")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("name", ""), ("description", "bar")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "description")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("name", "foo")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "description")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("name", "foo"), ("description", "")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , AuthorCreate "foo" "bar" |>> Right tauthor
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkAuthor $ ExAuthor tauthor)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("name", "foo"), ("description", "bar")])
        specifyMethod "authors/setName_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- author :: required author id -}
            {- name :: required non-empty text -}
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "author")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("name", "foo")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "name")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("author", toBase64 "aid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "name")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("author", toBase64 "aid"), ("name", "")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , AuthorSetName "aid" "foo" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("author", toBase64 "aid"), ("name", "foo")])
        specifyMethod "authors/setDescription_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- author :: required author id -}
            {- description :: required non-empty text -}
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "author")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("description", "foo")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "description")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("author", toBase64 "aid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "description")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("author", toBase64 "aid"), ("description", "")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , AuthorSetDescription "aid" "foo" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("author", toBase64 "aid"), ("description", "foo")])
        specifyMethod "authors/delete_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- author :: required author id -}
            {- confirm :: confirmation ticket -}
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "author")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , GroundGenerateBytes 4 |>> "tikt"
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyConfirm "tikt")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("author", toBase64 "aida")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , AuthorDelete "aida" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("author", toBase64 "aida"), ("confirm", toBase64 "tikt")])
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
                (SimpleRequest $ rqdata [("akey", "AQ:Ag")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , AuthorList (ListView 50 7 [] []) |>> Right authors
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkAuthorList $ map ExAuthor authors)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("offset", "50"), ("limit", "7")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , AuthorList (ListView 0 10 [] [(OrderAuthorName, Ascending)]) |>> Left InternalError
                ]
            exec
                (expectResponse $ Response StatusInternalError $ ResponseBodyError ErrInternal)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("order", "name")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "user")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("user", "not a ref")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , AuthorList (ListView 0 10 [FilterAuthorUserId "uid"] []) |>> Left InternalError
                ]
            exec
                (expectResponse $ Response StatusInternalError $ ResponseBodyError ErrInternal)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("user", toBase64 "uid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , AuthorList (ListView 1 2 [FilterAuthorUserId "uid"] [(OrderAuthorName, Descending)]) |>> Left InternalError
                ]
            exec
                (expectResponse $ Response StatusInternalError $ ResponseBodyError ErrInternal)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("user", toBase64 "uid"), ("offset", "1"), ("limit", "2"), ("order", "nameDesc")])
        specifyMethod "authors/addOwner_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- author :: required author id -}
            {- user :: required user id -}
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "author")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("user", toBase64 "uid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "user")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("author", toBase64 "aid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , AuthorSetOwnership "aid" "uid" True |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("author", toBase64 "aid"), ("user", toBase64 "uid")])
        specifyMethod "authors/removeOwner_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- author :: required author id -}
            {- user :: required user id -}
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "author")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("user", toBase64 "uid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "user")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("author", toBase64 "aid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , AuthorSetOwnership "aid" "uid" False |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("author", toBase64 "aid"), ("user", toBase64 "uid")])
        specifyMethod "categories/info" $ \(MethodEnv fake exec) -> do
            {- category :: required category id -}
            let catList = [tcategory1, tcategory2]
            let exCatList = [ExCategory tcategory1, ExCategory tcategory2]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "category")
                (SimpleRequest $ rqdata [])
            checkpoint fake
                [ CategoryAncestry "cat1" |>> Right catList
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkCategoryAncestryList exCatList)
                (SimpleRequest $ rqdata [("category", toBase64 "cat1")])
            checkpoint fake
                [ CategoryAncestry "cat1" |>> Right []
                ]
            exec
                (expectResponse $ Response StatusNotFound $ ResponseBodyError ErrNotFound)
                (SimpleRequest $ rqdata [("category", toBase64 "cat1")])
        specifyMethod "categories/list" $ \(MethodEnv fake exec) -> do
            {- offset :: optional integer, 0 <= offset, default -> 0 -}
            {- limit :: optional integer, 0 < limit <= Config.maxPageLimit, default -> Config.defaultPageLimit -}
            {- order :: optional sorting order spec, columns: "name" (utf8 lexicographical order) -}
            {- parent :: optional category id -}
            {- strict :: flag -}
            let catList = [tcategory1, tcategory2]
            let exCatList = [ExCategory tcategory1, ExCategory tcategory2]
            checkpoint fake
                [ CategoryList (ListView 0 10 [] []) |>> Right catList
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkCategoryList exCatList)
                (SimpleRequest $ rqdata [])
            checkpoint fake
                [ CategoryList (ListView 50 7 [] []) |>> Right catList
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkCategoryList exCatList)
                (SimpleRequest $ rqdata [("offset", "50"), ("limit", "7")])
            checkpoint fake
                [ CategoryList (ListView 0 10 [] [(OrderCategoryName, Ascending)]) |>> Left InternalError
                ]
            exec
                (expectResponse $ Response StatusInternalError $ ResponseBodyError ErrInternal)
                (SimpleRequest $ rqdata [("order", "name")])
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "parent")
                (SimpleRequest $ rqdata [("parent", "not a ref")])
            checkpoint fake
                [ CategoryList (ListView 0 10 [FilterCategoryTransitiveParentId "cid"] []) |>> Left InternalError
                ]
            exec
                (expectResponse $ Response StatusInternalError $ ResponseBodyError ErrInternal)
                (SimpleRequest $ rqdata [("parent", toBase64 "cid")])
            checkpoint fake
                [ CategoryList (ListView 1 2 [FilterCategoryTransitiveParentId "cid"] [(OrderCategoryName, Descending)]) |>> Left InternalError
                ]
            exec
                (expectResponse $ Response StatusInternalError $ ResponseBodyError ErrInternal)
                (SimpleRequest $ rqdata [("parent", toBase64 "cid"), ("offset", "1"), ("limit", "2"), ("order", "nameDesc")])
            checkpoint fake
                [ CategoryList (ListView 0 10 [FilterCategoryParentId "cid"] []) |>> Left InternalError
                ]
            exec
                (expectResponse $ Response StatusInternalError $ ResponseBodyError ErrInternal)
                (SimpleRequest $ rqdata [("parent", toBase64 "cid"), ("strict", "does not matter")])
            checkpoint fake
                [ CategoryList (ListView 1 2 [FilterCategoryParentId "cid"] [(OrderCategoryName, Descending)]) |>> Left InternalError
                ]
            exec
                (expectResponse $ Response StatusInternalError $ ResponseBodyError ErrInternal)
                (SimpleRequest $ rqdata [("parent", toBase64 "cid"), ("strict", ""), ("offset", "1"), ("limit", "2"), ("order", "nameDesc")])
            checkpoint fake
                [ CategoryList (ListView 1 2 [] []) |>> Left InternalError
                ]
            exec
                (expectResponse $ Response StatusInternalError $ ResponseBodyError ErrInternal)
                (SimpleRequest $ rqdata [("strict", "ignored without parent"), ("offset", "1"), ("limit", "2")])
            checkpoint fake
                [ CategoryList (ListView 0 10 [FilterCategoryParentId ""] []) |>> Left InternalError
                ]
            exec
                (expectResponse $ Response StatusInternalError $ ResponseBodyError ErrInternal)
                (SimpleRequest $ rqdata [("parent", "."), ("strict", "parent is null")])
        specifyMethod "categories/create_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- name :: required non-empty text -}
            {- parent :: required category id -}
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "name")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("parent", "cat1")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "name")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("name", ""), ("parent", toBase64 "cat1")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "parent")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("name", "foo")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , CategoryAncestry "cat1" |>> Right []
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "parent")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("name", "foo"), ("parent", toBase64 "cat1")])
            let newcat = Category "cat0" "foo" "cat1" "cat name 1"
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , CategoryAncestry "cat1" |>> Right [tcategory1, tcategory2]
                , CategoryCreate "foo" "cat1" |>> Right newcat
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkCategoryAncestryList $ map ExCategory [newcat, tcategory1, tcategory2])
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("name", "foo"), ("parent", toBase64 "cat1")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , CategoryCreate "foo" "" |>> Right (Category "cat3" "foo" "" "")
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkCategoryAncestryList [ExCategory $ Category "cat3" "foo" "" ""])
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("name", "foo"), ("parent", "")])
        specifyMethod "categories/setName_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- category :: required category id -}
            {- name :: required non-empty text -}
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "category")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("name", "foo")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "name")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("category", toBase64 "cid"), ("name", "")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , CategorySetName "cid" "foo" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("category", toBase64 "cid"), ("name", "foo")])
        specifyMethod "categories/setParent_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- category :: required category id -}
            {- parent :: required category id -}
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "category")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("parent", toBase64 "cid2")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "parent")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("category", toBase64 "cid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , CategorySetParent "cid" "cid2" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("category", toBase64 "cid"), ("parent", toBase64 "cid2")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , CategorySetParent "cid" "" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("category", toBase64 "cid"), ("parent", ".")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , CategorySetParent "cid" "cid2" |>> Left CyclicReferenceError
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError ErrCyclicReference)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("category", toBase64 "cid"), ("parent", toBase64 "cid2")])
        specifyMethod "categories/delete_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- category :: required category id -}
            {- confirm :: confirmation ticket -}
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "category")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , GroundGenerateBytes 4 |>> "tikt"
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyConfirm "tikt")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("category", toBase64 "cid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , CategoryDelete "cid" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("category", toBase64 "cid"), ("confirm", toBase64 "tikt")])
        specifyMethod "tags/info" $ \(MethodEnv fake exec) -> do
            {- tag :: required tag id -}
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "tag")
                (SimpleRequest $ rqdata [])
            checkpoint fake
                [ TagList (ListView 0 1 [FilterTagId "tag1"] []) |>> Right [ttag1]
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkTag $ ExTag ttag1)
                (SimpleRequest $ rqdata [("tag", toBase64 "tag1")])
            checkpoint fake
                [ TagList (ListView 0 1 [FilterTagId "tag1"] []) |>> Right []
                ]
            exec
                (expectResponse $ Response StatusNotFound $ ResponseBodyError ErrNotFound)
                (SimpleRequest $ rqdata [("tag", toBase64 "tag1")])
        specifyMethod "tags/list" $ \(MethodEnv fake exec) -> do
            {- offset :: optional integer, 0 <= offset, default -> 0 -}
            {- limit :: optional integer, 0 < limit <= Config.maxPageLimit, default -> Config.defaultPageLimit -}
            {- order :: optional sorting order spec, columns: "name" (utf8 lexicographical order) -}
            let tagList = [ttag1, ttag2]
            let exTagList = [ExTag ttag1, ExTag ttag2]
            checkpoint fake
                [ TagList (ListView 0 10 [] []) |>> Right tagList
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkTagList exTagList)
                (SimpleRequest $ rqdata [])
            checkpoint fake
                [ TagList (ListView 50 7 [] [(OrderTagName, Ascending)]) |>> Right tagList
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkTagList exTagList)
                (SimpleRequest $ rqdata [("offset", "50"), ("limit", "7"), ("order", "name")])
        specifyMethod "tags/create_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- name :: required non-empty text -}
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "name")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "name")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("name", "")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , TagCreate "foo" |>> Right (Tag "tid" "foo")
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyOkTag $ ExTag $ Tag "tid" "foo")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("name", "foo")])
        specifyMethod "tags/setName_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- tag :: required tag id -}
            {- name :: required non-empty text -}
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "tag")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("name", "foo")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "name")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("tag", toBase64 "tid"), ("name", "")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , TagSetName "tid" "foo" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("tag", toBase64 "tid"), ("name", "foo")])
        specifyMethod "tags/delete_" $ \(MethodEnv fake exec) -> do
            {- akey :: required admin access key -}
            {- tag :: required tag id -}
            {- confirm :: confirmation ticket -}
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                ]
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "tag")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , GroundGenerateBytes 4 |>> "tikt"
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyConfirm "tikt")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("tag", toBase64 "tid")])
            checkpoint fake
                [ adminLookup |>> Right [tadmin]
                , TagDelete "tid" |>> Right ()
                ]
            exec
                (expectResponse $ Response StatusOk ResponseBodyOk)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("tag", toBase64 "tid"), ("confirm", toBase64 "tikt")])
        specifyMethod "files/upload" $ \(MethodEnv fake exec) -> do
            {- akey :: required user access key -}
            {- article :: required article id -}
            {- user must possess the article -}
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrMissingParameter "article")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag")])
            exec
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError $ ErrInvalidParameter "article")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("article", "not an id")])
            checkpoint fake
                [ userLookup |>> Right [tuser]
                , ArticleList (ListView 0 1 [FilterArticleId "aid", FilterArticleUserId "uid"] []) |>> Right []
                ]
            exec
                (expectResponse $ Response StatusForbidden $ ResponseBodyError ErrArticleNotEditable)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("article", toBase64 "aid")])
            checkpoint fake
                [ userLookup |>> Right [tuser]
                , ArticleList (ListView 0 1 [FilterArticleId "aid", FilterArticleUserId "uid"] []) |>> Right [tarticle]
                , GroundGenerateBytes 4 |>> "uptk"
                ]
            exec
                (expectResponse $ Response StatusOk $ ResponseBodyFollow $ "host/upload/" <> toBase64Text "uptk")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("article", toBase64 "aid")])
        specify "upload/..." $ \(TestEnv fake medium) -> do
            checkpoint fake
                [ userLookup |>> Right [tuser]
                , ArticleList (ListView 0 1 [FilterArticleId "aid", FilterArticleUserId "uid"] []) |>> Right [tarticle]
                , GroundGenerateBytes 4 |>> "uptk"
                ]
            executeRequest medium ["files", "upload"]
                (expectResponse $ Response StatusOk $ ResponseBodyFollow $ "host/upload/" <> toBase64Text "uptk")
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("article", toBase64 "aid")])
            executeRequest medium ["upload", toBase64Text "uptk"]
                (expectResponse $ Response StatusBadRequest $ ResponseBodyError ErrInvalidRequest)
                (SimpleRequest $ rqdata [("akey", "AQ:Ag"), ("article", toBase64 "aid")])
            checkpoint fake
                [ GroundUpload "file1.txt" "text/plain" "aid" "uid"
                    |>> Right (UploadProcess
                        (FileInfo "fid1" "file1.txt" "text/plain" (mkTime "2020-01-01T01:01:01Z") "aid" 0 "uid")
                        ["1234567890", "1234567890", "1234567890", "1234567890", "1234567890"]
                        (Right (True, Nothing)))
                , GroundUpload "file2.txt" "text/plain" "aid" "uid"
                    |>> Right (UploadProcess
                        (FileInfo "fid2" "file2.txt" "text/plain" (mkTime "2020-01-01T01:01:02Z") "aid" 1 "uid")
                        ["1234567890", "1234567"]
                        (Right (True, Nothing)))
                , GroundUpload "file6.txt" "text/plain" "aid" "uid"
                    |>> Right (UploadProcess
                        (FileInfo "fid6" "file6.txt" "text/plain" (mkTime "2020-01-01T01:01:06Z") "aid" 3 "uid")
                        ["1234567890", "1234567890", "1234567890", "1234567890"]
                        (Right (False, Nothing)))
                , GroundUpload "file7.txt" "text/plain" "aid" "uid"
                    |>> Right (UploadProcess
                        (FileInfo "fid7" "file7.txt" "text/plain" (mkTime "2020-01-01T01:01:07Z") "aid" 3 "uid")
                        ["1234567890"]
                        (Left InternalError))
                , ArticleList (ListView 0 1 [FilterArticleId "aid"] []) |>> Right [tarticle]
                , AuthorList (ListView 0 1 [FilterAuthorId "author"] []) |>> Right [tauthor]
                , CategoryAncestry "cat1" |>> Right [tcategory1, tcategory2]
                , TagList (ListView 0 maxBound [FilterTagArticleId "aid"] [(OrderTagName, Ascending)]) |>> Right [ttag1, ttag2]
                , UserList (ListView 0 1 [FilterUserId "uid"] []) |>> Right [tuser]
                ]
            executeRequest medium ["upload", toBase64Text "uptk"]
                (expectResponse $ Response StatusOk $ ResponseBodyUploadStatusList
                    [ ExUploadStatus "param1" $ Right $ ExFileInfo
                        "fid1" "file1.txt" "text/plain" (mkTime "2020-01-01T01:01:01Z")
                        (Just tarticleEx) 0 (Just $ ExUser tuser) ("host/get/" <> toBase64Text "fid1" <> "/file1.txt")
                    , ExUploadStatus "param2" $ Right $ ExFileInfo
                        "fid2" "file2.txt" "text/plain" (mkTime "2020-01-01T01:01:02Z")
                        (Just tarticleEx) 1 (Just $ ExUser tuser) ("host/get/" <> toBase64Text "fid2" <> "/file2.txt")
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
        specify "get/..." $ \(TestEnv fake medium) -> do
            checkpoint fake
                [ GroundDownload "fid1" |>>
                    Right (DownloadProcess "text/plain" ["1234567890", "1234567890", "123"])
                ]
            executeRequest medium ["get", toBase64Text "fid1", "filename that should be ignored"]
                (expectStream "text/plain" ["1234567890", "1234567890", "123"])
                (SimpleRequest $ rqdata [])
            checkpoint fake
                [ GroundDownload "fid2" |>> Left NotFoundError
                ]
            executeRequest medium ["get", toBase64Text "fid2", "filename that should be ignored"]
                (expectResponse $ Response StatusNotFound $ ResponseBodyError ErrNotFound)
                (SimpleRequest $ rqdata [])
