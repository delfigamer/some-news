{-# LANGUAGE StandaloneDeriving #-}

module JsonInterfaceSpec
    ( spec
    ) where

import Control.Exception
import Control.Monad
import Data.Aeson
import Data.Aeson.Text
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

testConfig :: Config.Config
testConfig = Config.Config
    { Config.defaultPageLimit = 10
    , Config.maxPageLimit = 100
    , Config.maxAccessKeyCount = 3
    }

spec :: Spec
spec = do
    describe "JsonInterface" $ do
        it "(unknown URI)" $ do
            testSimpleRequest "not a valid URI"
                mempty
                (RequestExpectation [] errUnknownRequest)
        it "(access key deserialization)" $ do
            let validAkeys =
                    [ ("01234567:89abcdef", "\x01\x23\x45\x67", "\x89\xab\xcd\xef")
                    , ("fe:3210", "\xfe", "\x32\x10")
                    ]
            forM_ validAkeys $ \(akey, keyref, keytoken) -> do
                testSimpleRequest "users/me"
                    (Map.singleton "akey" akey)
                    (RequestExpectation
                        [UserList (ListView 0 1 [FilterUserAccessKey $ AccessKey (Reference keyref) keytoken] []) |>> Left InternalError]
                        errInternal)
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
                testSimpleRequest "users/me"
                    (Map.singleton "akey" akey)
                    (RequestExpectation [] (errInvalidParameter "akey"))
        specifyPublicRequest "users/create"
            [ requiredNonEmptyParameter "name" "foo"
            , requiredNonEmptyParameter "surname" "bar"
            ]
            [ RequestExpectation
                [UserCreate "foo" "bar" |>> Right sampleUser]
                okSampleUser
            ]
        specifyUserRequest "users/me"
            sampleUser
            []
            [ RequestExpectation
                []
                okSampleUser
            ]
        specifyUserRequest "users/setName"
            sampleUser
            [ requiredNonEmptyParameter "name" "foo2"
            , requiredNonEmptyParameter "surname" "bar2"
            ]
            [ RequestExpectation
                [UserSetName (userId sampleUser) "foo2" "bar2" |>> Right ()]
                okTrue
            ]
        specifyUserRequest "users/delete"
            (sampleUser {userId = Reference "\x12\x34"})
            [ ParameterSpec "user" "1234" -- parameter 'user' must be equal to requester's id
                [ ParameterSpecModifier
                    True
                    (Map.insert "user" "5678")
                    [ RequestExpectation
                        []
                        (errInvalidParameter "user")
                    ]
                ]
            ]
            [ RequestExpectation
                [UserDelete (Reference "\x12\x34") |>> Right ()]
                okTrue
            ]
        specifyAdminRequest "users/setName_"
            sampleUser
            [ requiredIdParameter "user" "1234"
            , requiredNonEmptyParameter "name" "foo2"
            , requiredNonEmptyParameter "surname" "bar2"
            ]
            [ RequestExpectation
                [UserSetName (Reference "\x12\x34") "foo2" "bar2" |>> Right ()]
                okTrue
            , RequestExpectation
                [UserSetName (Reference "\x12\x34") "foo2" "bar2" |>> Left NotFoundError]
                errNotFound
            ]
        specifyAdminRequest "users/grantAdmin_"
            sampleUser
            [ requiredIdParameter "user" "1234"
            ]
            [ RequestExpectation
                [UserSetIsAdmin (Reference "\x12\x34") True |>> Right ()]
                okTrue
            , RequestExpectation
                [UserSetIsAdmin (Reference "\x12\x34") True |>> Left NotFoundError]
                errNotFound
            ]
        specifyAdminRequest "users/dropAdmin_"
            sampleUser
            [ requiredIdParameter "user" "1234"
            ]
            [ RequestExpectation
                [UserSetIsAdmin (Reference "\x12\x34") False |>> Right ()]
                okTrue
            , RequestExpectation
                [UserSetIsAdmin (Reference "\x12\x34") False |>> Left NotFoundError]
                errNotFound
            ]
        specifyAdminRequest "users/delete_"
            sampleUser
            [ requiredIdParameter "user" "1234"
            ]
            [ RequestExpectation
                [UserDelete (Reference "\x12\x34") |>> Right ()]
                okTrue
            , RequestExpectation
                [UserDelete (Reference "\x12\x34") |>> Left NotFoundError]
                errNotFound
            ]
        specifyAdminRequest "users/list_"
            sampleUser
            []
            [ RequestExpectation
                [ UserList (ListView 0 10 [] [])
                    |>> Right
                        [ User (Reference "\x00\x01") "name1" "surname1" "2020-01-01T01:01:01Z" False
                        , User (Reference "\x00\x02") "name2" "surname2" "2020-01-02T03:04:05Z" True
                        , User (Reference "\x00\x03") "name3" "surname3" "2020-01-06T12:18:25Z" True
                        ]
                ]
                (okList
                    [ object
                        [ "class" .= String "User"
                        , "id" .= String "0001"
                        , "name" .= String "name1"
                        , "surname" .= String "surname1"
                        , "joinDate" .= String "2020-01-01T01:01:01Z"
                        , "isAdmin" .= False
                        ]
                    , object
                        [ "class" .= String "User"
                        , "id" .= String "0002"
                        , "name" .= String "name2"
                        , "surname" .= String "surname2"
                        , "joinDate" .= String "2020-01-02T03:04:05Z"
                        , "isAdmin" .= True
                        ]
                    , object
                        [ "class" .= String "User"
                        , "id" .= String "0003"
                        , "name" .= String "name3"
                        , "surname" .= String "surname3"
                        , "joinDate" .= String "2020-01-06T12:18:25Z"
                        , "isAdmin" .= True
                        ]
                    ])
            ]
        it "users/list_ (option parsing)" $ do
            let user = User (Reference "\x01\x23") "f" "g" "2020-01-01T01:01:01Z" True
            let userLookup = UserList $ ListView
                    0 1
                    [FilterUserAccessKey $ AccessKey (Reference "\x01\x02") "\x03\x04", FilterUserIsAdmin True]
                    []
            do
                forM_
                    [ ("0", 0)
                    , ("123456", 123456)
                    , ("1234567890123456789", 1234567890123456789) -- 64-bit integer
                    ] $ \(offsetParam, offsetValue) -> do
                        testSimpleRequest "users/list_"
                            (Map.fromList [("akey", "0102:0304"), ("offset", offsetParam)])
                            (RequestExpectation
                                [ userLookup |>> Right [user]
                                , UserList (ListView offsetValue 10 [] []) |>> Left InternalError
                                ]
                                errInternal)
                forM_
                    [ ""
                    , "-1"
                    , "not a number"
                    , "4.5"
                    , "123456789012345678901234567890" -- not a 64-bit integer
                    ] $ \offsetParam -> do
                        testSimpleRequest "users/list_"
                            (Map.fromList [("akey", "0102:0304"), ("offset", offsetParam)])
                            (RequestExpectation
                                [ userLookup |>> Right [user]
                                ]
                                (errInvalidParameter "offset"))
            do
                forM_
                    [ ("1", 1)
                    , ("12", 12)
                    , ("100", 100) -- maxPageLimit
                    ] $ \(limitParam, limitValue) -> do
                        testSimpleRequest "users/list_"
                            (Map.fromList [("akey", "0102:0304"), ("limit", limitParam)])
                            (RequestExpectation
                                [ userLookup |>> Right [user]
                                , UserList (ListView 0 limitValue [] []) |>> Left InternalError
                                ]
                                errInternal)
                forM_
                    [ ""
                    , "0"
                    , "-1"
                    , "101" -- maxPageLimit + 1
                    , "not a number"
                    , "4.5"
                    , "123456789012345678901234567890" -- not a 64-bit integer
                    ] $ \limitParam -> do
                        testSimpleRequest "users/list_"
                            (Map.fromList [("akey", "0102:0304"), ("limit", limitParam)])
                            (RequestExpectation
                                [ userLookup |>> Right [user]
                                ]
                                (errInvalidParameter "limit"))
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
                        testSimpleRequest "users/list_"
                            (Map.fromList [("akey", "0102:0304"), ("orderBy", orderParam)])
                            (RequestExpectation
                                [ userLookup |>> Right [user]
                                , UserList (ListView 0 10 [] orderValue) |>> Left InternalError
                                ]
                                errInternal)
                forM_
                    [ "not an order"
                    , "+"
                    , "+nameDesc"
                    , "nameDesc+"
                    , "nameAscsurnameAsc"
                    ] $ \orderParam -> do
                        testSimpleRequest "users/list_"
                            (Map.fromList [("akey", "0102:0304"), ("orderBy", orderParam)])
                            (RequestExpectation
                                [ userLookup |>> Right [user]
                                ]
                                (errInvalidParameter "orderBy"))
        it "(upload stub???)" $ do
            withFakeStorage $ \context storage -> do
                checkpoint context
                    [ UploadAction
                        "foo"
                        "text/plain"
                        "\x01\x02"
                        "\x0a\x0b"
                        |>> ExpectChunks
                            (FileInfo "\x34\x56" "foo" "text/plain" "2020-01-02T03:04:05Z" "\x01\x02" 10 "\x0a\x0b")
                            ["foo", "bar", "quux"]
                            True
                            Nothing
                    ]
                result <- storageUpload storage "foo" "text/plain" "\x01\x02" "\x0a\x0b" $ \finfo -> do
                    return $ UploadChunk "foo" $ do
                        return $ UploadChunk "bar" $ do
                            return $ UploadChunk "quux" $ do
                                return $ UploadFinish "xxx"
                result `shouldBe` Right "xxx"
  where
    sampleUser = User (Reference "\x01\x23\xab\xcd") "foo" "bar" "2020-01-02T03:04:05.67Z" False
    okSampleUser = okValue $ object
        [ "class" .= String "User"
        , "id" .= String "0123abcd"
        , "name" .= String "foo"
        , "surname" .= String "bar"
        , "joinDate" .= String "2020-01-02T03:04:05.67Z"
        , "isAdmin" .= False
        ]

errInternal :: Response
errInternal = Response StatusInternalError $ JsonResponse $ object
    [ "error" .= object
        [ "class" .= String "Internal error"
        ]
    ]

errInvalidAccessKey :: Response
errInvalidAccessKey = Response StatusForbidden $ JsonResponse $ object
    [ "error" .= object
        [ "class" .= String "Invalid access key"
        ]
    ]

errInvalidParameter :: BS.ByteString -> Response
errInvalidParameter pname = Response StatusBadRequest $ JsonResponse $ object
    [ "error" .= object
        [ "class" .= String "Invalid parameter"
        , "parameterName" .= Text.decodeUtf8 pname
        ]
    ]

errMissingParameter :: BS.ByteString -> Response
errMissingParameter pname = Response StatusBadRequest $ JsonResponse $ object
    [ "error" .= object
        [ "class" .= String "Missing parameter"
        , "parameterName" .= Text.decodeUtf8 pname
        ]
    ]

errNotFound :: Response
errNotFound = Response StatusNotFound $ JsonResponse $ object
    [ "error" .= object
        [ "class" .= String "Not found"
        ]
    ]

errUnknownRequest :: Response
errUnknownRequest = Response StatusNotFound $ JsonResponse $ object
    [ "error" .= object
        [ "class" .= String "Unknown request"
        ]
    ]

okTrue :: Response
okTrue = Response StatusOk $ JsonResponse $ object
    [ "ok" .= True
    ]

okValue :: Value -> Response
okValue value = Response StatusOk $ JsonResponse $ object
    [ "ok" .= value
    ]

okList :: [Value] -> Response
okList elems = Response StatusOk $ JsonResponse $ object
    [ "ok" .= elems
    ]

data ParameterSpec = ParameterSpec
    BS.ByteString
    BS.ByteString
    [ParameterSpecModifier]

data ParameterSpecModifier = ParameterSpecModifier
    Bool
    (Map.HashMap BS.ByteString BS.ByteString -> Map.HashMap BS.ByteString BS.ByteString)
    [RequestExpectation]

data RequestExpectation = RequestExpectation [ActionExpectation] Response

prependAction :: ActionExpectation -> RequestExpectation -> RequestExpectation
prependAction ae (RequestExpectation aes re) = RequestExpectation (ae : aes) re

requiredParameter :: BS.ByteString -> BS.ByteString -> ParameterSpec
requiredParameter pname pvalue = ParameterSpec pname pvalue
    [ ParameterSpecModifier
        False
        (Map.delete pname)
        [ RequestExpectation
            []
            (errMissingParameter pname)
        ]
    ]

requiredIdParameter :: BS.ByteString -> BS.ByteString -> ParameterSpec
requiredIdParameter pname pvalue = ParameterSpec pname pvalue
    [ ParameterSpecModifier
        False
        (Map.delete pname)
        [ RequestExpectation
            []
            (errMissingParameter pname)
        ]
    , ParameterSpecModifier
        False
        (Map.insert pname "invalid identifier")
        [ RequestExpectation
            []
            (errInvalidParameter pname)
        ]
    ]

requiredNonEmptyParameter :: BS.ByteString -> BS.ByteString -> ParameterSpec
requiredNonEmptyParameter pname pvalue = ParameterSpec pname pvalue
    [ ParameterSpecModifier
        False
        (Map.delete pname)
        [ RequestExpectation
            []
            (errMissingParameter pname)
        ]
    , ParameterSpecModifier
        False
        (Map.insert pname "")
        [ RequestExpectation
            []
            (errInvalidParameter pname)
        ]
    ]

specifyPublicRequest
    :: HasCallStack
    => Text.Text
    -> [ParameterSpec]
    -> [RequestExpectation]
    -> Spec
specifyPublicRequest uri paramSpec reqExps = do
    specify (Text.unpack uri ++ " (public method)") $ do
        let params = Map.fromList $ map (\(ParameterSpec pname pvalue _) -> (pname, pvalue)) paramSpec
        forM_ paramSpec $ \(ParameterSpec _ _ mods) -> do
            forM_ mods $ \(ParameterSpecModifier _ paramsMod reqExps2) -> do
                forM_ reqExps2 $ \reqExp -> do
                    testSimpleRequest uri (paramsMod params) reqExp
        forM_ reqExps $ \reqExp -> do
            testSimpleRequest uri params reqExp

specifyUserRequest
    :: HasCallStack
    => Text.Text
    -> User
    -> [ParameterSpec]
    -> [RequestExpectation]
    -> Spec
specifyUserRequest uri user paramSpec reqExps = do
    specify (Text.unpack uri ++ " (user method)") $ do
        let paramsBase = Map.fromList $ map (\(ParameterSpec pname pvalue _) -> (pname, pvalue)) paramSpec
        let params = Map.insert "akey" "01234567:89abcdef" paramsBase
        let userLookup = UserList $ ListView
                0 1
                [FilterUserAccessKey $ AccessKey (Reference "\x01\x23\x45\x67") "\x89\xab\xcd\xef"]
                []
        forM_ paramSpec $ \(ParameterSpec _ _ mods) -> do
            forM_ mods $ \(ParameterSpecModifier accessRequired paramsMod reqExps2) -> do
                forM_ reqExps2 $ \reqExp -> do
                    case accessRequired of
                        False -> testSimpleRequest uri (paramsMod params)
                            reqExp
                        True -> testSimpleRequest uri (paramsMod params)
                            (prependAction (userLookup |>> Right [user]) reqExp)
        testSimpleRequest uri paramsBase
            (RequestExpectation
                []
                (errMissingParameter "akey"))
        testSimpleRequest uri params
            (RequestExpectation
                [userLookup |>> Right []]
                errInvalidAccessKey)
        forM_ reqExps $ \reqExp -> do
            testSimpleRequest uri params
                (prependAction (userLookup |>> Right [user]) reqExp)

specifyAdminRequest
    :: HasCallStack
    => Text.Text
    -> User
    -> [ParameterSpec]
    -> [RequestExpectation]
    -> Spec
specifyAdminRequest uri user paramSpec reqExps = do
    specify (Text.unpack uri ++ " (admin method)") $ do
        let paramsBase = Map.fromList $ map (\(ParameterSpec pname pvalue _) -> (pname, pvalue)) paramSpec
        let params = Map.insert "akey" "01234567:89abcdef" paramsBase
        let userLookup = UserList $ ListView 0 1
                [FilterUserAccessKey $ AccessKey (Reference "\x01\x23\x45\x67") "\x89\xab\xcd\xef", FilterUserIsAdmin True]
                []
        forM_ paramSpec $ \(ParameterSpec _ _ mods) -> do
            forM_ mods $ \(ParameterSpecModifier _ paramsMod reqExps2) -> do
                forM_ reqExps2 $ \reqExp -> do
                    testSimpleRequest uri (paramsMod params)
                        (prependAction (userLookup |>> Right [user]) reqExp)
        testSimpleRequest uri paramsBase
            (RequestExpectation
                []
                errUnknownRequest)
        testSimpleRequest uri params
            (RequestExpectation
                [userLookup |>> Right []]
                errUnknownRequest)
        forM_ reqExps $ \reqExp -> do
            testSimpleRequest uri params
                (prependAction (userLookup |>> Right [user]) reqExp)

testSimpleRequest
    :: HasCallStack
    => Text.Text
    -> Map.HashMap BS.ByteString BS.ByteString
    -> RequestExpectation
    -> IO ()
testSimpleRequest uri params (RequestExpectation expectedActions expectedResponse) = do
    withTestLogger $ \logger -> do
        withFakeStorage $ \context storage -> do
            checkpoint context expectedActions
            result <- withJsonInterface testConfig logger storage $ \jint -> do
                simpleRequest jint (Text.splitOn "/" uri) params
            result `shouldBe` expectedResponse

instance Show ResponseContent where
    showsPrec _ (JsonResponse j) =
        showString "[JsonResponse| " . showJson (toJSON j) . showString " |]"
      where
        showJson (Object obj) = do
            let fields = sortOn fst $ Map.toList obj
            let parts = map (\(k,v) -> showJson (String k) . showString ":" . showJson v) fields
            let inner = if null parts
                    then id
                    else foldr1 (\a b -> a . showString "," . b) parts
            showString "{" . inner . showString "}"
        showJson (Array arr) = do
            let parts = Vector.map showJson arr
            let inner = if Vector.null parts
                    then id
                    else Vector.foldr1 (\a b -> a . showString "," . b) parts
            showString "[" . inner . showString "]"
        showJson x = \after -> TextLazy.unpack (encodeToLazyText x) ++ after

instance Eq ResponseContent where
    JsonResponse j1 == JsonResponse j2 = toJSON j1 == toJSON j2

deriving instance Show Response
deriving instance Eq Response

instance IsString TClock.UTCTime where
    fromString = fromJust . TFormat.iso8601ParseM
