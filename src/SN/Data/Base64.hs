module SN.Data.Base64
    ( toBase64
    , toBase64Text
    , fromBase64
    , fromBase64Text
    , base64
    , showBase64
    ) where

import Data.String
import qualified Data.ByteString as BS
import qualified Data.ByteString.Base64.URL as B64
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text

toBase64 :: BS.ByteString -> BS.ByteString
toBase64 = B64.encodeBase64Unpadded'

toBase64Text :: BS.ByteString -> Text.Text
toBase64Text = B64.encodeBase64Unpadded

fromBase64 :: BS.ByteString -> Maybe BS.ByteString
fromBase64 = either (const Nothing) Just . B64.decodeBase64Unpadded

fromBase64Text :: Text.Text -> Maybe BS.ByteString
fromBase64Text = fromBase64 . Text.encodeUtf8

base64 :: IsString a => BS.ByteString -> a
base64 = fromString . Text.unpack . toBase64Text

showBase64 :: Int -> BS.ByteString -> ShowS
showBase64 d cs = showParen (d > 10) $ showString $ "fromBase64' \"" ++ base64 cs ++ "\""
