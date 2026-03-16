{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | C-exported entry points for the Python bindings.
module DataFrame.FFI where

import Control.Exception (SomeException, try)
import Data.Word (Word64)
import Foreign (Ptr, castPtr, poke, ptrToWordPtr)
import Foreign.C.String (CString)
import Foreign.C.Types (CInt (..))
import System.IO (hPrint, hPutStrLn, stderr)

import qualified Data.Aeson as Aeson
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL

import DataFrame.IO.Arrow (dataframeToArrow)
import DataFrame.IR (executePlan)

foreign export ccall "dfExecutePlan"
    dfExecutePlan :: CString -> Ptr Word64 -> Ptr Word64 -> IO CInt

{- | Execute a JSON-encoded query plan and return Arrow C Data Interface
  pointers to Python via output parameters.

  @planCS@    – JSON plan string (see DataFrame.IR for schema)
  @schemaOut@ – receives the ArrowSchema* address as a Word64
  @arrayOut@  – receives the ArrowArray*  address as a Word64

  Returns 0 on success, -1 on error (error message written to stderr).
-}
dfExecutePlan :: CString -> Ptr Word64 -> Ptr Word64 -> IO CInt
dfExecutePlan planCS schemaOut arrayOut = do
    planBytes <- BS.packCString planCS
    result <- try @SomeException $ do
        node <-
            either
                fail
                return
                (Aeson.eitherDecode (BL.fromStrict planBytes))
        df <- executePlan node
        (sPtr, aPtr) <- dataframeToArrow df
        poke schemaOut (fromIntegral (ptrToWordPtr (castPtr sPtr)))
        poke arrayOut (fromIntegral (ptrToWordPtr (castPtr aPtr)))
    case result of
        Left ex -> hPrint stderr ex >> return (CInt (-1))
        Right _ -> return (CInt 0)
