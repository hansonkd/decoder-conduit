{-# LANGUAGE DeriveDataTypeable #-}

module Data.Conduit.Decoder 
    ( conduitDecoder
    , BinaryDecodeException(BinaryDecodeException)
    ) where
        
import           Control.Exception (Exception)
import           Data.Binary.Get (Get, Decoder(Fail, Partial, Done), runGetIncremental, pushChunk)
import           Data.ByteString (ByteString)
import           Data.Conduit (Conduit, await, yield, leftover)
import           Data.Typeable (Typeable)
import Control.Monad.Trans.Resource (MonadThrow, monadThrow)


-- | Basic decoder exception
data BinaryDecodeException = BinaryDecodeException String
    deriving (Show, Typeable)
    
instance Exception BinaryDecodeException

-- | Incrementally reads ByteStrings and builds from supplied Get monad.
-- Will throw an exception if there was an error parsing
conduitDecoder :: (MonadThrow m) => Get a -> Conduit ByteString m a
conduitDecoder parser = decode parser (runGetIncremental parser)

decode :: (MonadThrow m) => Get a -> Decoder a -> Conduit ByteString m a
decode parser decoder = await >>= maybe (return ()) handleConvert
   where
     handleConvert msg = let decoded = decoder `pushChunk` msg in
         case decoded of
            Fail _ _ err -> monadThrow (BinaryDecodeException err) >> conduitDecoder parser
            Partial _ -> decode parser decoded
            Done rest offs what -> yield what >> leftover rest >> conduitDecoder parser
