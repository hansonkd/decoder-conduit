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
import           Control.Monad.Trans.Resource (MonadThrow, monadThrow)


-- | Basic decoder exception
data BinaryDecodeException = BinaryDecodeException String
    deriving (Show, Typeable)
    
instance Exception BinaryDecodeException

-- | Incrementally reads ByteStrings and builds from supplied Get monad.
-- Will throw an exception if there was an error parsing
conduitDecoder :: MonadThrow m => Get a -> Conduit ByteString m a
conduitDecoder decoderGet = incrementalDecode emptyDecoder
        where emptyDecoder = runGetIncremental decoderGet
              incrementalDecode built = await >>= maybe (return ()) handleConvert
                  where handleConvert bytestringInput = do
                            case pushChunk built bytestringInput of
                                    Done a n doc      -> do yield doc
                                                            incrementalDecode $ pushChunk emptyDecoder a
                                    curBS@(Partial _) -> incrementalDecode curBS
                                    Fail a _ err -> do
                                        monadThrow $ BinaryDecodeException err
                                        incrementalDecode $ pushChunk emptyDecoder a