{-# LANGUAGE DeriveDataTypeable #-}

module Data.Conduit.Decoder 
    ( conduitDecoder
    , BinaryDecodeException(BinaryDecodeException)
    ) where
        
import           Control.Exception (Exception)
import           Data.Binary.Get (Get, Decoder(Fail, Partial, Done), runGetIncremental, pushChunk)
import           Data.ByteString (ByteString)
import           Data.Conduit (Conduit, MonadThrow, monadThrow, await, yield)
import           Data.Typeable (Typeable)


-- | Basic decoder exception
data BinaryDecodeException = BinaryDecodeException String
    deriving (Show, Typeable)
    
instance Exception BinaryDecodeException

-- | Incrementally reads ByteStrings and builds from supplied Get monad.
-- Will throw an exception if there was an error parsing
conduitDecoder :: MonadThrow m => Get a -> Conduit ByteString m a
conduitDecoder decoderGet = incrementalDecode emptyGet
        where emptyGet = runGetIncremental decoderGet
              incrementalDecode built = await >>= maybe (return ()) handleConvert
                  where handleConvert msg = do
                            let newMsg = pushChunk built msg
                            case newMsg of
                                    Done a n doc -> do yield doc
                                                       incrementalDecode $ pushChunk emptyGet a
                                    Partial _    -> incrementalDecode newMsg
                                    Fail a _ err -> do
                                        monadThrow $ BinaryDecodeException err
                                        incrementalDecode $ pushChunk emptyGet a