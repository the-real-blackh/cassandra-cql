{-# LANGUAGE OverloadedStrings #-}
module Database.Cassandra.CQL (
        Server,
        createCassandraPool,
        CPool
    ) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad.Maybe
import Control.Monad.State.Strict
import Control.Monad.Trans
import Data.Binary
import Data.Bits
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import Data.Int
import Data.Map (Map)
import qualified Data.Map as M
import Data.Word
import Data.IORef
import Data.Sequence (Seq, (|>))
import qualified Data.Sequence as Seq
import Data.Serialize
import Data.String
import Data.Text (Text)
import qualified Data.Text as T
import Network.Socket (Socket, HostName, ServiceName)
import Network.Socket.ByteString (send, sendAll, recv)
import Numeric

type Server = (HostName, ServiceName)
type KeySpace = ByteString

data Session = Session {
        sesServer :: Server,
        sesSocket :: Maybe Socket
    }

data CPool = CPool {
        piKeySpace :: ByteString,
        piSessions :: TVar (Seq (IORef Session))
    }

class MonadCassandra m where
    getCassandraPool :: m CPool

data CassandraException = ProtocolException String

createCassandraPool :: [Server] -> KeySpace -> IO CPool
createCassandraPool svrs ks = do
    sessions <- forM svrs $ \svr -> newIORef $ Session {
            sesServer = svr,
            sesSocket = Nothing
        }
    sess <- atomically $ newTVar sessions
    return $ CPool {
            piKeySpace = ks,
            piSessions = sess
        }

takeSession :: CPool -> IO (IORef Session)
takeSession pool = atomically $ do
    sess <- readTVar (piSessions pool)
    if Seq.empty
        then retry
        else do
            let ses = sess `index` 0
            writeTVar (piSessions pool) (Seq.drop 1 sess)
            return ses

putSession :: CPool -> IORef Session -> IO ()
putSession pool ses = atomically $ modifyTVar (piSessions pool) (|> ses)

connectIfNeeded :: IORef Session -> IO Socket
connectIfNeeded sesRef = do
    ses <- readIORef sesRef
    case sesSocket ses of
        Just socket -> return socket
        Nothing -> do
            let (host, service) = sesServer ses
            ais <- getAddrInfo Nothing (Just  host) (Just service)
            mSocket <- runMaybeT $ forM ais $ \ai -> do
                MaybeT $ do
                    s <- socket (addrFamily ai) (addrSocketType ai) (addrProtocol ai)
                    do
                        connect s (addrAddress ai)
                        return (Just s)
                      `catch` \(exc :: IOException) -> do
                        sClose s
                        return Nothing
            case mSocket of
                Just socket -> do
                    writeIORef sesRef $ ses { sesSocket = Just socket }
                    introduce socket
                    return socket
                Nothing -> do
                    connectIfNeeded

data Flag = Compression | Tracing
    deriving Show

instance Serialize Flag where
    put flags = putWord8 $ foldl' (+) 0 $ map toWord8 flags
      where
        toWord8 Compression = 0x01
        toWord8 Tracing = 0x02
    get = do
        flagsB <- getWord8
        return $ case flagsB .&. 3 of
            0 -> []
            1 -> [Compression]
            2 -> [Tracing]
            3 -> [Compression, Tracing]
            _ -> error "recvFrame impossible"

data Opcode = ERROR | STARTUP | READY | AUTHENTICATE | CREDENTIALS | OPTIONS |
              SUPPORTED | QUERY | RESULT | PREPARE | EXECUTE | REGISTER | EVENT
    deriving (Eq, Show)

instance Serialize Opcode where
    put op = putWord8 $ case op of
        ERROR        -> 0x00
        STARTUP      -> 0x01
        READY        -> 0x02
        AUTHENTICATE -> 0x03
        CREDENTIALS  -> 0x04
        OPTIONS      -> 0x05
        SUPPORTED    -> 0x06
        QUERY        -> 0x07
        RESULT       -> 0x08
        PREPARE      -> 0x09
        EXECUTE      -> 0x0a
        REGISTER     -> 0x0b
        EVENT        -> 0x0c
    get = do
        w <- getWord8
        case w of
            0x00 -> ERROR        
            0x01 -> STARTUP      
            0x02 -> READY        
            0x03 -> AUTHENTICATE 
            0x04 -> CREDENTIALS  
            0x05 -> OPTIONS      
            0x06 -> SUPPORTED    
            0x07 -> QUERY        
            0x08 -> RESULT                            
            0x09 -> PREPARE      
            0x0a -> EXECUTE      
            0x0b -> REGISTER     
            0x0c -> EVENT
            _    -> fail $ "unknown opcode 0x"++showHex w ""

data Frame a = Frame {
        frFlags :: [Flag],
        frStream :: Int8,
        frOpcode :: Opcode,
        frBody   :: a
    }
    deriving Show

recvAll :: Socket -> Int -> IO ByteString
recvAll s n = do
    bs <- recv s n
    let left = B.length - n
    if left == 0
        then return bs
        else do
            bs' <- recvAll s left
            return (bs `B.append` bs')

protocolVersion :: Word8
protocolVersion = 1

recvFrame :: Socket -> IO (Frame ByteString)
recvFrame = do
    hdrBs <- recvAll 8
    case runGet parseHeader hdrBs of
        Left err -> throwIO $ ProtocolException $ "recvFrame: "++err
        Right (ver, flags, stream, opcode, length) ->
            when (ver /= protocolVersion) $ throwIO $ ProtocolException $ "unexpected version "++show ver
            body <- recvAll (fromIntegral length)
            return $ Frame ver flags stream opcode body
  where
    parseHeader = do
        ver <- getWord8
        flags <- get
        stream <- getInt8
        opcode <- get
        length <- getWord32be
        return (ver, flags, stream, opcode, length)

sendFrame :: Socket -> Frame ByteString -> IO ()
sendFrame s (Frame flags stream opcode body) =
    sendAll s $ runPut $ do
        putWord8 protocolVersion
        put flags
        putInt8 stream
        put opcode
        putWord32be $ fromIntegral $ B.length body

class CasType a where
    getCas :: Get a
    putCas :: a -> Put

encodeCas :: CasType a => a -> ByteString
encodeCas = runPut . putCas

decodeCas :: CasType a => ByteString -> Either String a
decodeCas bs = runGet getCas bs

newtype Long a = Long a deriving Functor
newtype Short a = Short a deriving Functor

instance CasType (Map Text Text) where
    putCas = putCas . M.assocs
    getCas = M.toList <$> get

instance CasType [(Text, Text)] where
    putCas pairs = do
        putWord16be (fromIntegral $ B.length pairs)
        forM_ pairs $ \(key, value) -> do
            putCas key
            putCas value
    getCas = do
        n <- getWord16be
        replicateM (fromIntegral n) $ do
            key <- getCas
            value <- getCas
            return (key, value)

instance CasType Text where
    putCas = putCas . T.encodeUtf8
    getCas = T.decodeUtf8 <$> get

instance CasType (Long Text) where
    putCas = putCas . fmap T.encodeUtf8
    getCas = fmap T.decodeUtf8 <$> get

instance CasType ByteString where
    putCas bs = do
        putWord16be (B.length bs)
        putByteString bs
    getCas = do
        len <- getWord16be
        getByteString (fromIntegral len)

instance CasType (Long ByteString) where
    putCas bs = do
        putWord32be (B.length bs)
        putByteString bs
    getCas = do
        len <- getWord32be
        getByteString (fromIntegral len)

introduce :: CPool -> Socket -> IO ()
introduce s = do
    let out = Frame [] 0 $ STARTUP $ encodeCas $ ([("CQL_VERSION", "3.0.0")] :: [(Text, Text)])
    print out
    sendFrame out 
    fr <- recvFrame
    print fr
    case frOpcode fr of
        AUTHENTICATE -> throwIO $ AuthenticationException "authentication not implemented yet"
        READY -> return ()
        op -> throwIO $ ProtocolException $ "introduce: unexpected opcode "++show op

withSession :: MonadCassandra m => (CPool -> Socket -> IO a) -> m a
withSession code = do
    pool <- getCassandraPool
    liftIO $ do
        mA <- bracket takeSession putSession $ \sesRef -> do
            ses <- readIORef sesRef
            mSocket <- connectIfNeeded ses
            case mSocket of
                Just socket -> do
                    a <- code socket
                    return (Just a)
                Nothing -> return Nothing
        case mA of
            Just a -> return a
            Nothing -> withSession code  -- Try again until we succeed

data Result = Prepared 
newtype 

prepare :: MonadCassandra m => ByteString -> IO 
