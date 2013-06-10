{-# LANGUAGE OverloadedStrings, GeneralizedNewtypeDeriving, ScopedTypeVariables,
        FlexibleInstances, DeriveDataTypeable, UndecidableInstances,
        BangPatterns, OverlappingInstances #-}
module Database.Cassandra.CQL (
        -- * Initialization
        Server,
        createCassandraPool,
        CPool,
        -- * Monads
        MonadCassandra,
        Cas,
        runCas,
        CassandraException(..),
        -- * Type used in operations
        Keyspace(..),
        Result(..),
        TableSpec(..),
        ColumnSpec(..),
        Metadata(..),
        CType(..),
        Row(..),
        Query(..),
        QueryID(..),
        Change(..),
        Table(..),
        Consistency(..),
        -- * Operations
        prepare,
        execute,
        -- * Value types
        CasType(..),
        CasValues(..),
        Blob(..)
    ) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad.Maybe
import Control.Monad.Reader
import Control.Monad.Trans
import Data.Bits
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C  -- ###
import Text.Hexdump  -- ###
import Data.Int
import Data.List
import Data.Map (Map)
import qualified Data.Map as M
import Data.Word
import Data.Typeable
import Data.IORef
import Data.Sequence (Seq, (|>))
import qualified Data.Sequence as Seq
import Data.Serialize hiding (Result)
import Data.String
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Network.Socket (Socket, HostName, ServiceName, getAddrInfo, socket, AddrInfo(..),
    connect, sClose)
import Network.Socket.ByteString (send, sendAll, recv)
import Numeric

type Server = (HostName, ServiceName)

data Session = Session {
        sesServer :: Server,
        sesSocket :: Maybe Socket
    }

data CPool = CPool {
        piKeyspace :: Keyspace,
        piSessions :: TVar (Seq (IORef Session))
    }

class MonadIO m => MonadCassandra m where
    getEltsandraPool :: m CPool

createCassandraPool :: [Server] -> Keyspace -> IO CPool
createCassandraPool svrs ks = do
    sessions <- forM svrs $ \svr -> newIORef $ Session {
            sesServer = svr,
            sesSocket = Nothing
        }
    sess <- atomically $ newTVar (Seq.fromList sessions)
    return $ CPool {
            piKeyspace = ks,
            piSessions = sess
        }

takeSession :: CPool -> IO (IORef Session)
takeSession pool = atomically $ do
    sess <- readTVar (piSessions pool)
    if Seq.null sess
        then retry
        else do
            let ses = sess `Seq.index` 0
            writeTVar (piSessions pool) (Seq.drop 1 sess)
            return ses

putSession :: CPool -> IORef Session -> IO ()
putSession pool ses = atomically $ modifyTVar (piSessions pool) (|> ses)

connectIfNeeded :: CPool -> IORef Session -> IO (Maybe Socket)
connectIfNeeded pool sesRef = do
    ses <- readIORef sesRef
    case sesSocket ses of
        Just socket -> return $ Just socket
        Nothing -> do
            let (host, service) = sesServer ses
            ais <- getAddrInfo Nothing (Just  host) (Just service)
            mSocket <- foldM (\mSocket ai -> do
                    case mSocket of
                        Nothing -> do
                            s <- socket (addrFamily ai) (addrSocketType ai) (addrProtocol ai)
                            do
                                connect s (addrAddress ai)
                                return (Just s)
                              `catch` \(exc :: IOException) -> do
                                sClose s
                                return Nothing
                        Just _ -> return mSocket
                ) Nothing ais
            case mSocket of
                Just socket -> do
                    writeIORef sesRef $ ses { sesSocket = Just socket }
                    introduce pool socket
                    return $ Just socket
                Nothing ->
                    return Nothing

data Flag = Compression | Tracing
    deriving Show

putFlags :: [Flag] -> Put
putFlags flags = putWord8 $ foldl' (+) 0 $ map toWord8 flags
  where
    toWord8 Compression = 0x01
    toWord8 Tracing = 0x02

getFlags :: Get [Flag]
getFlags = do
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
            0x00 -> return $ ERROR        
            0x01 -> return $ STARTUP      
            0x02 -> return $ READY        
            0x03 -> return $ AUTHENTICATE 
            0x04 -> return $ CREDENTIALS  
            0x05 -> return $ OPTIONS      
            0x06 -> return $ SUPPORTED    
            0x07 -> return $ QUERY        
            0x08 -> return $ RESULT                            
            0x09 -> return $ PREPARE      
            0x0a -> return $ EXECUTE      
            0x0b -> return $ REGISTER     
            0x0c -> return $ EVENT
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
    when (B.null bs) $ throwIO $ LocalProtocolException $ "short read"
    let left = n - B.length bs
    if left == 0
        then return bs
        else do
            bs' <- recvAll s left
            return (bs `B.append` bs')

protocolVersion :: Word8
protocolVersion = 1

recvFrame :: Socket -> IO (Frame ByteString)
recvFrame s = do
    hdrBs <- recvAll s 8
    case runGet parseHeader hdrBs of
        Left err -> throwIO $ LocalProtocolException $ "recvFrame: " `T.append` T.pack err
        Right (ver0, flags, stream, opcode, length) -> do
            let ver = ver0 .&. 0x7f
            when (ver /= protocolVersion) $
                throwIO $ LocalProtocolException $ "unexpected version " `T.append` T.pack (show ver)
            body <- if length == 0
                then pure B.empty
                else recvAll s (fromIntegral length)
            return $ Frame flags stream opcode body
  where
    parseHeader = do
        ver <- getWord8
        flags <- getFlags
        stream <- fromIntegral <$> getWord8
        opcode <- get
        length <- getWord32be
        return (ver, flags, stream, opcode, length)

sendFrame :: Socket -> Frame ByteString -> IO ()
sendFrame s fr@(Frame flags stream opcode body) = do
    let bs = runPut $ do
            putWord8 protocolVersion
            putFlags flags
            putWord8 (fromIntegral stream)
            put opcode
            putWord32be $ fromIntegral $ B.length body
            putByteString body
    -- putStrLn $ hexdump 0 (C.unpack bs)
    sendAll s bs

class ProtoElt a where
    getElt :: Get a
    putElt :: a -> Put

class CasType a where
    getCas :: Get a
    putCas :: a -> Put
    casType :: a -> CType

encodeElt :: ProtoElt a => a -> ByteString
encodeElt = runPut . putElt

encodeCas :: CasType a => a -> ByteString
encodeCas = runPut . putCas

decodeElt :: ProtoElt a => ByteString -> Either String a
decodeElt bs = runGet getElt bs

decodeCas :: CasType a => ByteString -> Either String a
decodeCas bs = runGet getCas bs

decodeEltM :: (ProtoElt a, MonadIO m) => Text -> ByteString -> m a
decodeEltM what bs =
    case decodeElt bs of
        Left err -> liftIO $ throwIO $ LocalProtocolException $
            "can't parse" `T.append` what `T.append` ": " `T.append` T.pack err
        Right res -> return res

newtype Long a = Long { unLong :: a } deriving (Eq, Ord, Show, Read)

instance Functor Long where
    f `fmap` Long a = Long (f a)

newtype Short a = Short { unShort :: a } deriving (Eq, Ord, Show, Read)

instance Functor Short where
    f `fmap` Short a = Short (f a)

instance ProtoElt (Map Text Text) where
    putElt = putElt . M.assocs
    getElt = M.fromList <$> getElt

instance ProtoElt [(Text, Text)] where
    putElt pairs = do
        putWord16be (fromIntegral $ length pairs)
        forM_ pairs $ \(key, value) -> do
            putElt key
            putElt value
    getElt = do
        n <- getWord16be
        replicateM (fromIntegral n) $ do
            key <- getElt
            value <- getElt
            return (key, value)

instance ProtoElt Text where
    putElt = putElt . T.encodeUtf8
    getElt = T.decodeUtf8 <$> getElt

instance ProtoElt (Long Text) where
    putElt = putElt . fmap T.encodeUtf8
    getElt = fmap T.decodeUtf8 <$> getElt

instance ProtoElt ByteString where
    putElt bs = do
        putWord16be (fromIntegral $ B.length bs)
        putByteString bs
    getElt = do
        len <- getWord16be
        getByteString (fromIntegral len)

instance ProtoElt (Long ByteString) where
    putElt (Long bs) = do
        putWord32be (fromIntegral $ B.length bs)
        putByteString bs
    getElt = do
        len <- getWord32be
        Long <$> getByteString (fromIntegral len)

data CassandraException = LocalProtocolException Text
                        | AuthenticationException Text
                        | DataCodingException Text
                        | ServerError Text
                        | ProtocolError Text
                        | BadCredentials Text
                        | UnavailableException Text Consistency Int Int
                        | Overloaded Text
                        | IsBootstrapping Text
                        | TruncateError Text
                        | WriteTimeout Text Consistency Int Int Text
                        | ReadTimeout Text Consistency Int Int Bool
                        | SyntaxError Text
                        | Unauthorized Text
                        | Invalid Text
                        | ConfigError Text
                        | AlreadyExists Text Keyspace Table
                        | Unprepared Text QueryID
    deriving (Show, Typeable)

instance Exception CassandraException where

throwError :: ByteString -> IO a
throwError bs = do
    case runGet parseError bs of
        Left err -> throwIO $ LocalProtocolException $ "failed to parse error: " `T.append` T.pack err
        Right exc -> throwIO exc
  where
    parseError :: Get CassandraException
    parseError = do
       code <- getWord32be
       case code of
            0x0000 -> ServerError <$> getElt 
            0x000A -> ProtocolError <$> getElt
            0x0100 -> BadCredentials <$> getElt
            0x1000 -> UnavailableException <$> getElt <*> getElt
                                           <*> (fromIntegral <$> getWord32be)
                                           <*> (fromIntegral <$> getWord32be)
            0x1001 -> Overloaded <$> getElt
            0x1002 -> IsBootstrapping <$> getElt
            0x1003 -> TruncateError <$> getElt
            0x1100 -> WriteTimeout <$> getElt <*> getElt
                                   <*> (fromIntegral <$> getWord32be)
                                   <*> (fromIntegral <$> getWord32be)
                                   <*> getElt
            0x1200 -> ReadTimeout <$> getElt <*> getElt
                                  <*> (fromIntegral <$> getWord32be)
                                  <*> (fromIntegral <$> getWord32be)
                                  <*> ((/=0) <$> getWord8)
            0x2000 -> SyntaxError <$> getElt
            0x2100 -> Unauthorized <$> getElt
            0x2200 -> Invalid <$> getElt
            0x2300 -> ConfigError <$> getElt
            0x2400 -> AlreadyExists <$> getElt <*> getElt <*> getElt
            0x2500 -> Unprepared <$> getElt <*> getElt
            _      -> fail $ "unknown error code 0x"++showHex code ""

introduce :: CPool -> Socket -> IO ()
introduce pool s = do
    sendFrame s $ Frame [] 0 STARTUP $ encodeElt $ ([("CQL_VERSION", "3.0.0")] :: [(Text, Text)])
    fr <- recvFrame s
    case frOpcode fr of
        AUTHENTICATE -> throwIO $ AuthenticationException "authentication not implemented yet"
        READY -> return ()
        ERROR -> throwError (frBody fr)
        op -> throwIO $ LocalProtocolException $ "introduce: unexpected opcode " `T.append` T.pack (show op)

withSession :: MonadCassandra m => (CPool -> Socket -> IO a) -> m a
withSession code = do
    pool <- getEltsandraPool
    mA <- liftIO $ bracket (takeSession pool) (putSession pool) $ \sesRef -> do
        mSocket <- connectIfNeeded pool sesRef
        case mSocket of
            Just socket -> do
                a <- code pool socket
                return (Just a)
            Nothing -> return Nothing
    case mA of
        Just a -> return a
        Nothing -> withSession code  -- Try again until we succeed

newtype Keyspace = Keyspace Text
    deriving (Eq, Ord, Show, IsString, ProtoElt)

newtype Table = Table Text
    deriving (Eq, Ord, Show, IsString, ProtoElt)

data TableSpec = TableSpec Keyspace Table
    deriving Show

instance ProtoElt TableSpec where
    putElt _ = error "formatting TableSpec is not implemented"
    getElt = TableSpec <$> getElt <*> getElt

data ColumnSpec = ColumnSpec TableSpec Text CType
    deriving Show

data Metadata = Metadata [ColumnSpec]
    deriving Show

data CType = CCustom Text
          | CAscii
          | CBigint
          | CBlob
          | CBoolean
          | CCounter
          | CDecimal
          | CDouble
          | CFloat
          | CInt
          | CText
          | CTimestamp
          | CUuid
          | CVarchar
          | CVarint
          | CTimeuuid
          | CInet
          | CList CType
          | CMap CType CType
          | CSet CType
    deriving (Eq, Ord, Show)

instance CasType ByteString where
    getCas = unLong <$> getElt
    putCas = putElt . Long
    casType _ = CAscii

instance CasType Int64 where
    getCas = fromIntegral <$> getWord64be
    putCas = putWord64be . fromIntegral
    casType _ = CBigint

newtype Blob = Blob ByteString
    deriving (Eq, Ord, Show)

instance CasType Blob where
    getCas = Blob . unLong <$> getElt
    putCas (Blob bs) = putElt $ Long bs
    casType _ = CBlob

instance CasType Bool where
    getCas = (/= 0) <$> getWord8
    putCas True = putWord8 1
    putCas False = putWord8 0
    casType _ = CBoolean

instance CasType Text where
    getCas = unLong <$> getElt
    putCas = putElt . Long
    casType _ = CText

instance ProtoElt CType where
    putElt _ = error "formatting CType is not implemented"
    getElt = do
        op <- getWord16be
        case op of
            0x0000 -> CCustom <$> getElt
            0x0001 -> pure CAscii
            0x0002 -> pure CBigint
            0x0003 -> pure CBlob
            0x0004 -> pure CBoolean
            0x0005 -> pure CCounter
            0x0006 -> pure CDecimal
            0x0007 -> pure CDouble
            0x0008 -> pure CFloat
            0x0009 -> pure CInt
            0x000a -> pure CVarchar
            0x000b -> pure CTimestamp
            0x000c -> pure CUuid
            0x000d -> pure CText
            0x000e -> pure CVarint
            0x000f -> pure CTimeuuid
            0x0010 -> pure CInet
            0x0020 -> CList <$> getElt
            0x0021 -> CMap <$> getElt <*> getElt
            0x0022 -> CSet <$> getElt

instance ProtoElt Metadata where
    putElt _ = error "formatting Metadata is not implemented"
    getElt = do
        flags <- getWord32be
        colCount <- fromIntegral <$> getWord32be
        gtSpec <- if (flags .&. 1) /= 0 then Just <$> getElt
                                        else pure Nothing
        cols <- replicateM colCount $ do
            tSpec <- case gtSpec of
                Just spec -> pure spec
                Nothing   -> getElt
            ColumnSpec tSpec <$> getElt <*> getElt
        return $ Metadata cols

newtype Row = Row [ByteString]
    deriving Show

newtype QueryID = QueryID ByteString
    deriving (Eq, Ord, Show, ProtoElt)

data Query = Query QueryID Metadata
    deriving Show

data Change = CREATED | UPDATED | DROPPED
    deriving (Eq, Ord, Show)

instance ProtoElt Change where
    putElt _ = error $ "formatting Change is not implemented"
    getElt = do
        str <- getElt :: Get Text
        case str of
            "CREATED" -> pure CREATED
            "UPDATED" -> pure UPDATED
            "DROPPED" -> pure DROPPED
            _ -> fail $ "unexpected change string: "++show str

data Result = Void
            | Rows Metadata [Row]
            | SetKeyspace Text
            | Prepared QueryID Metadata
            | SchemaChange Change Keyspace Table
    deriving Show

instance ProtoElt Result where
    putElt _ = error "formatting RESULT is not implemented"
    getElt = do
        kind <- getWord32be
        case kind of
            0x0001 -> pure Void
            0x0002 -> do
                meta@(Metadata colSpecs) <- getElt
                let colCount = length colSpecs
                rowCount <- fromIntegral <$> getWord32be
                rows <- replicateM rowCount (Row <$> replicateM colCount (unLong <$> getElt))
                return $ Rows meta rows
            0x0003 -> SetKeyspace <$> getElt
            0x0004 -> Prepared <$> getElt <*> getElt
            0x0005 -> SchemaChange <$> getElt <*> getElt <*> getElt
            _ -> fail $ "bad result kind: 0x"++showHex kind ""

prepare :: MonadCassandra m => Text -> m Query
prepare cql = withSession $ \pool s -> do
    sendFrame s $ Frame [] 0 PREPARE $ encodeElt (Long cql)
    fr <- recvFrame s
    case frOpcode fr of
        RESULT -> do
            res <- decodeEltM "RESULT" (frBody fr)
            case res of
                Prepared qid meta -> return $ Query qid meta
                _ -> throwIO $ LocalProtocolException $ "prepare: unexpected result " `T.append` T.pack (show res)
        _ -> throwIO $ LocalProtocolException $ "prepare: unexpected opcode " `T.append` T.pack (show (frOpcode fr))

data CodingFailure = Mismatch Int CType CType
                   | TooMany Int Int
                   | NotEnough Int Int
                   | DecodeFailure Int String

instance Show CodingFailure where
    show (Mismatch i t1 t2)    = "value index "++show (i+1)++" expected type "++show t1++", got "++show t2
    show (TooMany i1 i2)       = "too many values, expected "++show i1++" but got "++show i2
    show (NotEnough i1 i2)     = "not enough values, expected "++show i1++" but got "++show i2
    show (DecodeFailure i why) = "failed to decode value index "++show (i+1)++": "++why

class CasNested v where
    encodeNested :: Int -> v -> [CType] -> Either CodingFailure [ByteString]
    decodeNested :: Int -> [(CType, ByteString)] -> Either CodingFailure v
    countNested  :: v -> Int

instance CasNested () where
    encodeNested !i () [] = Right []
    encodeNested !i () ts = Left $ TooMany i (i + length ts)
    decodeNested !i []    = Right ()
    decodeNested !i vs    = Left $ TooMany (i + length vs) i
    countNested ()        = 0

instance (CasType a, CasNested rem) => CasNested (a, rem) where
    encodeNested !i (a, rem) (ta:trem) | ta == casType a =
        case encodeNested (i+1) rem trem of
            Left err -> Left err
            Right brem -> Right $ encodeCas a : brem
    encodeNested !i (a, _) (ta:_) = Left $ Mismatch i ta (casType a)
    encodeNested !i vs      []    = Left $ NotEnough (i + countNested vs) i 
    decodeNested !i ((ta, ba):rem) | ta == casType (undefined :: a) =
        case decodeCas ba of
            Left err -> Left $ DecodeFailure i err
            Right a ->
                case decodeNested (i+1) rem of
                    Left err -> Left err
                    Right arem -> Right (a, arem)  
    countNested (_, rem) = let n = 1 + countNested rem 
                           in  seq n n

class CasValues v where
    encodeValues :: v -> [CType] -> Either CodingFailure [ByteString]
    decodeValues :: [(CType, ByteString)] -> Either CodingFailure v

instance CasValues () where
    encodeValues () types = encodeNested 0 () types
    decodeValues vs = decodeNested 0 vs

instance CasType a => CasValues a where
    encodeValues a = encodeNested 0 (a, ())
    decodeValues vs = (\(a, ()) -> a) <$> decodeNested 0 vs

instance (CasType a, CasType b) => CasValues (a, b) where
    encodeValues (a, b) = encodeNested 0 (a, (b, ()))
    decodeValues vs = (\(a, (b, ())) -> (a, b)) <$> decodeNested 0 vs

data Consistency = ANY | ONE | TWO | THREE | QUORUM | ALL | LOCAL_QUORUM | EACH_QUORUM
    deriving (Eq, Ord, Show, Bounded, Enum)

instance ProtoElt Consistency where
    putElt c = putWord16be $ case c of
        ANY          -> 0x0000
        ONE          -> 0x0001
        TWO          -> 0x0002
        THREE        -> 0x0003
        QUORUM       -> 0x0004
        ALL          -> 0x0005
        LOCAL_QUORUM -> 0x0006
        EACH_QUORUM  -> 0x0007
    getElt = do
        w <- getWord16be
        case w of
            0x0000 -> pure ANY
            0x0001 -> pure ONE
            0x0002 -> pure TWO
            0x0003 -> pure THREE
            0x0004 -> pure QUORUM
            0x0005 -> pure ALL
            0x0006 -> pure LOCAL_QUORUM
            0x0007 -> pure EACH_QUORUM
            _      -> fail $ "unknown consistency value 0x"++showHex w ""

execute :: (MonadCassandra m, CasValues vs) => Query -> vs -> Consistency -> m Result
execute (Query qid (Metadata colspecs)) vs cons = withSession $ \pool s -> do
    let bss = encodeValues vs
    values <- case encodeValues vs (map (\(ColumnSpec _ _ typ) -> typ) colspecs) of
        Left err -> throwIO $ DataCodingException $ T.pack $ show err
        Right values -> return values
    print values
    sendFrame s $ Frame [] 0 EXECUTE $ runPut $ do
        putElt qid
        putWord16be (fromIntegral $ length values)
        mapM_ putCas values
        putElt cons
    fr <- recvFrame s
    case frOpcode fr of
        RESULT -> decodeEltM "RESULT" (frBody fr)
        ERROR -> throwError (frBody fr)
        _ -> throwIO $ LocalProtocolException $ "execute: unexpected opcode " `T.append` T.pack (show (frOpcode fr))

newtype Cas a = Cas (ReaderT CPool IO a)
    deriving (Functor, Applicative, Monad, MonadIO)

instance MonadCassandra Cas where
    getEltsandraPool = Cas ask

runCas :: CPool -> Cas a -> IO a
runCas pool (Cas code) = runReaderT code pool 

