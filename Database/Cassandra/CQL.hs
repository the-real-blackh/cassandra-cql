{-# LANGUAGE OverloadedStrings, GeneralizedNewtypeDeriving, ScopedTypeVariables,
        FlexibleInstances, DeriveDataTypeable, UndecidableInstances,
        BangPatterns, OverlappingInstances, DataKinds, GADTs, KindSignatures #-}
-- | Haskell client for Cassandra's CQL protocol
--
-- This module isn't properly documented yet. For now, take a look at tests/example.hs.
--
-- Credentials are not implemented yet.
--
-- Here's the correspondence between Haskell and CQL types. Not all Cassandra data
-- types supported as yet: Haskell types listed below have been implemented.
--
-- * ascii - 'ByteString'
--
-- * bigint - 'Int64'
--
-- * blob - 'Blob'
--
-- * boolean - 'Bool'
--
-- * counter - 'Counter'
--
-- * decimal
--
-- * double
--
-- * float
--
-- * int - 'Int'
--
-- * text - 'Text'
--
-- * timestamp
--
-- * uuid - 'UUID'
--
-- * varchar
--
-- * varint
--
-- * timeuuid
--
-- * inet
--
-- * list\<type\>
--
-- * map\<type, type\>
--
-- * set\<type\>
--
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
        TransportDirection(..),
        -- * Type used in operations
        Keyspace(..),
        Result(..),
        TableSpec(..),
        ColumnSpec(..),
        Metadata(..),
        CType(..),
        Change(..),
        Table(..),
        Consistency(..),
        -- * Queries
        Query,
        query,
        Style(..),
        -- * Operations
        execute,
        executeWrite,
        executeSchema,
        executeRaw,
        -- * Value types
        CasType(..),
        CasValues(..),
        Blob(..),
        Counter(..),
        metadataTypes
    ) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception (IOException, SomeException)
import Control.Monad.CatchIO
import Control.Monad.Maybe
import Control.Monad.Reader
import Control.Monad.State hiding (get, put)
import qualified Control.Monad.State as State
import Control.Monad.Trans
import Crypto.Hash (hash, Digest, SHA1)
import Data.Bits
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import Data.Either (lefts)
import Data.Int
import Data.List
import Data.Map (Map)
import qualified Data.Map as M
import Data.Maybe
import Data.Sequence (Seq, (|>))
import qualified Data.Sequence as Seq
import Data.Serialize hiding (Result)
import Data.String
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Typeable
import Data.UUID (UUID)
import qualified Data.UUID as UUID
import Data.Word
import Network.Socket (Socket, HostName, ServiceName, getAddrInfo, socket, AddrInfo(..),
    connect, sClose)
import Network.Socket.ByteString (send, sendAll, recv)
import Numeric

type Server = (HostName, ServiceName)

data ActiveSession = ActiveSession {
        actSocket     :: Socket,
        actQueryCache :: Map QueryID PreparedQuery
    }

data Session = Session {
        sesServer :: Server,
        sesActive :: Maybe ActiveSession
    }

data CPool = CPool {
        piKeyspace :: Keyspace,
        piSessions :: TVar (Seq Session)
    }

class MonadCatchIO m => MonadCassandra m where
    getEltsandraPool :: m CPool

createCassandraPool :: [Server] -> Keyspace -> IO CPool
createCassandraPool svrs ks = do
    let sessions = map (\svr -> Session svr Nothing) svrs
    sess <- atomically $ newTVar (Seq.fromList sessions)
    return $ CPool {
            piKeyspace = ks,
            piSessions = sess
        }

takeSession :: CPool -> IO Session
takeSession pool = atomically $ do
    sess <- readTVar (piSessions pool)
    if Seq.null sess
        then retry
        else do
            let ses = sess `Seq.index` 0
            writeTVar (piSessions pool) (Seq.drop 1 sess)
            return ses

putSession :: CPool -> Session -> IO ()
putSession pool ses = atomically $ modifyTVar (piSessions pool) (|> ses)

connectIfNeeded :: CPool -> Session -> IO Session
connectIfNeeded pool session =
    if isJust (sesActive session)
        then return session
        else do
            let (host, service) = sesServer session
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
                    let active = ActiveSession {
                                actSocket = socket,
                                actQueryCache = M.empty
                            }
                    active' <- execStateT (introduce pool) active
                    return $ session { sesActive = Just active' }
                Nothing ->
                    return session

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
    when (B.null bs) $ throw $ LocalProtocolError $ "short read"
    let left = n - B.length bs
    if left == 0
        then return bs
        else do
            bs' <- recvAll s left
            return (bs `B.append` bs')

protocolVersion :: Word8
protocolVersion = 1

recvFrame :: StateT ActiveSession IO (Frame ByteString)
recvFrame = do
    s <- gets actSocket
    hdrBs <- liftIO $ recvAll s 8
    case runGet parseHeader hdrBs of
        Left err -> throw $ LocalProtocolError $ "recvFrame: " `T.append` T.pack err
        Right (ver0, flags, stream, opcode, length) -> do
            let ver = ver0 .&. 0x7f
            when (ver /= protocolVersion) $
                throw $ LocalProtocolError $ "unexpected version " `T.append` T.pack (show ver)
            body <- if length == 0
                then pure B.empty
                else liftIO $ recvAll s (fromIntegral length)
            return $ Frame flags stream opcode body
  where
    parseHeader = do
        ver <- getWord8
        flags <- getFlags
        stream <- fromIntegral <$> getWord8
        opcode <- get
        length <- getWord32be
        return (ver, flags, stream, opcode, length)

sendFrame :: Frame ByteString -> StateT ActiveSession IO ()
sendFrame fr@(Frame flags stream opcode body) = do
    let bs = runPut $ do
            putWord8 protocolVersion
            putFlags flags
            putWord8 (fromIntegral stream)
            put opcode
            putWord32be $ fromIntegral $ B.length body
            putByteString body
    --liftIO $ putStrLn $ hexdump 0 (C.unpack bs)
    s <- gets actSocket
    liftIO $ sendAll s bs

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
        Left err -> throw $ LocalProtocolError $
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

data TransportDirection = TransportSending | TransportReceiving
    deriving Show

data CassandraException = LocalProtocolError Text
                        | AuthenticationException Text
                        | ValueMarshallingException TransportDirection Text
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
                        | Unprepared Text PreparedQueryID
    deriving (Show, Typeable)

instance Exception CassandraException where

throwError :: MonadCatchIO m => ByteString -> m a
throwError bs = do
    case runGet parseError bs of
        Left err -> throw $ LocalProtocolError $ "failed to parse error: " `T.append` T.pack err
        Right exc -> throw exc
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

introduce :: CPool -> StateT ActiveSession IO ()
introduce pool = do
    sendFrame $ Frame [] 0 STARTUP $ encodeElt $ ([("CQL_VERSION", "3.0.0")] :: [(Text, Text)])
    fr <- recvFrame
    case frOpcode fr of
        AUTHENTICATE -> throw $ AuthenticationException "authentication not implemented yet"
        READY -> return ()
        ERROR -> throwError (frBody fr)
        op -> throw $ LocalProtocolError $ "introduce: unexpected opcode " `T.append` T.pack (show op)
    let Keyspace ksName = piKeyspace pool
    let q = query $ "USE " `T.append` ksName :: Query Rows () 
    res <- executeInternal q () ONE
    case res of
        SetKeyspace ks -> return ()
        _ -> throw $ ProtocolError $ "expected SetKeyspace, but got " `T.append` T.pack (show res)
                              `T.append` " for query: " `T.append` T.pack (show q)

withSession :: MonadCassandra m => (CPool -> StateT ActiveSession IO a) -> m a
withSession code = do
    pool <- getEltsandraPool
    mA <- liftIO $ do
        session <- connectIfNeeded pool =<< takeSession pool
        case sesActive session of
            Just active -> do
                (a, active') <- runStateT (code pool) active
                putSession pool $ session { sesActive = Just active' }
                return (Just a)
              `catches` [
                -- Close the session if we get any IOException
                Handler $ \(exc :: IOException) -> do
                    sClose (actSocket active)
                    putSession pool $ session { sesActive = Nothing }
                    throw exc,
                Handler $ \(exc :: SomeException) -> do
                    putSession pool session
                    throw exc
              ]
            Nothing -> do
                putSession pool session
                return Nothing
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
    getCas = getByteString =<< remaining
    putCas = putByteString
    casType _ = CAscii

instance CasType Int64 where
    getCas = fromIntegral <$> getWord64be
    putCas = putWord64be . fromIntegral
    casType _ = CBigint

newtype Blob = Blob ByteString
    deriving (Eq, Ord, Show)

instance CasType Blob where
    getCas = Blob <$> (getByteString =<< remaining)
    putCas (Blob bs) = putByteString bs
    casType _ = CBlob

instance CasType Bool where
    getCas = (/= 0) <$> getWord8
    putCas True = putWord8 1
    putCas False = putWord8 0
    casType _ = CBoolean

newtype Counter = Counter Int64
    deriving (Eq, Ord, Show, Read)

instance CasType Counter where
    getCas = Counter . fromIntegral <$> getWord64be
    putCas (Counter c) = putWord64be (fromIntegral c)
    casType _ = CCounter

instance CasType Int where
    getCas = fromIntegral <$> getWord32be
    putCas = putWord32be . fromIntegral
    casType _ = CInt

instance CasType Text where
    getCas = T.decodeUtf8 <$> (getByteString =<< remaining)
    putCas = putByteString . T.encodeUtf8
    casType _ = CText

instance CasType UUID where
    getCas = do
        mUUID <- UUID.fromByteString . L.fromStrict <$> (getByteString =<< remaining)
        case mUUID of
            Just uuid -> return uuid
            Nothing -> fail "malformed UUID"
    putCas = putByteString . L.toStrict . UUID.toByteString
    casType _ = CUuid

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
            _      -> fail $ "unknown data type code 0x"++showHex op ""

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

newtype PreparedQueryID = PreparedQueryID ByteString
    deriving (Eq, Ord, Show, ProtoElt)

newtype QueryID = QueryID (Digest SHA1)
    deriving (Eq, Ord, Show)

data Style = Rows | Write | Schema

data Query :: Style -> * -> * where
    Query :: QueryID -> Text -> Query style values
    deriving Show

instance IsString (Query style values) where
    fromString = query . T.pack

query :: Text -> Query style values
query cql = Query (QueryID . hash . T.encodeUtf8 $ cql) cql

data PreparedQuery = PreparedQuery PreparedQueryID Metadata
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

data Result vs = Void
               | RowsResult Metadata [vs]
               | SetKeyspace Text
               | Prepared PreparedQueryID Metadata
               | SchemaChange Change Keyspace Table
    deriving Show

instance Functor Result where
    f `fmap` Void = Void
    f `fmap` RowsResult meta rows = RowsResult meta (f `fmap` rows)
    f `fmap` SetKeyspace ks = SetKeyspace ks
    f `fmap` Prepared pqid meta = Prepared pqid meta
    f `fmap` SchemaChange ch ks t = SchemaChange ch ks t

instance ProtoElt (Result [ByteString]) where
    putElt _ = error "formatting RESULT is not implemented"
    getElt = do
        kind <- getWord32be
        case kind of
            0x0001 -> pure Void
            0x0002 -> do
                meta@(Metadata colSpecs) <- getElt
                let colCount = length colSpecs
                rowCount <- fromIntegral <$> getWord32be
                rows <- replicateM rowCount (replicateM colCount (unLong <$> getElt))
                return $ RowsResult meta rows
            0x0003 -> SetKeyspace <$> getElt
            0x0004 -> Prepared <$> getElt <*> getElt
            0x0005 -> SchemaChange <$> getElt <*> getElt <*> getElt
            _ -> fail $ "bad result kind: 0x"++showHex kind ""

prepare :: Query style values -> StateT ActiveSession IO PreparedQuery
prepare (Query qid cql) = do
    cache <- gets actQueryCache
    case qid `M.lookup` cache of
        Just pq -> return pq
        Nothing -> do
            sendFrame $ Frame [] 0 PREPARE $ encodeElt (Long cql)
            fr <- recvFrame
            case frOpcode fr of
                RESULT -> do
                    res <- decodeEltM "RESULT" (frBody fr)
                    case (res :: Result [ByteString]) of
                        Prepared pqid meta -> do
                            let pq = PreparedQuery pqid meta
                            modify $ \act -> act { actQueryCache = M.insert qid pq (actQueryCache act) }
                            return pq
                        _ -> throw $ LocalProtocolError $ "prepare: unexpected result " `T.append` T.pack (show res)
                ERROR -> throwError (frBody fr)
                _ -> throw $ LocalProtocolError $ "prepare: unexpected opcode " `T.append` T.pack (show (frOpcode fr))

data CodingFailure = Mismatch Int CType CType
                   | WrongNumber Int Int
                   | DecodeFailure Int String

instance Show CodingFailure where
    show (Mismatch i t1 t2)    = "at value index "++show (i+1)++", Haskell type specifies "++show t1++", but database metadata says "++show t2
    show (WrongNumber i1 i2)   = "wrong number of values: Haskell type specifies "++show i1++" but database metadata says "++show i2
    show (DecodeFailure i why) = "failed to decode value index "++show (i+1)++": "++why

class CasNested v where
    encodeNested :: Int -> v -> [CType] -> Either CodingFailure [ByteString]
    decodeNested :: Int -> [(CType, ByteString)] -> Either CodingFailure v
    countNested  :: v -> Int

instance CasNested () where
    encodeNested !i () [] = Right []
    encodeNested !i () ts = Left $ WrongNumber i (i + length ts)
    decodeNested !i []    = Right ()
    decodeNested !i vs    = Left $ WrongNumber i (i + length vs)
    countNested _         = 0

instance (CasType a, CasNested rem) => CasNested (a, rem) where
    encodeNested !i (a, rem) (ta:trem) | ta == casType a =
        case encodeNested (i+1) rem trem of
            Left err -> Left err
            Right brem -> Right $ encodeCas a : brem
    encodeNested !i (a, _) (ta:_) = Left $ Mismatch i (casType a) ta
    encodeNested !i vs      []    = Left $ WrongNumber (i + countNested vs) i 
    decodeNested !i ((ta, ba):rem) | ta == casType (undefined :: a) =
        case decodeCas ba of
            Left err -> Left $ DecodeFailure i err
            Right a ->
                case decodeNested (i+1) rem of
                    Left err -> Left err
                    Right arem -> Right (a, arem)
    decodeNested !i ((ta, _):rem) = Left $ Mismatch i (casType (undefined :: a)) ta
    decodeNested !i []            = Left $ WrongNumber (i + 1 + countNested (undefined :: rem)) i
    countNested _ = let n = 1 + countNested (undefined :: rem) 
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

instance (CasType a, CasType b, CasType c) => CasValues (a, b, c) where
    encodeValues (a, b, c) = encodeNested 0 (a, (b, (c, ())))
    decodeValues vs = (\(a, (b, (c, ()))) -> (a, b, c)) <$> decodeNested 0 vs

instance (CasType a, CasType b, CasType c, CasType d) => CasValues (a, b, c, d) where
    encodeValues (a, b, c, d) = encodeNested 0 (a, (b, (c, (d, ()))))
    decodeValues vs = (\(a, (b, (c, (d, ())))) -> (a, b, c, d)) <$> decodeNested 0 vs

instance (CasType a, CasType b, CasType c, CasType d, CasType e) => CasValues (a, b, c, d, e) where
    encodeValues (a, b, c, d, e) = encodeNested 0 (a, (b, (c, (d, (e, ())))))
    decodeValues vs = (\(a, (b, (c, (d, (e, ()))))) -> (a, b, c, d, e)) <$> decodeNested 0 vs

instance (CasType a, CasType b, CasType c, CasType d, CasType e,
          CasType f) => CasValues (a, b, c, d, e, f) where
    encodeValues (a, b, c, d, e, f) =
        encodeNested 0 (a, (b, (c, (d, (e, (f, ()))))))
    decodeValues vs = (\(a, (b, (c, (d, (e, (f, ())))))) ->
        (a, b, c, d, e, f)) <$> decodeNested 0 vs

instance (CasType a, CasType b, CasType c, CasType d, CasType e,
          CasType f, CasType g) => CasValues (a, b, c, d, e, f, g) where
    encodeValues (a, b, c, d, e, f, g) =
        encodeNested 0 (a, (b, (c, (d, (e, (f, (g, ())))))))
    decodeValues vs = (\(a, (b, (c, (d, (e, (f, (g, ()))))))) ->
        (a, b, c, d, e, f, g)) <$> decodeNested 0 vs

instance (CasType a, CasType b, CasType c, CasType d, CasType e,
          CasType f, CasType g, CasType h) => CasValues (a, b, c, d, e, f, g, h) where
    encodeValues (a, b, c, d, e, f, g, h) =
        encodeNested 0 (a, (b, (c, (d, (e, (f, (g, (h, ()))))))))
    decodeValues vs = (\(a, (b, (c, (d, (e, (f, (g, (h, ())))))))) ->
        (a, b, c, d, e, f, g, h)) <$> decodeNested 0 vs

instance (CasType a, CasType b, CasType c, CasType d, CasType e,
          CasType f, CasType g, CasType h, CasType i) => CasValues (a, b, c, d, e, f, g, h, i) where
    encodeValues (a, b, c, d, e, f, g, h, i) =
        encodeNested 0 (a, (b, (c, (d, (e, (f, (g, (h, (i, ())))))))))
    decodeValues vs = (\(a, (b, (c, (d, (e, (f, (g, (h, (i, ()))))))))) ->
        (a, b, c, d, e, f, g, h, i)) <$> decodeNested 0 vs

instance (CasType a, CasType b, CasType c, CasType d, CasType e,
          CasType f, CasType g, CasType h, CasType i, CasType j)
              => CasValues (a, b, c, d, e, f, g, h, i, j) where
    encodeValues (a, b, c, d, e, f, g, h, i, j) =
        encodeNested 0 (a, (b, (c, (d, (e, (f, (g, (h, (i, (j, ()))))))))))
    decodeValues vs = (\(a, (b, (c, (d, (e, (f, (g, (h, (i, (j, ())))))))))) ->
        (a, b, c, d, e, f, g, h, i, j)) <$> decodeNested 0 vs

instance (CasType a, CasType b, CasType c, CasType d, CasType e,
          CasType f, CasType g, CasType h, CasType i, CasType j,
          CasType k)
              => CasValues (a, b, c, d, e, f, g, h, i, j, k) where
    encodeValues (a, b, c, d, e, f, g, h, i, j, k) =
        encodeNested 0 (a, (b, (c, (d, (e, (f, (g, (h, (i, (j, (k, ())))))))))))
    decodeValues vs = (\(a, (b, (c, (d, (e, (f, (g, (h, (i, (j, (k, ()))))))))))) ->
        (a, b, c, d, e, f, g, h, i, j, k)) <$> decodeNested 0 vs

instance (CasType a, CasType b, CasType c, CasType d, CasType e,
          CasType f, CasType g, CasType h, CasType i, CasType j,
          CasType k, CasType l)
              => CasValues (a, b, c, d, e, f, g, h, i, j, k, l) where
    encodeValues (a, b, c, d, e, f, g, h, i, j, k, l) =
        encodeNested 0 (a, (b, (c, (d, (e, (f, (g, (h, (i, (j, (k, (l, ()))))))))))))
    decodeValues vs = (\(a, (b, (c, (d, (e, (f, (g, (h, (i, (j, (k, (l, ())))))))))))) ->
        (a, b, c, d, e, f, g, h, i, j, k, l)) <$> decodeNested 0 vs

instance (CasType a, CasType b, CasType c, CasType d, CasType e,
          CasType f, CasType g, CasType h, CasType i, CasType j,
          CasType k, CasType l, CasType m)
              => CasValues (a, b, c, d, e, f, g, h, i, j, k, l, m) where
    encodeValues (a, b, c, d, e, f, g, h, i, j, k, l, m) =
        encodeNested 0 (a, (b, (c, (d, (e, (f, (g, (h, (i, (j, (k, (l, (m, ())))))))))))))
    decodeValues vs = (\(a, (b, (c, (d, (e, (f, (g, (h, (i, (j, (k, (l, (m, ()))))))))))))) ->
        (a, b, c, d, e, f, g, h, i, j, k, l, m)) <$> decodeNested 0 vs

instance (CasType a, CasType b, CasType c, CasType d, CasType e,
          CasType f, CasType g, CasType h, CasType i, CasType j,
          CasType k, CasType l, CasType m, CasType n)
              => CasValues (a, b, c, d, e, f, g, h, i, j, k, l, m, n) where
    encodeValues (a, b, c, d, e, f, g, h, i, j, k, l, m, n) =
        encodeNested 0 (a, (b, (c, (d, (e, (f, (g, (h, (i, (j, (k, (l, (m, (n, ()))))))))))))))
    decodeValues vs = (\(a, (b, (c, (d, (e, (f, (g, (h, (i, (j, (k, (l, (m, (n, ())))))))))))))) ->
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n)) <$> decodeNested 0 vs

instance (CasType a, CasType b, CasType c, CasType d, CasType e,
          CasType f, CasType g, CasType h, CasType i, CasType j,
          CasType k, CasType l, CasType m, CasType n, CasType o)
              => CasValues (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o) where
    encodeValues (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o) =
        encodeNested 0 (a, (b, (c, (d, (e, (f, (g, (h, (i, (j, (k, (l, (m, (n, (o, ())))))))))))))))
    decodeValues vs = (\(a, (b, (c, (d, (e, (f, (g, (h, (i, (j, (k, (l, (m, (n, (o, ()))))))))))))))) ->
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)) <$> decodeNested 0 vs

instance (CasType a, CasType b, CasType c, CasType d, CasType e,
          CasType f, CasType g, CasType h, CasType i, CasType j,
          CasType k, CasType l, CasType m, CasType n, CasType o,
          CasType p)
              => CasValues (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p) where
    encodeValues (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p) =
        encodeNested 0 (a, (b, (c, (d, (e, (f, (g, (h, (i, (j, (k, (l, (m, (n, (o, (p, ()))))))))))))))))
    decodeValues vs = (\(a, (b, (c, (d, (e, (f, (g, (h, (i, (j, (k, (l, (m, (n, (o, (p, ())))))))))))))))) ->
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)) <$> decodeNested 0 vs

instance (CasType a, CasType b, CasType c, CasType d, CasType e,
          CasType f, CasType g, CasType h, CasType i, CasType j,
          CasType k, CasType l, CasType m, CasType n, CasType o,
          CasType p, CasType q)
              => CasValues (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q) where
    encodeValues (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q) =
        encodeNested 0 (a, (b, (c, (d, (e, (f, (g, (h, (i, (j, (k, (l, (m, (n, (o, (p, (q, ())))))))))))))))))
    decodeValues vs = (\(a, (b, (c, (d, (e, (f, (g, (h, (i, (j, (k, (l, (m, (n, (o, (p, (q, ()))))))))))))))))) ->
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q)) <$> decodeNested 0 vs

instance (CasType a, CasType b, CasType c, CasType d, CasType e,
          CasType f, CasType g, CasType h, CasType i, CasType j,
          CasType k, CasType l, CasType m, CasType n, CasType o,
          CasType p, CasType q, CasType r)
              => CasValues (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r) where
    encodeValues (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r) =
        encodeNested 0 (a, (b, (c, (d, (e, (f, (g, (h, (i, (j, (k, (l, (m, (n, (o, (p, (q, (r, ()))))))))))))))))))
    decodeValues vs = (\(a, (b, (c, (d, (e, (f, (g, (h, (i, (j, (k, (l, (m, (n, (o, (p, (q, (r, ())))))))))))))))))) ->
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r)) <$> decodeNested 0 vs

instance (CasType a, CasType b, CasType c, CasType d, CasType e,
          CasType f, CasType g, CasType h, CasType i, CasType j,
          CasType k, CasType l, CasType m, CasType n, CasType o,
          CasType p, CasType q, CasType r, CasType s)
              => CasValues (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s) where
    encodeValues (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s) =
        encodeNested 0 (a, (b, (c, (d, (e, (f, (g, (h, (i, (j, (k, (l, (m, (n, (o, (p, (q, (r, (s, ())))))))))))))))))))
    decodeValues vs = (\(a, (b, (c, (d, (e, (f, (g, (h, (i, (j, (k, (l, (m, (n, (o, (p, (q, (r, (s, ()))))))))))))))))))) ->
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s)) <$> decodeNested 0 vs

instance (CasType a, CasType b, CasType c, CasType d, CasType e,
          CasType f, CasType g, CasType h, CasType i, CasType j,
          CasType k, CasType l, CasType m, CasType n, CasType o,
          CasType p, CasType q, CasType r, CasType s, CasType t)
              => CasValues (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t) where
    encodeValues (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t) =
        encodeNested 0 (a, (b, (c, (d, (e, (f, (g, (h, (i, (j, (k, (l, (m, (n, (o, (p, (q, (r, (s, (t, ()))))))))))))))))))))
    decodeValues vs = (\(a, (b, (c, (d, (e, (f, (g, (h, (i, (j, (k, (l, (m, (n, (o, (p, (q, (r, (s, (t, ())))))))))))))))))))) ->
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)) <$> decodeNested 0 vs

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

executeRaw :: (MonadCassandra m, CasValues values) =>
              Query style ignored -> values -> Consistency -> m (Result [ByteString])
executeRaw query i cons = withSession $ \_ -> executeInternal query i cons

executeInternal :: CasValues values =>
                   Query style ignored -> values -> Consistency -> StateT ActiveSession IO (Result [ByteString])
executeInternal query i cons = do
    pq@(PreparedQuery pqid queryMeta) <- prepare query
    values <- case encodeValues i (metadataTypes queryMeta) of
        Left err -> throw $ ValueMarshallingException TransportSending $ T.pack $ show err
        Right values -> return values
    sendFrame $ Frame [] 0 EXECUTE $ runPut $ do
        putElt pqid
        putWord16be (fromIntegral $ length values)
        forM_ values $ \value -> do
            let enc = encodeCas value
            putWord32be (fromIntegral $ B.length enc) 
            putByteString enc
        putElt cons
    fr <- recvFrame
    case frOpcode fr of
        RESULT -> decodeEltM "RESULT" (frBody fr)
        ERROR -> throwError (frBody fr)
        _ -> throw $ LocalProtocolError $ "execute: unexpected opcode " `T.append` T.pack (show (frOpcode fr))

-- | Execute a query that returns rows
execute :: (MonadCassandra m, CasValues values) =>
           Consistency -> Query Rows values -> m [values]
execute cons q = do
    res <- executeRaw q () cons
    case res of
        RowsResult meta rows -> decodeRows meta rows
        _ -> throw $ ProtocolError $ "expected Rows, but got " `T.append` T.pack (show res)
                              `T.append` " for query: " `T.append` T.pack (show q)

decodeRows :: (MonadCatchIO m, CasValues values) => Metadata -> [[ByteString]] -> m [values]
decodeRows meta rows0 = do
    let rows1 = flip map rows0 $ \cols -> decodeValues (zip (metadataTypes meta) cols)
    case lefts rows1 of
        (err:_) -> throw $ ValueMarshallingException TransportReceiving $ T.pack $ show err
        [] -> return ()
    let rows2 = flip map rows1 $ \(Right v) -> v
    return $ rows2

-- | Execute a write operation that returns void
executeWrite :: (MonadCassandra m, CasValues values) =>
                Consistency -> Query Write values -> values -> m ()
executeWrite cons q i = do
    res <- executeRaw q i cons
    case res of
        Void -> return ()
        _ -> throw $ ProtocolError $ "expected Void, but got " `T.append` T.pack (show res)
                              `T.append` " for query: " `T.append` T.pack (show q)

-- | Execute a schema change, such as creating or dropping a table.
executeSchema :: (MonadCassandra m, CasValues values) =>
                 Consistency -> Query Schema values -> values -> m (Change, Keyspace, Table)
executeSchema cons q i = do
    res <- executeRaw q i cons
    case res of
        SchemaChange ch ks ta -> return (ch, ks, ta)
        _ -> throw $ ProtocolError $ "expected SchemaChange, but got " `T.append` T.pack (show res)
                              `T.append` " for query: " `T.append` T.pack (show q)

metadataTypes :: Metadata -> [CType]
metadataTypes (Metadata colspecs) = map (\(ColumnSpec _ _ typ) -> typ) colspecs

newtype Cas a = Cas (ReaderT CPool IO a)
    deriving (Functor, Applicative, Monad, MonadIO, MonadCatchIO)

instance MonadCassandra Cas where
    getEltsandraPool = Cas ask

runCas :: CPool -> Cas a -> IO a
runCas pool (Cas code) = runReaderT code pool 

