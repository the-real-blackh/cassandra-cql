{-# LANGUAGE OverloadedStrings, GeneralizedNewtypeDeriving, ScopedTypeVariables,
        FlexibleInstances, DeriveDataTypeable, UndecidableInstances,
        BangPatterns, OverlappingInstances, DataKinds, GADTs, KindSignatures #-}
-- | Haskell client for Cassandra's CQL protocol
--
-- For examples, take a look at the /tests/ directory in the source archive. 
--
-- Here's the correspondence between CQL and Haskell types:
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
-- * decimal - 'Decimal'
--
-- * double - 'Double'
--
-- * float - 'Float'
--
-- * int - 'Int'
--
-- * text / varchar - 'Text'
--
-- * timestamp - 'UTCTime'
--
-- * uuid - 'UUID'
--
-- * varint - 'Integer'
--
-- * timeuuid - 'TimeUUID'
--
-- * inet - 'SockAddr'
--
-- * list\<a\> - [a]
--
-- * map\<a, b\> - 'Map' a b
--
-- * set\<a\> - 'Set' b
--
-- ...and you can define your own 'CasType' instances to extend these types, which is
-- a very powerful way to write your code.
--
-- One way to do things is to specify your queries with a type signature, like this:
--
-- > createSongs :: Query Schema () ()
-- > createSongs = "create table songs (id uuid PRIMARY KEY, title text, artist text, comment text)"
-- >
-- > insertSong :: Query Write (UUID, Text, Text, Maybe Text) ()
-- > insertSong = "insert into songs (id, title, artist, comment) values (?, ?, ?)"
-- >
-- > getOneSong :: Query Rows UUID (Text, Text, Maybe Text)
-- > getOneSong = "select title, artist, comment from songs where id=?"
--
-- The three type parameters are the query type ('Schema', 'Write' or 'Rows') followed by the
-- input and output types, which are given as tuples whose constituent types must match
-- the ones in the query CQL. If you do not match them correctly, you'll get a runtime
-- error when you execute the query. If you do, then the query becomes completely type
-- safe.
--
-- Types can be 'Maybe' types, in which case you can read and write a Cassandra \'null\'
-- in the table. Cassandra allows any column to be null, but you can lock this out by
-- specifying non-Maybe types.
--
-- The query types are:
--
-- * 'Schema' for modifications to the schema. The output tuple type must be ().
--
-- * 'Write' for row inserts and updates, and such. The output tuple type must be ().
--
-- * 'Rows' for selects that give a list of rows in response.
--
-- The functions to use for these query types are 'executeSchema', 'executeWrite' and
-- 'executeRows' or 'executeRow' respectively.
--
-- The following pattern seems to work very well, especially along with your own 'CasType'
-- instances, because it gives you a place to neatly add marshalling details that keeps
-- away from the body of your code.
--
-- > insertSong :: UUID -> Text -> Text -> Maybe Text -> Cas ()
-- > insertSong id title artist comment = executeWrite QUORUM q (id, title, artist, comment)
-- >      where q = "insert into songs (id, title, artist, comment) values (?, ?, ?, ?)"
--
-- Incidentally, here's Haskell's little-known multi-line string syntax.
-- You escape it using \\ and then another \\ where the string starts again.
--
-- > str = "multi\
-- >        \line"
--
-- (gives \"multiline\")
--
-- /To do/
--
-- * Add credentials.
--
-- * Improve connection pooling.
--
-- * Add the ability to easily run queries in parallel.

module Database.Cassandra.CQL (
        -- * Initialization
        Server,
        Keyspace(..),
        Pool,
        newPool,
        -- * Cassandra monad
        MonadCassandra(..),
        Cas,
        runCas,
        CassandraException(..),
        CassandraCommsError(..),
        TransportDirection(..),
        -- * Queries
        Query,
        Style(..),
        query,
        -- * Executing queries
        Consistency(..),
        Change(..),
        executeSchema,
        executeWrite,
        executeRows,
        executeRow,
        -- * Value types
        Blob(..),
        Counter(..),
        TimeUUID(..),
        metadataTypes,
        CasType(..),
        CasValues(..),
        -- * Lower-level interfaces
        executeRaw,
        Result(..),
        TableSpec(..),
        ColumnSpec(..),
        Metadata(..),
        CType(..),
        Table(..),
        PreparedQueryID(..)
    ) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception (IOException, SomeException)
import Control.Monad.CatchIO
import Control.Monad.Reader
import Control.Monad.State hiding (get, put)
import qualified Control.Monad.State as State
import qualified Control.Monad.Reader
import qualified Control.Monad.RWS
import qualified Control.Monad.State
import qualified Control.Monad.State.Strict
import qualified Control.Monad.Error
import qualified Control.Monad.Writer
import Control.Monad.Trans
import Crypto.Hash (hash, Digest, SHA1)
import Data.Bits
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import Data.Data
import Data.Decimal
import Data.Either (lefts)
import Data.Int
import Data.List
import Data.Map (Map)
import qualified Data.Map as M
import Data.Maybe
import Data.Monoid (Monoid)
import Data.Sequence (Seq, (|>))
import qualified Data.Sequence as Seq
import Data.Serialize hiding (Result)
import Data.Set (Set)
import qualified Data.Set as S
import Foreign.Storable
import Data.String
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Time.Calendar
import Data.Time.Clock
import Data.Typeable
import Data.UUID (UUID)
import qualified Data.UUID as UUID
import Data.Word
import Network.Socket (Socket, HostName, ServiceName, getAddrInfo, socket, AddrInfo(..),
    connect, sClose, SockAddr(..), SocketType(..), defaultHints)
import Network.Socket.ByteString (send, sendAll, recv)
import Numeric
import Unsafe.Coerce
--import qualified Data.ByteString.Char8 as C
--import Text.Hexdump

type Server = (HostName, ServiceName)

data ActiveSession = ActiveSession {
        actSocket     :: Socket,
        actQueryCache :: Map QueryID PreparedQuery
    }

data Session = Session {
        sesServer :: Server,
        sesActive :: Maybe ActiveSession
    }

-- | A handle for the state of the connection pool.
data Pool = Pool {
        piKeyspace :: Keyspace,
        piSessions :: TVar (Seq Session)
    }

class MonadCatchIO m => MonadCassandra m where
    getCassandraPool :: m Pool

instance MonadCassandra m => MonadCassandra (Control.Monad.Reader.ReaderT a m) where
    getCassandraPool = lift getCassandraPool

instance MonadCassandra m => MonadCassandra (Control.Monad.State.StateT a m) where
    getCassandraPool = lift getCassandraPool

instance (MonadCassandra m, Control.Monad.Error.Error e) => MonadCassandra (Control.Monad.Error.ErrorT e m) where
    getCassandraPool = lift getCassandraPool

instance (MonadCassandra m, Monoid a) => MonadCassandra (Control.Monad.Writer.WriterT a m) where
    getCassandraPool = lift getCassandraPool

instance (MonadCassandra m, Monoid w) => MonadCassandra (Control.Monad.RWS.RWST r w s m) where
    getCassandraPool = lift getCassandraPool

-- | Construct a pool of Cassandra connections.
newPool :: [Server] -> Keyspace -> IO Pool
newPool svrs ks = do
    let sessions = map (\svr -> Session svr Nothing) svrs
    sess <- atomically $ newTVar (Seq.fromList sessions)
    return $ Pool {
            piKeyspace = ks,
            piSessions = sess
        }

takeSession :: Pool -> IO Session
takeSession pool = atomically $ do
    sess <- readTVar (piSessions pool)
    if Seq.null sess
        then retry
        else do
            let ses = sess `Seq.index` 0
            writeTVar (piSessions pool) (Seq.drop 1 sess)
            return ses

putSession :: Pool -> Session -> IO ()
putSession pool ses = atomically $ modifyTVar (piSessions pool) (|> ses)

connectIfNeeded :: Pool -> Session -> IO Session
connectIfNeeded pool session =
    if isJust (sesActive session)
        then return session
        else do
            let hints = defaultHints { addrSocketType = Stream }
            let (host, service) = sesServer session
            ais <- getAddrInfo (Just hints) (Just host) (Just service)
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
    when (B.null bs) $ throw ShortRead
    let left = n - B.length bs
    if left == 0
        then return bs
        else do
            bs' <- recvAll s left
            return (bs `B.append` bs')

protocolVersion :: Word8
protocolVersion = 1

recvFrame :: Text -> StateT ActiveSession IO (Frame ByteString)
recvFrame qt = do
    s <- gets actSocket
    hdrBs <- liftIO $ recvAll s 8
    case runGet parseHeader hdrBs of
        Left err -> throw $ LocalProtocolError ("recvFrame: " `T.append` T.pack err) qt
        Right (ver0, flags, stream, opcode, length) -> do
            let ver = ver0 .&. 0x7f
            when (ver /= protocolVersion) $
                throw $ LocalProtocolError ("unexpected version " `T.append` T.pack (show ver)) qt
            body <- if length == 0
                then pure B.empty
                else liftIO $ recvAll s (fromIntegral length)
            --liftIO $ putStrLn $ hexdump 0 (C.unpack $ hdrBs `B.append` body)
            return $ Frame flags stream opcode body
  `catch` \exc -> throw $ CassandraIOException exc
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
  `catch` \exc -> throw $ CassandraIOException exc

class ProtoElt a where
    getElt :: Get a
    putElt :: a -> Put

encodeElt :: ProtoElt a => a -> ByteString
encodeElt = runPut . putElt

encodeCas :: CasType a => a -> ByteString
encodeCas = runPut . putCas

decodeElt :: ProtoElt a => ByteString -> Either String a
decodeElt bs = runGet getElt bs

decodeCas :: CasType a => ByteString -> Either String a
decodeCas bs = runGet getCas bs

decodeEltM :: (ProtoElt a, MonadIO m) => Text -> ByteString -> Text -> m a
decodeEltM what bs qt =
    case decodeElt bs of
        Left err -> throw $ LocalProtocolError
            ("can't parse" `T.append` what `T.append` ": " `T.append` T.pack err) qt
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
    deriving (Eq, Show)

-- | An exception that indicates an error originating in the Cassandra server.
data CassandraException = ServerError Text Text
                        | ProtocolError Text Text
                        | BadCredentials Text Text
                        | UnavailableException Text Consistency Int Int Text
                        | Overloaded Text Text
                        | IsBootstrapping Text Text
                        | TruncateError Text Text
                        | WriteTimeout Text Consistency Int Int Text Text
                        | ReadTimeout Text Consistency Int Int Bool Text
                        | SyntaxError Text Text
                        | Unauthorized Text Text
                        | Invalid Text Text
                        | ConfigError Text Text
                        | AlreadyExists Text Keyspace Table Text
                        | Unprepared Text PreparedQueryID Text
    deriving (Show, Typeable)

instance Exception CassandraException where

-- | All errors at the communications level are reported with this exception
-- ('IOException's from socket I/O are always wrapped), and this exception
-- typically would mean that a retry is warranted.
--
-- Note that this exception isn't guaranteed to be a transient one, so a limit
-- on the number of retries is likely to be a good idea.
-- 'LocalProtocolError' probably indicates a corrupted database or driver
-- bug.
data CassandraCommsError = AuthenticationException Text
                         | LocalProtocolError Text Text
                         | ValueMarshallingException TransportDirection Text Text
                         | CassandraIOException IOException
                         | ShortRead
    deriving (Show, Typeable)

instance Exception CassandraCommsError

throwError :: MonadCatchIO m => Text -> ByteString -> m a
throwError qt bs = do
    case runGet parseError bs of
        Left err -> throw $ LocalProtocolError ("failed to parse error: " `T.append` T.pack err) qt
        Right exc -> throw exc
  where
    parseError :: Get CassandraException
    parseError = do
       code <- getWord32be
       case code of
            0x0000 -> ServerError <$> getElt <*> pure qt 
            0x000A -> ProtocolError <$> getElt <*> pure qt
            0x0100 -> BadCredentials <$> getElt <*> pure qt
            0x1000 -> UnavailableException <$> getElt <*> getElt
                                           <*> (fromIntegral <$> getWord32be)
                                           <*> (fromIntegral <$> getWord32be) <*> pure qt
            0x1001 -> Overloaded <$> getElt <*> pure qt
            0x1002 -> IsBootstrapping <$> getElt <*> pure qt
            0x1003 -> TruncateError <$> getElt <*> pure qt
            0x1100 -> WriteTimeout <$> getElt <*> getElt
                                   <*> (fromIntegral <$> getWord32be)
                                   <*> (fromIntegral <$> getWord32be)
                                   <*> getElt <*> pure qt
            0x1200 -> ReadTimeout <$> getElt <*> getElt
                                  <*> (fromIntegral <$> getWord32be)
                                  <*> (fromIntegral <$> getWord32be)
                                  <*> ((/=0) <$> getWord8) <*> pure qt
            0x2000 -> SyntaxError <$> getElt <*> pure qt
            0x2100 -> Unauthorized <$> getElt <*> pure qt
            0x2200 -> Invalid <$> getElt <*> pure qt
            0x2300 -> ConfigError <$> getElt <*> pure qt
            0x2400 -> AlreadyExists <$> getElt <*> getElt <*> getElt <*> pure qt
            0x2500 -> Unprepared <$> getElt <*> getElt <*> pure qt
            _      -> fail $ "unknown error code 0x"++showHex code ""

introduce :: Pool -> StateT ActiveSession IO ()
introduce pool = do
    let qt = "<startup>"
    sendFrame $ Frame [] 0 STARTUP $ encodeElt $ ([("CQL_VERSION", "3.0.0")] :: [(Text, Text)])
    fr <- recvFrame qt
    case frOpcode fr of
        AUTHENTICATE -> throw $ AuthenticationException "authentication not implemented yet"
        READY -> return ()
        ERROR -> throwError qt (frBody fr)
        op -> throw $ LocalProtocolError ("introduce: unexpected opcode " `T.append` T.pack (show op)) qt
    let Keyspace ksName = piKeyspace pool
    let q = query $ "USE " `T.append` ksName :: Query Rows () ()
    res <- executeInternal q () ONE
    case res of
        SetKeyspace ks -> return ()
        _ -> throw $ LocalProtocolError ("expected SetKeyspace, but got " `T.append` T.pack (show res)) (queryText q)

withSession :: MonadCassandra m => (Pool -> StateT ActiveSession IO a) -> m a
withSession code = do
    pool <- getCassandraPool
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

-- | The name of a Cassandra keyspace. See the Cassandra documentation for more
-- information.
newtype Keyspace = Keyspace Text
    deriving (Eq, Ord, Show, IsString, ProtoElt)

-- | The name of a Cassandra table (a.k.a. column family).
newtype Table = Table Text
    deriving (Eq, Ord, Show, IsString, ProtoElt)

-- | A fully qualified identification of a table that includes the 'Keyspace'. 
data TableSpec = TableSpec Keyspace Table
    deriving (Eq, Ord, Show)

instance ProtoElt TableSpec where
    putElt _ = error "formatting TableSpec is not implemented"
    getElt = TableSpec <$> getElt <*> getElt

-- | Information about a table column.
data ColumnSpec = ColumnSpec TableSpec Text CType
    deriving Show

-- | The specification of a list of result set columns.
data Metadata = Metadata [ColumnSpec]
    deriving Show

-- | Cassandra data types as used in metadata.
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
           | CVarint
           | CTimeuuid
           | CInet
           | CList CType
           | CMap CType CType
           | CSet CType
           | CMaybe CType
        deriving (Eq)

instance Show CType where
    show ct = case ct of
        CCustom name -> T.unpack name
        CAscii -> "ascii"
        CBigint -> "bigint"
        CBlob -> "blob"
        CBoolean -> "boolean"
        CCounter -> "counter"
        CDecimal -> "decimal"
        CDouble -> "double"
        CFloat -> "float"
        CInt -> "int"
        CText -> "text"
        CTimestamp -> "timestamp"
        CUuid -> "uuid"
        CVarint -> "varint"
        CTimeuuid -> "timeuuid"
        CInet -> "inet"
        CList t -> "list<"++show t++">"
        CMap t1 t2 -> "map<"++show t1++","++show t2++">"
        CSet t -> "set<"++show t++">"
        CMaybe t -> "nullable "++show t

equivalent :: CType -> CType -> Bool
equivalent (CMaybe a) (CMaybe b) = a == b
equivalent (CMaybe a) b = a == b
equivalent a (CMaybe b) = a == b
equivalent a b = a == b

-- | A type class for types that can be used in query arguments or column values in
-- returned results.
--
-- To define your own newtypes for Cassandra data, you only need to define 'getCas',
-- 'putCas' and 'casType', like this:
--
-- > newtype UserId = UserId UUID deriving (Eq, Show)
-- >
-- > instance CasType UserId where
-- >     getCas = UserId <$> getCas
-- >     putCas (UserId i) = putCas i
-- >     casType (UserId i) = casType i
--
-- The same can be done more simply using the /GeneralizedNewtypeDeriving/ language
-- extension, e.g.
--
-- > {-# LANGUAGE GeneralizedNewtypeDeriving #-}
-- >
-- > ...
-- > newtype UserId = UserId UUID deriving (Eq, Show, CasType)
--
-- If you have a more complex type you want to store as a Cassandra blob, you could
-- write an instance like this (assuming it's an instance of the /cereal/ package's
-- 'Serialize' class):
--
-- > instance CasType User where
-- >     getCas = decode . unBlob <$> getCas
-- >     putCas = putCas . Blob . encode
-- >     casType _ = CBlob

class CasType a where
    getCas :: Get a
    putCas :: a -> Put
    -- | For a given Haskell type given as ('undefined' :: a), tell the caller how Cassandra
    -- represents it.
    casType :: a -> CType
    casNothing :: a
    casNothing = error "casNothing impossible"
    casObliterate :: a -> ByteString -> Maybe ByteString
    casObliterate _ bs = Just bs

instance CasType a => CasType (Maybe a) where
    getCas = Just <$> getCas
    putCas Nothing = return ()
    putCas (Just a) = putCas a
    casType _ = CMaybe (casType (undefined :: a))
    casNothing = Nothing
    casObliterate (Just a) bs = Just bs
    casObliterate Nothing  _  = Nothing

instance CasType ByteString where
    getCas = getByteString =<< remaining
    putCas = putByteString
    casType _ = CAscii

instance CasType Int64 where
    getCas = fromIntegral <$> getWord64be
    putCas = putWord64be . fromIntegral
    casType _ = CBigint

-- | If you wrap this round a 'ByteString', it will be treated as a /blob/ type
-- instead of /ascii/ (if it was a plain 'ByteString' type).
newtype Blob = Blob { unBlob :: ByteString }
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

-- | A Cassandra distributed counter value.
newtype Counter = Counter { unCounter :: Int64 }
    deriving (Eq, Ord, Show, Read)

instance CasType Counter where
    getCas = Counter . fromIntegral <$> getWord64be
    putCas (Counter c) = putWord64be (fromIntegral c)
    casType _ = CCounter

instance CasType Integer where
    getCas = do
        ws <- B.unpack <$> (getByteString =<< remaining)
        return $
            if null ws
                then 0
                else
                    let i = foldl' (\i w -> i `shiftL` 8 + fromIntegral w) 0 ws
                    in  if head ws >= 0x80
                            then i - 1 `shiftL` (length ws * 8)
                            else i
    putCas i = putByteString . B.pack $
        if i < 0
            then encodeNeg $ positivize 0x80 i
            else encodePos i
      where
        encodePos :: Integer -> [Word8]
        encodePos i = reverse $ enc i
          where
            enc i | i == 0   = [0]
            enc i | i < 0x80 = [fromIntegral i]
            enc i            = fromIntegral i : enc (i `shiftR` 8)
        encodeNeg :: Integer -> [Word8]
        encodeNeg i = reverse $ enc i
          where
            enc i | i == 0    = []
            enc i | i < 0x100 = [fromIntegral i]
            enc i             = fromIntegral i : enc (i `shiftR` 8)
        positivize :: Integer -> Integer -> Integer
        positivize bits i = case bits + i of
                                i' | i' >= 0 -> i' + bits
                                _            -> positivize (bits `shiftL` 8) i
    casType _ = CVarint

instance CasType Decimal where
    getCas = Decimal <$> (fromIntegral . min 0xff <$> getWord32be) <*> getCas
    putCas (Decimal places mantissa) = do
        putWord32be (fromIntegral places)
        putCas mantissa
    casType _ = CDecimal

instance CasType Double where
    getCas = unsafeCoerce <$> getWord64be
    putCas dbl = putWord64be (unsafeCoerce dbl)
    casType _ = CDouble

instance CasType Float where
    getCas = unsafeCoerce <$> getWord32be
    putCas dbl = putWord32be (unsafeCoerce dbl)
    casType _ = CFloat

epoch :: UTCTime
epoch = UTCTime (fromGregorian 1970 1 1) 0

instance CasType UTCTime where
    getCas = do
        ms <- getWord64be
        let difft = realToFrac $ (fromIntegral ms :: Double) / 1000
        return $ addUTCTime difft epoch
    putCas utc = do
        let seconds = realToFrac $ diffUTCTime utc epoch :: Double
            ms = round (seconds * 1000) :: Word64
        putWord64be ms
    casType _ = CTimestamp

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

-- | If you wrap this round a 'UUID' then it is treated as a /timeuuid/ type instead of
-- /uuid/ (if it was a plain 'UUID' type).
newtype TimeUUID = TimeUUID { unTimeUUID :: UUID } deriving (Eq, Data, Ord, Read, Show, Typeable)

instance CasType TimeUUID where
    getCas = TimeUUID <$> getCas
    putCas (TimeUUID uuid) = putCas uuid
    casType _ = CTimeuuid

instance CasType SockAddr where
    getCas = do
        len <- remaining
        case len of
            4  -> SockAddrInet 0 <$> getWord32le
            16 -> do
                a <- getWord32be
                b <- getWord32be
                c <- getWord32be
                d <- getWord32be
                return $ SockAddrInet6 0 0 (a,b,c,d) 0
            _  -> fail "malformed Inet"
    putCas sa = do
         case sa of
             SockAddrInet _ w -> putWord32le w
             SockAddrInet6 _ _ (a,b,c,d) _ -> putWord32be a >> putWord32be b
                                           >> putWord32be c >> putWord32be d
             _ -> fail $ "address type not supported in formatting Inet: " ++ show sa
    casType _ = CInet

instance CasType a => CasType [a] where
    getCas = do
        n <- getWord16be
        replicateM (fromIntegral n) $ do
            len <- getWord16be
            bs <- getByteString (fromIntegral len)
            case decodeCas bs of
                Left err -> fail err
                Right x -> return x
    putCas xs = do
        putWord16be (fromIntegral $ length xs)
        forM_ xs $ \x -> do
            let bs = encodeCas x
            putWord16be (fromIntegral $ B.length bs)
            putByteString bs
    casType _ = CList (casType (undefined :: a))

instance (CasType a, Ord a) => CasType (Set a) where
    getCas = S.fromList <$> getCas
    putCas = putCas . S.toList
    casType _ = CSet (casType (undefined :: a))

instance (CasType a, Ord a, CasType b) => CasType (Map a b) where
    getCas = do
        n <- getWord16be
        items <- replicateM (fromIntegral n) $ do
            len_a <- getWord16be
            bs_a <- getByteString (fromIntegral len_a)
            a <- case decodeCas bs_a of
                Left err -> fail err
                Right x -> return x
            len_b <- getWord16be
            bs_b <- getByteString (fromIntegral len_b)
            b <- case decodeCas bs_b of
                Left err -> fail err
                Right x -> return x
            return (a,b)
        return $ M.fromList items
    putCas m = do
        let items = M.toList m
        putWord16be (fromIntegral $ length items)
        forM_ items $ \(a,b) -> do
            let bs_a = encodeCas a
            putWord16be (fromIntegral $ B.length bs_a)
            putByteString bs_a
            let bs_b = encodeCas b
            putWord16be (fromIntegral $ B.length bs_b)
            putByteString bs_b
    casType _ = CMap (casType (undefined :: a)) (casType (undefined :: b))

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
            --0x000a -> pure CVarchar  -- Server seems to use CText even when 'varchar' is specified
                                       -- i.e. they're interchangeable in the CQL and always
                                       -- 'text' in the protocol.
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

-- | The first type argument for Query. Tells us what kind of query it is. 
data Style = Schema   -- ^ A query that modifies the schema, such as DROP TABLE or CREATE TABLE
           | Write    -- ^ A query that writes data, such as an INSERT or UPDATE
           | Rows     -- ^ A query that returns a list of rows, such as SELECT

-- | The text of a CQL query, along with type parameters to make the query type safe.
-- The type arguments are 'Style', followed by input and output column types for the
-- query each represented as a tuple.
--
-- The /DataKinds/ language extension is required for 'Style'.
data Query :: Style -> * -> * -> * where
    Query :: QueryID -> Text -> Query style i o
    deriving Show

queryText :: Query s i o -> Text
queryText (Query _ txt) = txt

instance IsString (Query style i o) where
    fromString = query . T.pack

-- | Construct a query. Another way to construct one is as an overloaded string through
-- the 'IsString' instance if you turn on the /OverloadedStrings/ language extension, e.g.
--
-- > {-# LANGUAGE OverloadedStrings #-}
-- > ...
-- >
-- > getOneSong :: Query Rows UUID (Text, Text, Maybe Text)
-- > getOneSong = "select title, artist, comment from songs where id=?"
query :: Text -> Query style i o
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

-- | A low-level query result used with 'executeRaw'.
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

instance ProtoElt (Result [Maybe ByteString]) where
    putElt _ = error "formatting RESULT is not implemented"
    getElt = do
        kind <- getWord32be
        case kind of
            0x0001 -> pure Void
            0x0002 -> do
                meta@(Metadata colSpecs) <- getElt
                let colCount = length colSpecs
                rowCount <- fromIntegral <$> getWord32be
                rows <- replicateM rowCount $ replicateM colCount $ do
                    len <- getWord32be
                    if len == 0xffffffff
                        then return Nothing
                        else Just <$> getByteString (fromIntegral len)
                return $ RowsResult meta rows
            0x0003 -> SetKeyspace <$> getElt
            0x0004 -> Prepared <$> getElt <*> getElt
            0x0005 -> SchemaChange <$> getElt <*> getElt <*> getElt
            _ -> fail $ "bad result kind: 0x"++showHex kind ""

prepare :: Query style i o -> StateT ActiveSession IO PreparedQuery
prepare (Query qid cql) = do
    cache <- gets actQueryCache
    case qid `M.lookup` cache of
        Just pq -> return pq
        Nothing -> do
            sendFrame $ Frame [] 0 PREPARE $ encodeElt (Long cql)
            fr <- recvFrame cql
            case frOpcode fr of
                RESULT -> do
                    res <- decodeEltM "RESULT" (frBody fr) cql
                    case (res :: Result [Maybe ByteString]) of
                        Prepared pqid meta -> do
                            let pq = PreparedQuery pqid meta
                            modify $ \act -> act { actQueryCache = M.insert qid pq (actQueryCache act) }
                            return pq
                        _ -> throw $ LocalProtocolError ("prepare: unexpected result " `T.append` T.pack (show res)) cql
                ERROR -> throwError cql (frBody fr)
                _ -> throw $ LocalProtocolError ("prepare: unexpected opcode " `T.append` T.pack (show (frOpcode fr))) cql

data CodingFailure = Mismatch Int CType CType
                   | WrongNumber Int Int
                   | DecodeFailure Int String
                   | NullValue Int CType

instance Show CodingFailure where
    show (Mismatch i t1 t2)    = "at value index "++show (i+1)++", Haskell type specifies "++show t1++", but database metadata says "++show t2
    show (WrongNumber i1 i2)   = "wrong number of values: Haskell type specifies "++show i1++" but database metadata says "++show i2
    show (DecodeFailure i why) = "failed to decode value index "++show (i+1)++": "++why
    show (NullValue i t)       = "at value index "++show (i+1)++" received a null "++show t++" value but Haskell type is not a Maybe"

class CasNested v where
    encodeNested :: Int -> v -> [CType] -> Either CodingFailure [Maybe ByteString]
    decodeNested :: Int -> [(CType, Maybe ByteString)] -> Either CodingFailure v
    countNested  :: v -> Int

instance CasNested () where
    encodeNested !i () [] = Right []
    encodeNested !i () ts = Left $ WrongNumber i (i + length ts)
    decodeNested !i []    = Right ()
    decodeNested !i vs    = Left $ WrongNumber i (i + length vs)
    countNested _         = 0

instance (CasType a, CasNested rem) => CasNested (a, rem) where
    encodeNested !i (a, rem) (ta:trem) | ta `equivalent` casType a =
        case encodeNested (i+1) rem trem of
            Left err -> Left err
            Right brem -> Right $ ba : brem
      where
        ba = casObliterate a . encodeCas $ a
    encodeNested !i (a, _) (ta:_) = Left $ Mismatch i (casType a) ta
    encodeNested !i vs      []    = Left $ WrongNumber (i + countNested vs) i 
    decodeNested !i ((ta, mba):rem) | ta `equivalent` casType (undefined :: a) =
        case (decodeCas <$> mba, casType (undefined :: a), decodeNested (i+1) rem) of
            (Nothing,         CMaybe _, Right arem) -> Right (casNothing, arem)
            (Nothing,         _,        _)          -> Left $ NullValue i ta
            (Just (Left err), _,        _)          -> Left $ DecodeFailure i err
            (_,               _,        Left err)   -> Left err 
            (Just (Right a),  _,        Right arem) -> Right (a, arem)
    decodeNested !i ((ta, _):rem) = Left $ Mismatch i (casType (undefined :: a)) ta
    decodeNested !i []            = Left $ WrongNumber (i + 1 + countNested (undefined :: rem)) i
    countNested _ = let n = 1 + countNested (undefined :: rem) 
                    in  seq n n

-- | A type class for a tuple of 'CasType' instances, representing either a list of
-- arguments for a query, or the values in a row of returned query results.
class CasValues v where
    encodeValues :: v -> [CType] -> Either CodingFailure [Maybe ByteString]
    decodeValues :: [(CType, Maybe ByteString)] -> Either CodingFailure v

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

-- | Cassandra consistency level. See the Cassandra documentation for an explanation.
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

-- | A low-level function in case you need some rarely-used capabilities. 
executeRaw :: (MonadCassandra m, CasValues i) =>
              Query style any_i any_o -> i -> Consistency -> m (Result [Maybe ByteString])
executeRaw query i cons = withSession $ \_ -> executeInternal query i cons

executeInternal :: CasValues values =>
                   Query style any_i any_o -> values -> Consistency -> StateT ActiveSession IO (Result [Maybe ByteString])
executeInternal query i cons = do
    pq@(PreparedQuery pqid queryMeta) <- prepare query
    values <- case encodeValues i (metadataTypes queryMeta) of
        Left err -> throw $ ValueMarshallingException TransportSending (T.pack $ show err) (queryText query)
        Right values -> return values
    sendFrame $ Frame [] 0 EXECUTE $ runPut $ do
        putElt pqid
        putWord16be (fromIntegral $ length values)
        forM_ values $ \mValue ->
            case mValue of
                Nothing -> putWord32be 0xffffffff
                Just value -> do
                    let enc = encodeCas value
                    putWord32be (fromIntegral $ B.length enc) 
                    putByteString enc
        putElt cons
    fr <- recvFrame (queryText query)
    case frOpcode fr of
        RESULT -> decodeEltM "RESULT" (frBody fr) (queryText query)
        ERROR -> throwError (queryText query) (frBody fr)
        _ -> throw $ LocalProtocolError ("execute: unexpected opcode " `T.append` T.pack (show (frOpcode fr))) (queryText query)

-- | Execute a query that returns rows.
executeRows :: (MonadCassandra m, CasValues i, CasValues o) =>
               Consistency
            -> Query Rows i o  -- ^ CQL query to execute
            -> i               -- ^ Input values substituted in the query
            -> m [o]
executeRows cons q i = do
    res <- executeRaw q i cons
    case res of
        RowsResult meta rows -> decodeRows q meta rows
        _ -> throw $ LocalProtocolError ("expected Rows, but got " `T.append` T.pack (show res)) (queryText q)

-- | Helper for 'executeRows' useful in situations where you are only expecting one row
-- to be returned.
executeRow :: (MonadCassandra m, CasValues i, CasValues o) =>
              Consistency
           -> Query Rows i o  -- ^ CQL query to execute
           -> i               -- ^ Input values substituted in the query
           -> m (Maybe o)
executeRow cons q i = do
    rows <- executeRows cons q i
    return $ listToMaybe rows

decodeRows :: (MonadCatchIO m, CasValues values) => Query Rows any_i values -> Metadata -> [[Maybe ByteString]] -> m [values]
decodeRows query meta rows0 = do
    let rows1 = flip map rows0 $ \cols -> decodeValues (zip (metadataTypes meta) cols)
    case lefts rows1 of
        (err:_) -> throw $ ValueMarshallingException TransportReceiving (T.pack $ show err) (queryText query)
        [] -> return ()
    let rows2 = flip map rows1 $ \(Right v) -> v
    return $ rows2

-- | Execute a write operation that returns void.
executeWrite :: (MonadCassandra m, CasValues i) =>
                Consistency
             -> Query Write i ()  -- ^ CQL query to execute
             -> i                 -- ^ Input values substituted in the query
             -> m ()
executeWrite cons q i = do
    res <- executeRaw q i cons
    case res of
        Void -> return ()
        _ -> throw $ LocalProtocolError ("expected Void, but got " `T.append` T.pack (show res)) (queryText q)

-- | Execute a schema change, such as creating or dropping a table.
executeSchema :: (MonadCassandra m, CasValues i) =>
                 Consistency
              -> Query Schema i ()  -- ^ CQL query to execute
              -> i                  -- ^ Input values substituted in the query
              -> m (Change, Keyspace, Table)
executeSchema cons q i = do
    res <- executeRaw q i cons
    case res of
        SchemaChange ch ks ta -> return (ch, ks, ta)
        _ -> throw $ LocalProtocolError ("expected SchemaChange, but got " `T.append` T.pack (show res)) (queryText q)

-- | A helper for extracting the types from a metadata definition.
metadataTypes :: Metadata -> [CType]
metadataTypes (Metadata colspecs) = map (\(ColumnSpec _ _ typ) -> typ) colspecs

-- | The monad used to run Cassandra queries in.
newtype Cas a = Cas (ReaderT Pool IO a)
    deriving (Functor, Applicative, Monad, MonadIO, MonadCatchIO)

instance MonadCassandra Cas where
    getCassandraPool = Cas ask

-- | Execute Cassandra queries.
runCas :: Pool -> Cas a -> IO a
runCas pool (Cas code) = runReaderT code pool 

