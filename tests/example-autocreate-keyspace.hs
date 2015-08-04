{-# LANGUAGE OverloadedStrings, DataKinds #-}

import Database.Cassandra.CQL
import Control.Monad
import Control.Monad.CatchIO
import Control.Monad.Trans (liftIO)
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as C
import Data.Text (Text)
import qualified Data.Text as T
import Data.UUID
import System.Random

dropSongs :: Query Schema () ()
dropSongs = "drop table songs"

createSongs :: Query Schema () ()
createSongs = "create table songs (id uuid PRIMARY KEY, title ascii, artist varchar, femaleSinger boolean, timesPlayed int, comment text)"

insertSong :: Query Write (UUID, ByteString, Text, Bool, Int, Maybe Text) ()
insertSong = "insert into songs (id, title, artist, femaleSinger, timesPlayed, comment) values (?, ?, ?, ?, ?, ?)"

getSongs :: Query Rows () (UUID, ByteString, Text, Bool, Int, Maybe Text)
getSongs = "select id, title, artist, femaleSinger, timesPlayed, comment from songs"

getOneSong :: Query Rows UUID (Text, Int)
getOneSong = "select artist, timesPlayed from songs where id=?"

ignoreDropFailure :: Cas () -> Cas ()
ignoreDropFailure code = code `catch` \exc -> case exc of
    ConfigError _ _ -> return ()  -- Ignore the error if the table doesn't exist
    Invalid _ _ -> return ()
    _               -> throw exc

-- | Construct a pool of Cassandra connections.
newPool :: [Server] -> Keyspace -> Maybe Authentication -> IO Pool
newPool servers keyspace auth = newPool' $ defaultConfig servers keyspace auth

main = do
    -- let auth = Just (PasswordAuthenticator "cassandra" "cassandra")
    let auth = Nothing

    -- this config will automatically run keyspace creation cql script during each connection initializationj
    -- suitable for a development purposes

    let ksCfg  = "CREATE KEYSPACE IF NOT EXISTS test1 WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };"
    let poolCfg = (defaultConfig [("localhost", "9042")] "test1" auth){ piKeyspaceConfig = Just ksCfg}

    pool <- newPool' poolCfg -- servers, keyspace, maybe auth
    runCas pool $ do
        ignoreDropFailure $ liftIO . print =<< executeSchema QUORUM dropSongs ()
        liftIO . print =<< executeSchema QUORUM createSongs ()

        u1 <- liftIO randomIO
        u2 <- liftIO randomIO
        u3 <- liftIO randomIO
        executeWrite QUORUM insertSong (u1, "La Grange", "ZZ Top", False, 2, Nothing)
        executeWrite QUORUM insertSong (u2, "Your Star", "Evanescence", True, 799, Nothing)
        executeWrite QUORUM insertSong (u3, "Angel of Death", "Slayer", False, 50, Just "Singer Tom Araya")

        songs <- executeRows QUORUM getSongs ()
        liftIO $ forM_ songs $ \(uuid, title, artist, female, played, mComment) -> do
            putStrLn ""
            putStrLn $ "id            : "++show uuid
            putStrLn $ "title         : "++C.unpack title
            putStrLn $ "artist        : "++T.unpack artist
            putStrLn $ "female singer : "++show female
            putStrLn $ "times played  : "++show played
            putStrLn $ "comment       : "++show mComment

        liftIO $ putStrLn ""
        liftIO . print =<< executeRow QUORUM getOneSong u2
