{-# LANGUAGE OverloadedStrings, DataKinds, ScopedTypeVariables #-}

import Control.Applicative ((<$>))
import Control.Concurrent (threadDelay, forkIO)
import Control.Concurrent.MVar
import Control.Exception (SomeException)
import Control.Monad (replicateM, forM, unless)
import Control.Monad.CatchIO (throw, catch)
import Control.Monad.Trans (liftIO)
import Data.Text (Text, pack)
import Data.UUID (UUID)
import Data.UUID.V4 (nextRandom)
import Database.Cassandra.CQL
import System.Log.Logger

createThings :: Query Schema () ()
createThings = "create table if not exists things (id uuid PRIMARY KEY, val text)"

insertThing :: Query Write (UUID, Text) ()
insertThing = "insert into things (id, val) values (?, ?)"


ignoreProtocolError :: Cas () -> Cas ()
ignoreProtocolError code = code `catch` \exc -> case exc of
    LocalProtocolError _ _ -> return ()
    _               -> throw exc

simpleRetry :: IO a -> IO a
simpleRetry todo = do
  z <- fmap Just todo `catch` (\(e :: SomeException) -> putStrLn ("exception : " ++ show e) >> return Nothing)
  case z of
    Just a -> return a
    Nothing -> do
           threadDelay 1000000
           simpleRetry todo

-- Debug and higher goes to STDERR
setupLogging :: IO ()
setupLogging = updateGlobalLogger rootLoggerName (setLevel DEBUG)


main :: IO ()
main = do
  setupLogging

  let auth = Nothing
  -- first server should be unresponsive
  pool <- newPool [("172.16.0.1", "9042"), ("localhost", "9042")] "test" auth
  simpleRetry $ runCas pool $ ignoreProtocolError $ liftIO . print =<< executeSchema QUORUM createThings ()

  let writeBatch ids = mapM_ (runCas pool . (\id' -> executeWrite QUORUM insertThing (id', pack $ show id'))) ids


  joinOn <- replicateM 30 newEmptyMVar

  -- Not properly exception safe, but good enough for a test.
  _ <- forM joinOn $ \j -> forkIO $ do
         _ <- replicateM 10 $ do
                putStrLn "starting batch"

                ids <- replicateM 100 nextRandom
                simpleRetry (writeBatch ids)

                putStrLn "finished batch"
                threadDelay 10000000

         putMVar j True

  mapM_ takeMVar joinOn
  
  threadDelay 130000000

  unfinished <- filter ((/= 0) . statSessionCount) <$> serverStats pool
  unless (null unfinished) $ putStrLn $ "servers with outstanding sessions (should be none) : " ++ show unfinished
