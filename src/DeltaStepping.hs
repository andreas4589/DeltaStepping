{-# LANGUAGE RecordWildCards  #-}
--
-- INFOB3CC Concurrency
-- Practical 2: Single Source Shortest Path
--
--    Δ-stepping: A parallelisable shortest path algorithm
--    https://www.sciencedirect.com/science/article/pii/S0196677403000762
--
-- https://ics.uu.nl/docs/vakken/b3cc/assessment.html
--
-- https://cs.iupui.edu/~fgsong/LearnHPC/sssp/deltaStep.html
--

module DeltaStepping (

  Graph, Node, Distance,
  deltaStepping,

) where

import Sample
import Utils

import Control.Concurrent
import Control.Concurrent.MVar
import Control.Monad
import Data.Bits
import Data.Graph.Inductive                                         ( Gr )
import Data.IORef
import Data.IntMap.Strict                                           ( IntMap )
import Data.IntSet                                                  ( IntSet )
import Data.Vector.Storable                                         ( Vector )
import Data.Word
import Foreign.Ptr
import Foreign.Storable
import Control.Concurrent.STM
import Text.Printf
import qualified Data.Graph.Inductive                               as G
import qualified Data.IntMap.Strict                                 as Map
import qualified Data.IntSet                                        as Set
import qualified Data.Vector.Mutable                                as V
import qualified Data.Vector.Storable                               as S ( unsafeFreeze)
import qualified Data.Vector.Storable.Mutable                       as M
import qualified Data.Graph.Inductive.Internal.Heap as Data.IntSet
import Data.Primitive (emptyArray)
import qualified Data.IntMap as IntMap


type Graph    = Gr String Distance  -- Graphs have nodes labelled with Strings and edges labelled with their distance
type Node     = Int                 -- Nodes (vertices) in the graph are integers in the range [0..]
type Distance = Float               -- Distances between nodes are (positive) floating point values


-- | Find the length of the shortest path from the given node to all other nodes
-- in the graph. If the destination is not reachable from the starting node the
-- distance is 'Infinity'.
--
-- Nodes must be numbered [0..]
--
-- Negative edge weights are not supported.
--
-- NOTE: The type of the 'deltaStepping' function should not change (since that
-- is what the test suite expects), but you are free to change the types of all
-- other functions and data structures in this module as you require.


deltaStepping
    :: Bool                             -- Whether to print intermediate states to the console, for debugging purposes
    -> Graph                            -- graph to analyse
    -> Distance                         -- delta (step width, bucket width)
    -> Node                             -- index of the starting node
    -> IO (Vector Distance)
deltaStepping verbose graph delta source = do
  threadCount <- getNumCapabilities             -- the number of (kernel) threads to use: the 'x' in '+RTS -Nx'

  -- Initialise the algorithm
  (buckets, distances)  <- initialise graph delta source
  printVerbose verbose "initialse" graph delta buckets distances

  let
    -- The algorithm loops while there are still non-empty buckets
    loop = do
      done <- allBucketsEmpty buckets
      if done
      then return ()
      else do
        printVerbose verbose "result" graph delta buckets distances
        step verbose threadCount graph delta buckets distances
        loop
  loop

  printVerbose verbose "result" graph delta buckets distances
  -- Once the tentative distances are finalised, convert into an immutable array
  -- to prevent further updates. It is safe to use this "unsafe" function here
  -- because the mutable vector will not be used any more, so referential
  -- transparency is preserved for the frozen immutable vector.
  --
  -- NOTE: The function 'Data.Vector.convert' can be used to translate between
  -- different (compatible) vector types (e.g. boxed to storable)
  --
  S.unsafeFreeze distances

-- Initialise algorithm state
--
initialise
    :: Graph
    -> Distance
    -> Node
    -> IO (Buckets, TentativeDistances)
initialise graph delta source = do
  let numNodes   = G.order graph
      maxEdge    = maximum [dist | (_, _, dist) <- G.labEdges graph] -- Look up the maximum edge to decide the #buckets
      numBuckets = ceiling (maxEdge / delta)   
      srcBucket  = 0

  tentativeDistances <- M.replicate numNodes infinity                -- All vertices have infinite tentative distance, except s which has distance zero
  M.write tentativeDistances source 0
           
  bucketArray <- V.replicate numBuckets Set.empty
  V.modify bucketArray (Set.insert source) srcBucket           
  firstBucket <- newIORef srcBucket                                  -- All buckets are empty, except B[0] which contains s

  return (Buckets firstBucket bucketArray, tentativeDistances)


-- Take a single step of the algorithm.
-- That is, one iteration of the outer while loop.
--
step
    :: Bool
    -> Int
    -> Graph
    -> Distance
    -> Buckets
    -> TentativeDistances
    -> IO ()
step verbose threadCount graph delta buckets distances = do
  i <- findNextBucket buckets                                             -- (* Smallest nonempty bucket *)
  r <- newIORef Set.empty                                                 -- (* No nodes deleted for bucket B[i] yet *)

  let
    loop = do                                                             -- (* New phase *)
      bucket <- V.read (bucketArray buckets) i  
      let done = Set.null bucket

      if done then return ()
      else do
        printVerbose verbose "inner step" graph delta buckets distances
        req <- findRequests threadCount (<= delta) graph bucket distances -- (* Create requests for light edges *)
        modifyIORef' r (Set.union bucket)                                 -- (* Remember deleted nodes *)
        V.write (bucketArray buckets) i Set.empty                         -- (* Current bucket empty *)
        relaxRequests threadCount buckets distances delta req             -- (* Do relaxations, nodes may (re)enter B[i] *)
        loop
  loop

  rContent <- readIORef r 
  req <- findRequests threadCount (> delta) graph rContent distances      -- (* Create requests for heavy edges *)
  relaxRequests threadCount buckets distances delta req                   -- (* Relaxations will not refill B[i] *)

  
-- Once all buckets are empty, the tentative distances are finalised and the
-- algorithm terminates.
--
allBucketsEmpty :: Buckets -> IO Bool
allBucketsEmpty Buckets{..} = do
   let numBuckets = V.length bucketArray
   foldM
     (\acc idx -> do 
        bucket <- V.read bucketArray idx
        return $ acc && Set.null bucket)
     True [0 .. numBuckets - 1]

-- Return the index of the smallest non-empty bucket. Assumes that there is at
-- least one non-empty bucket remaining.
--
findNextBucket :: Buckets -> IO Int
findNextBucket buckets = do
  go 0
  where
    go index = do
      bucket <- V.read (bucketArray buckets) index
      if Set.null bucket then go (index + 1)
      else return index


-- Create requests of (node, distance) pairs that fulfil the given predicate
--
findRequests
    :: Int
    -> (Distance -> Bool)
    -> Graph
    -> IntSet
    -> TentativeDistances
    -> IO (IntMap Distance)
findRequests threadCount p graph nodes distances = do
  let splitNodes = splitIntSet threadCount nodes                -- Split the IntSet of nodes in threadCount amount of IntSets 
  localRequests <- replicateM threadCount (newIORef Map.empty)  -- Use thread-local maps for requests (Threadcount amount of IORefs containg empty IntMaps)

  forkThreads threadCount $ \threadId -> do
    let nodesI = splitNodes !! threadId

    Set.foldl'
      (\accIO v -> do
          accIO
          tentV <- M.read distances v

          let neighbors = filter (\(_, _, cost) -> p cost) (G.out graph v)
              updates   = foldr (\(_, w, cost) reqMap ->                      -- Only insert in the map if the distance of key is 
                insertIfLower (w, tentV + cost) reqMap) Map.empty neighbors   -- smaller than the original one

          modifyIORef' (localRequests !! threadId) (\reqMap -> foldr insertIfLower reqMap (Map.toList updates)))
      (return ())
      nodesI

  -- Merge all local maps into a single global map
  foldM (\acc ref -> do
           localMap <- readIORef ref
           return $ foldr insertIfLower acc (Map.toList localMap))
        Map.empty
        localRequests

-- Lookup the key in the map: if it is present, check if the newvalue is lower, then insert. If it is not present just insert.
insertIfLower :: (Node, Distance) -> IntMap Distance -> IntMap Distance
insertIfLower (key, newCost) acc =
  case Map.lookup key acc of
      Just oldCost | oldCost <= newCost -> acc                        -- Keep the old cost
      _                                 -> Map.insert key newCost acc -- Insert the new cost

-- Split an IntSet into n parts
splitIntSet :: Int -> IntSet -> [IntSet]
splitIntSet n req
    | n == 1    = [req]                                               -- If only one thread, return the entire set
    | otherwise = take n (go n [req] ++ repeat Set.empty)
  where
    go 1 acc = acc                                                    -- Stop when threadCount is reduced to 1
    go m acc = go (m `div` 2) (concatMap Set.splitRoot acc)           -- Divide the set in two with splitRoot


-- Execute requests for each of the given (node, distance) pairs
--
relaxRequests
    :: Int
    -> Buckets
    -> TentativeDistances
    -> Distance
    -> IntMap Distance
    -> IO ()
relaxRequests threadCount buckets distances delta req = do
  let splitReqs = splitIntMap threadCount req                           -- Split the IntMap in threadCount amount of IntMaps

  forkThreads threadCount $ \threadId -> do
    Map.foldrWithKey
        (\node newDistance acc -> do
            acc
            relax buckets distances delta (node, newDistance)           -- Relax alll the requests
        )
        (return ()) 
        (splitReqs !! threadId)
    
-- Function to split an IntMap into n parts
splitIntMap :: Int -> IntMap a -> [IntMap a]
splitIntMap n intmap
    | n == 1    = [intmap]                                              -- If only one thread, return the entire map
    | otherwise = take n (go n [intmap] ++ repeat Map.empty)
  where
    -- Recursive splitting of the IntMap
    go 1 acc = acc                                                      -- Stop splitting when we have enough parts
    go m acc = go (m - 1) (concatMap Map.splitRoot acc)


-- Execute a single relaxation, moving the given node to the appropriate bucket
-- as necessary
--
relax :: Buckets
      -> TentativeDistances
      -> Distance
      -> (Node, Distance) -- (w, x) in the paper
      -> IO ()
relax buckets distances delta (node, newDistance) = do
  oldDistance <- M.read distances node
  when (newDistance < oldDistance) $ do                                 -- (* Insert or move w in B if x < tent(w) *)
    let l         = V.length (bucketArray buckets)
        oldIndex  = floor (oldDistance / delta) `mod` l                 -- Make use of the cyclic array
        newIndex  = floor (newDistance / delta) `mod` l
        bArray    = bucketArray buckets
    
    atomicModifyIOVector bArray oldIndex $ \bucket -> (Set.delete node bucket, ())
    atomicModifyIOVector bArray newIndex $ \bucket -> (Set.insert node bucket, ())

    -- Update tentative distance atomically
    atomicModifyIOVectorFloat distances node $ \current ->
      if newDistance < current then (newDistance, ()) else (current, ())

-- -----------------------------------------------------------------------------
-- Starting framework
-- -----------------------------------------------------------------------------
--
-- Here are a collection of (data)types and utility functions that you can use.
-- You are free to change these as necessary.
--

type TentativeDistances = M.IOVector Distance

data Buckets = Buckets
  { firstBucket   :: {-# UNPACK #-} !(IORef Int)           -- real index of the first bucket (j)
  , bucketArray   :: {-# UNPACK #-} !(V.IOVector IntSet)   -- cyclic array of buckets
  }


-- The initial tentative distance, or the distance to unreachable nodes
--
infinity :: Distance
infinity = 1/0


-- Forks 'n' threads. Waits until those threads have finished. Each thread
-- runs the supplied function given its thread ID in the range [0..n).
--
forkThreads :: Int -> (Int -> IO ()) -> IO ()
forkThreads n action = do
  -- Fork the threads and create a list of the MVars which per thread tell
  -- whether the action has finished.
  finishVars <- mapM work [0 .. n - 1]
  -- Once all the worker threads have been launched, now wait for them all to
  -- finish by blocking on their signal MVars.
  mapM_ takeMVar finishVars
  where
    -- Create a new empty MVar that is shared between the main (spawning) thread
    -- and the worker (child) thread. The main thread returns immediately after
    -- spawning the worker thread. Once the child thread has finished executing
    -- the given action, it fills in the MVar to signal to the calling thread
    -- that it has completed.
    --
    work :: Int -> IO (MVar ())
    work index = do
      done <- newEmptyMVar
      _    <- forkOn index (action index >> putMVar done ())  -- pin the worker to a given CPU core
      return done


printVerbose :: Bool -> String -> Graph -> Distance -> Buckets -> TentativeDistances -> IO ()
printVerbose verbose title graph delta buckets distances = when verbose $ do
  putStrLn $ "# " ++ title
  printCurrentState graph distances
  printBuckets graph delta buckets distances
  putStrLn "Press enter to continue"
  _ <- getLine
  return ()

-- Print the current state of the algorithm (tentative distance to all nodes)
--
printCurrentState
    :: Graph
    -> TentativeDistances
    -> IO ()
printCurrentState graph distances = do
  printf "  Node  |  Label  |  Distance\n"
  printf "--------+---------+------------\n"
  forM_ (G.labNodes graph) $ \(v, l) -> do
    x <- M.read distances v
    if isInfinite x
       then printf "  %4d  |  %5v  |  -\n" v l
       else printf "  %4d  |  %5v  |  %f\n" v l x
  --
  printf "\n"

printBuckets
    :: Graph
    -> Distance
    -> Buckets
    -> TentativeDistances
    -> IO ()
printBuckets graph delta Buckets{..} distances = do
  first <- readIORef firstBucket
  mapM_
    (\idx -> do
      let idx' = first + idx
      printf "Bucket %d: [%f, %f)\n" idx' (fromIntegral idx' * delta) ((fromIntegral idx'+1) * delta)
      b <- V.read bucketArray (idx' `rem` V.length bucketArray)
      printBucket graph b distances
    )
    [ 0 .. V.length bucketArray - 1 ]

-- Print the current bucket
--
printCurrentBucket
    :: Graph
    -> Distance
    -> Buckets
    -> TentativeDistances
    -> IO ()
printCurrentBucket graph delta Buckets{..} distances = do
  j <- readIORef firstBucket
  b <- V.read bucketArray (j `rem` V.length bucketArray)
  printf "Bucket %d: [%f, %f)\n" j (fromIntegral j * delta) (fromIntegral (j+1) * delta)
  printBucket graph b distances

-- Print a given bucket
--
printBucket
    :: Graph
    -> IntSet
    -> TentativeDistances
    -> IO ()
printBucket graph bucket distances = do
  printf "  Node  |  Label  |  Distance\n"
  printf "--------+---------+-----------\n"
  forM_ (Set.toAscList bucket) $ \v -> do
    let ml = G.lab graph v
    x <- M.read distances v
    case ml of
      Nothing -> printf "  %4d  |   -   |  %f\n" v x
      Just l  -> printf "  %4d  |  %5v  |  %f\n" v l x
  --
  printf "\n"


