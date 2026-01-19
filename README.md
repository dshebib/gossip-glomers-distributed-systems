# Maelstrom Distributed Systems Challenge

This repo contains solutions to the set of Gossip Glomers distributed systems challenges posed by fly.io:
(https://fly.io/dist-sys/)

## Structure
Each directory models a common DS problem. It uses a testing platform called Maelstrom,
which provides a way to model a set of nodes and various failure states from a local environment.
Each contains a Go module run as an individual node which can only communicate with others using
a standardized set of messages.

## Summary
1. Echo - single-node echoing messages
2. Unique ID Generation - guaranteed unique ID generation using version 1 UUIDs
3. Broadcast - Eventually-consistent fault-tolerant broadcast system 
  - Sub-500ms latency with artifical 100ms message latency
  - Less than 20 messages per sent operation
  - Randomized network partitions to mimic faulty network
  - Best performance acheieved using tree node topology
4. Counter - Distributed grow-only counter using underlying KV-store primitive
5. Kafka - Kafka-style append-only log
  - A best-effort, eventually consistent log system that sacrifices durability for simplicity
  - Similar to Kafka with acks=0 (fire-and-forget) but with a centralized offset generation mechanism.
  - Uses centralized linearizable KV-store to acquire unique per-key offsets.

## Running
Follow the instructions at (https://fly.io/dist-sys/1/) to setup maelstrom and run the tests.
